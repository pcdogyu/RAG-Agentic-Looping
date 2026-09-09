package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Store) SaveCorporateAction(ctx context.Context, observation CorporateActionObservation) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("market data store is unavailable")
	}
	if err := observation.NormalizeAndValidate(); err != nil {
		return false, err
	}
	sourcePayload, err := json.Marshal(observation.SourcePayload)
	if err != nil {
		return false, fmt.Errorf("marshal corporate action source payload: %w", err)
	}
	metadata, err := json.Marshal(observation.Metadata)
	if err != nil {
		return false, fmt.Errorf("marshal corporate action metadata: %w", err)
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO corporate_action_observations(
        id,event_key,asset_id,market,currency,action_type,effective_at,observed_at,available_at,announcement_at,record_at,payment_at,end_at,
        ratio_numerator,ratio_denominator,cash_amount,adjusted_cash_amount,old_symbol,new_symbol,time_precision,source_name,source_document_id,source_url,source_payload,metadata)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
        ON CONFLICT(id) DO NOTHING`,
		observation.ID, observation.EventKey, observation.AssetID, observation.Market, observation.Currency, observation.ActionType,
		observation.EffectiveAt, observation.ObservedAt, observation.AvailableAt, observation.AnnouncementAt, observation.RecordAt, observation.PaymentAt, observation.EndAt,
		observation.RatioNumerator, observation.RatioDenominator, observation.CashAmount, observation.AdjustedCashAmount, observation.OldSymbol, observation.NewSymbol,
		observation.TimePrecision, observation.SourceName, observation.SourceDocumentID, observation.SourceURL, sourcePayload, metadata)
	if err != nil {
		return false, fmt.Errorf("save corporate action observation: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListCorporateActionsAvailable selects the latest provider revision for each
// logical event that was actually known by availableAsOf. Later backfills and
// corrections therefore cannot leak into an earlier research cutoff.
func (s *Store) ListCorporateActionsAvailable(ctx context.Context, assetID string, effectiveThrough, availableAsOf time.Time, actionType CorporateActionType, limit int) ([]CorporateActionObservation, error) {
	assetID = strings.TrimSpace(assetID)
	actionType = CorporateActionType(strings.ToLower(strings.TrimSpace(string(actionType))))
	if s.db == nil || assetID == "" || effectiveThrough.IsZero() || availableAsOf.IsZero() || limit < 1 || limit > 500 {
		return nil, fmt.Errorf("invalid corporate action observation query")
	}
	if actionType != "" && !corporateActionTypes[actionType] {
		return nil, fmt.Errorf("invalid corporate action type")
	}
	rows, err := s.db.Query(ctx, `WITH latest AS (
        SELECT DISTINCT ON (source_name,event_key)
            id,event_key,asset_id,market,currency,action_type,effective_at,observed_at,available_at,announcement_at,record_at,payment_at,end_at,
            ratio_numerator,ratio_denominator,cash_amount,adjusted_cash_amount,old_symbol,new_symbol,time_precision,source_name,source_document_id,source_url,source_payload::jsonb,metadata::jsonb,created_at
        FROM corporate_action_observations
        WHERE asset_id=$1 AND effective_at<=$2 AND available_at<=$3 AND ($4='' OR action_type=$4)
        ORDER BY source_name,event_key,available_at DESC,created_at DESC,id DESC
    ) SELECT id,event_key,asset_id,market,currency,action_type,effective_at,observed_at,available_at,announcement_at,record_at,payment_at,end_at,
        ratio_numerator,ratio_denominator,cash_amount,adjusted_cash_amount,old_symbol,new_symbol,time_precision,source_name,source_document_id,source_url,source_payload::jsonb,metadata::jsonb,created_at
      FROM latest ORDER BY effective_at DESC,action_type,event_key LIMIT $5`, assetID, effectiveThrough.UTC(), availableAsOf.UTC(), actionType, limit)
	if err != nil {
		return nil, fmt.Errorf("list corporate action observations: %w", err)
	}
	defer rows.Close()
	items := []CorporateActionObservation{}
	for rows.Next() {
		item, err := scanCorporateAction(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func scanCorporateAction(row pgx.Row) (CorporateActionObservation, error) {
	var item CorporateActionObservation
	var sourcePayload, metadata any
	err := row.Scan(&item.ID, &item.EventKey, &item.AssetID, &item.Market, &item.Currency, &item.ActionType, &item.EffectiveAt, &item.ObservedAt, &item.AvailableAt,
		&item.AnnouncementAt, &item.RecordAt, &item.PaymentAt, &item.EndAt, &item.RatioNumerator, &item.RatioDenominator, &item.CashAmount, &item.AdjustedCashAmount,
		&item.OldSymbol, &item.NewSymbol, &item.TimePrecision, &item.SourceName, &item.SourceDocumentID, &item.SourceURL, &sourcePayload, &metadata, &item.CreatedAt)
	if err != nil {
		return CorporateActionObservation{}, fmt.Errorf("scan corporate action observation: %w", err)
	}
	if err := decodeCorporateActionJSON(sourcePayload, &item.SourcePayload); err != nil {
		return CorporateActionObservation{}, fmt.Errorf("decode corporate action source payload: %w", err)
	}
	if err := decodeCorporateActionJSON(metadata, &item.Metadata); err != nil {
		return CorporateActionObservation{}, fmt.Errorf("decode corporate action metadata: %w", err)
	}
	return item, nil
}

func decodeCorporateActionJSON(raw, target any) error {
	var body []byte
	switch value := raw.(type) {
	case []byte:
		body = value
	case string:
		body = []byte(value)
	default:
		var err error
		body, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(body, target)
}
