package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) Save(ctx context.Context, observation PriceObservation) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("market data store is unavailable")
	}
	if err := observation.NormalizeAndValidate(); err != nil {
		return false, err
	}
	metadata, err := json.Marshal(observation.Metadata)
	if err != nil {
		return false, fmt.Errorf("marshal market price metadata: %w", err)
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO market_price_observations(
        id,asset_id,market,currency,session_date,observed_at,available_at,price,price_field,time_precision,source_name,source_document_id,source_url,metadata)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT(asset_id,source_name,source_document_id,observed_at,price_field,price) DO NOTHING`,
		observation.ID, observation.AssetID, observation.Market, observation.Currency, observation.SessionDate,
		observation.ObservedAt, observation.AvailableAt, observation.Price, observation.PriceField,
		observation.TimePrecision, observation.SourceName, observation.SourceDocumentID, observation.SourceURL, metadata)
	if err != nil {
		return false, fmt.Errorf("save market price observation: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListAvailable returns the latest provider revision of each observation that
// was actually available by the requested cutoff. observedThrough and
// availableAsOf are deliberately separate to prevent later corrections from
// leaking into historical evaluation.
func (s *Store) ListAvailable(ctx context.Context, assetID string, observedThrough, availableAsOf time.Time, priceField string, limit int) ([]PriceObservation, error) {
	assetID = strings.TrimSpace(assetID)
	priceField = strings.ToLower(strings.TrimSpace(priceField))
	if s.db == nil || assetID == "" || observedThrough.IsZero() || availableAsOf.IsZero() || limit < 1 || limit > 500 {
		return nil, fmt.Errorf("invalid market price observation query")
	}
	if priceField != "" && priceField != "close" && priceField != "adjusted_close" {
		return nil, fmt.Errorf("invalid market price field")
	}
	rows, err := s.db.Query(ctx, `WITH latest AS (
        SELECT DISTINCT ON (source_name,source_document_id,observed_at,price_field)
            id,asset_id,market,currency,session_date,observed_at,available_at,price,price_field,time_precision,source_name,source_document_id,source_url,metadata::jsonb,created_at
        FROM market_price_observations
        WHERE asset_id=$1 AND observed_at<=$2 AND available_at<=$3 AND ($4='' OR price_field=$4)
        ORDER BY source_name,source_document_id,observed_at,price_field,available_at DESC,created_at DESC,id DESC
    ) SELECT id,asset_id,market,currency,session_date,observed_at,available_at,price,price_field,time_precision,source_name,source_document_id,source_url,metadata::jsonb,created_at
      FROM latest ORDER BY observed_at DESC,price_field,source_name LIMIT $5`, assetID, observedThrough.UTC(), availableAsOf.UTC(), priceField, limit)
	if err != nil {
		return nil, fmt.Errorf("list market price observations: %w", err)
	}
	defer rows.Close()
	items := []PriceObservation{}
	for rows.Next() {
		var item PriceObservation
		var metadata any
		if err := rows.Scan(&item.ID, &item.AssetID, &item.Market, &item.Currency, &item.SessionDate, &item.ObservedAt, &item.AvailableAt, &item.Price, &item.PriceField, &item.TimePrecision, &item.SourceName, &item.SourceDocumentID, &item.SourceURL, &metadata, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan market price observation: %w", err)
		}
		body, err := json.Marshal(metadata)
		if raw, ok := metadata.([]byte); ok {
			body = raw
		} else if raw, ok := metadata.(string); ok {
			body = []byte(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("encode market price metadata: %w", err)
		}
		if err := json.Unmarshal(body, &item.Metadata); err != nil {
			return nil, fmt.Errorf("decode market price metadata: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
