package consensus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Guidance struct {
	ID               string         `json:"id"`
	AssetID          string         `json:"asset_id"`
	Metric           string         `json:"metric"`
	FiscalPeriod     string         `json:"fiscal_period,omitempty"`
	FiscalPeriodEnd  time.Time      `json:"fiscal_period_end"`
	AccountingBasis  string         `json:"accounting_basis"`
	LowValue         *float64       `json:"low_value,omitempty"`
	HighValue        *float64       `json:"high_value,omitempty"`
	Currency         string         `json:"currency,omitempty"`
	Unit             string         `json:"unit"`
	PublishedAt      time.Time      `json:"published_at"`
	AvailableAt      time.Time      `json:"available_at"`
	RevisionAt       *time.Time     `json:"revision_at,omitempty"`
	SourceName       string         `json:"source_name"`
	SourceURL        string         `json:"source_url"`
	SourceDocumentID string         `json:"source_document_id,omitempty"`
	SourcePayload    map[string]any `json:"source_payload"`
	RetrievedAt      time.Time      `json:"retrieved_at"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) SaveEstimate(ctx context.Context, value Estimate, payload map[string]any, retrievedAt time.Time) (bool, error) {
	if err := normalizeEstimate(&value, retrievedAt); err != nil {
		return false, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO consensus_snapshots(id,asset_id,metric,fiscal_period,fiscal_period_end,accounting_basis,statistic,estimate_value,analyst_count,currency,unit,published_at,available_at,revision_at,source_name,source_url,source_document_id,source_payload,retrieved_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT(asset_id,metric,fiscal_period_end,accounting_basis,statistic,source_name,source_document_id,published_at) DO NOTHING`, value.ID, value.AssetID, value.Metric, value.FiscalPeriod, value.FiscalPeriodEnd, value.AccountingBasis, value.Statistic, value.Value, value.AnalystCount, value.Currency, value.Unit, value.PublishedAt, value.AvailableAt, value.RevisionAt, value.SourceName, value.SourceURL, value.SourceDocumentID, body, retrievedAt.UTC())
	if err != nil {
		return false, fmt.Errorf("save consensus snapshot: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) SaveGuidance(ctx context.Context, value Guidance) (bool, error) {
	if err := normalizeGuidance(&value); err != nil {
		return false, err
	}
	body, err := json.Marshal(value.SourcePayload)
	if err != nil {
		return false, err
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO management_guidance_snapshots(id,asset_id,metric,fiscal_period,fiscal_period_end,accounting_basis,low_value,high_value,currency,unit,published_at,available_at,revision_at,source_name,source_url,source_document_id,source_payload,retrieved_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
ON CONFLICT(asset_id,metric,fiscal_period_end,accounting_basis,source_name,source_document_id,published_at) DO NOTHING`, value.ID, value.AssetID, value.Metric, value.FiscalPeriod, value.FiscalPeriodEnd, value.AccountingBasis, value.LowValue, value.HighValue, value.Currency, value.Unit, value.PublishedAt, value.AvailableAt, value.RevisionAt, value.SourceName, value.SourceURL, value.SourceDocumentID, body, value.RetrievedAt)
	if err != nil {
		return false, fmt.Errorf("save guidance snapshot: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) EstimatesBefore(ctx context.Context, assetID, metric string, periodEnd, announcementAt time.Time) ([]Estimate, error) {
	rows, err := s.db.Query(ctx, `SELECT id,asset_id,metric,fiscal_period,fiscal_period_end,accounting_basis,statistic,estimate_value,analyst_count,currency,unit,published_at,available_at,revision_at,source_name,source_url,source_document_id FROM consensus_snapshots WHERE asset_id=$1 AND metric=$2 AND fiscal_period_end=$3 AND available_at<$4 ORDER BY available_at,id`, assetID, metric, dateUTC(periodEnd), announcementAt.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []Estimate{}
	for rows.Next() {
		var value Estimate
		if err := rows.Scan(&value.ID, &value.AssetID, &value.Metric, &value.FiscalPeriod, &value.FiscalPeriodEnd, &value.AccountingBasis, &value.Statistic, &value.Value, &value.AnalystCount, &value.Currency, &value.Unit, &value.PublishedAt, &value.AvailableAt, &value.RevisionAt, &value.SourceName, &value.SourceURL, &value.SourceDocumentID); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func normalizeEstimate(value *Estimate, retrievedAt time.Time) error {
	value.AssetID, value.Metric, value.AccountingBasis, value.Statistic = strings.TrimSpace(value.AssetID), strings.TrimSpace(value.Metric), strings.ToLower(strings.TrimSpace(value.AccountingBasis)), strings.ToLower(strings.TrimSpace(value.Statistic))
	value.Currency, value.Unit, value.SourceName, value.SourceURL = strings.ToUpper(strings.TrimSpace(value.Currency)), strings.ToLower(strings.TrimSpace(value.Unit)), strings.TrimSpace(value.SourceName), strings.TrimSpace(value.SourceURL)
	if value.AssetID == "" || value.Metric == "" || value.FiscalPeriodEnd.IsZero() || value.PublishedAt.IsZero() || value.AvailableAt.IsZero() || value.SourceName == "" || value.SourceURL == "" {
		return fmt.Errorf("consensus snapshot has missing required metadata")
	}
	if value.AvailableAt.Before(value.PublishedAt) {
		return fmt.Errorf("consensus snapshot available_at precedes published_at")
	}
	if value.AccountingBasis == "" {
		value.AccountingBasis = "unknown"
	}
	if value.Statistic == "" {
		value.Statistic = "mean"
	}
	if value.Unit == "" {
		value.Unit = "reported"
	}
	if value.Unit != "reported" && value.Unit != "thousands" && value.Unit != "millions" && value.Unit != "billions" {
		return fmt.Errorf("invalid consensus unit")
	}
	if value.ID == "" {
		value.ID = consensusID(value.AssetID, value.Metric, value.FiscalPeriodEnd, value.AccountingBasis, value.Statistic, value.SourceName, value.SourceDocumentID, value.PublishedAt)
	}
	return nil
}

func normalizeGuidance(value *Guidance) error {
	if value.LowValue == nil && value.HighValue == nil {
		return fmt.Errorf("guidance requires a low or high value")
	}
	if value.LowValue != nil && value.HighValue != nil && *value.LowValue > *value.HighValue {
		return fmt.Errorf("guidance low_value exceeds high_value")
	}
	originalID := value.ID
	estimate := Estimate{ID: value.ID, AssetID: value.AssetID, Metric: value.Metric, FiscalPeriod: value.FiscalPeriod, FiscalPeriodEnd: value.FiscalPeriodEnd, AccountingBasis: value.AccountingBasis, Statistic: "mean", Currency: value.Currency, Unit: value.Unit, PublishedAt: value.PublishedAt, AvailableAt: value.AvailableAt, RevisionAt: value.RevisionAt, SourceName: value.SourceName, SourceURL: value.SourceURL, SourceDocumentID: value.SourceDocumentID}
	if err := normalizeEstimate(&estimate, value.RetrievedAt); err != nil {
		return err
	}
	value.ID, value.AssetID, value.Metric, value.FiscalPeriod, value.FiscalPeriodEnd, value.AccountingBasis, value.Currency, value.Unit, value.PublishedAt, value.AvailableAt, value.RevisionAt, value.SourceName, value.SourceURL, value.SourceDocumentID = originalID, estimate.AssetID, estimate.Metric, estimate.FiscalPeriod, estimate.FiscalPeriodEnd, estimate.AccountingBasis, estimate.Currency, estimate.Unit, estimate.PublishedAt, estimate.AvailableAt, estimate.RevisionAt, estimate.SourceName, estimate.SourceURL, estimate.SourceDocumentID
	if value.ID == "" {
		value.ID = consensusID(value.AssetID, value.Metric, value.FiscalPeriodEnd, value.AccountingBasis, "guidance", value.SourceName, value.SourceDocumentID, value.PublishedAt)
	}
	return nil
}

func consensusID(parts ...any) string {
	text := make([]string, len(parts))
	for i, part := range parts {
		switch v := part.(type) {
		case time.Time:
			text[i] = v.UTC().Format(time.RFC3339Nano)
		default:
			text[i] = fmt.Sprint(v)
		}
	}
	sum := sha256.Sum256([]byte(strings.Join(text, "\x1f")))
	return hex.EncodeToString(sum[:])
}
