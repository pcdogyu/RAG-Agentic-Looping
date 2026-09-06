package fundamentals

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

// Save preserves the original observation. Repeating an identical provider
// revision is idempotent; a later filing/acceptance timestamp creates a new
// row, retaining the earlier version for historical replay.
func (s *Store) Save(ctx context.Context, snapshot Snapshot) (bool, error) {
	if err := snapshot.NormalizeAndValidate(); err != nil {
		return false, err
	}
	metrics, err := json.Marshal(snapshot.Metrics)
	if err != nil {
		return false, fmt.Errorf("marshal fundamental metrics: %w", err)
	}
	payload, err := json.Marshal(snapshot.SourcePayload)
	if err != nil {
		return false, fmt.Errorf("marshal fundamental source payload: %w", err)
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO fundamental_snapshots(
        id,asset_id,statement_type,fiscal_period,report_period_end,published_at,available_at,revision_at,
        currency,unit,accounting_standard,source_name,source_url,source_document_id,metrics,source_payload,retrieved_at
    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    ON CONFLICT(asset_id,statement_type,report_period_end,source_name,source_document_id,published_at) DO NOTHING`,
		snapshot.ID, snapshot.AssetID, snapshot.StatementType, snapshot.FiscalPeriod, snapshot.ReportPeriodEnd,
		snapshot.PublishedAt, snapshot.AvailableAt, snapshot.RevisionAt, snapshot.Currency, snapshot.Unit,
		snapshot.AccountingStd, snapshot.SourceName, snapshot.SourceURL, snapshot.SourceDocumentID, metrics,
		payload, snapshot.RetrievedAt)
	if err != nil {
		return false, fmt.Errorf("save fundamental snapshot: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListAvailable reads only disclosures visible at cutoff and selects the most
// recent revision of each (asset, statement type, report period) fact.
func (s *Store) ListAvailable(ctx context.Context, assetID string, cutoff time.Time, limit int) ([]Snapshot, error) {
	if cutoff.IsZero() {
		return nil, fmt.Errorf("fundamental as_of cutoff is required")
	}
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("fundamental limit must be between 1 and 100")
	}
	rows, err := s.db.Query(ctx, `SELECT id,asset_id,statement_type,fiscal_period,report_period_end,published_at,available_at,revision_at,
        currency,unit,accounting_standard,source_name,source_url,source_document_id,metrics::jsonb,source_payload::jsonb,retrieved_at,created_at
    FROM fundamental_snapshots WHERE asset_id=$1 AND available_at <= $2
    ORDER BY statement_type,report_period_end DESC,COALESCE(revision_at,published_at) DESC,id DESC`, assetID, cutoff.UTC())
	if err != nil {
		return nil, fmt.Errorf("list fundamental snapshots: %w", err)
	}
	defer rows.Close()
	items := make([]Snapshot, 0)
	for rows.Next() {
		item, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	items = LatestAvailable(items, cutoff)
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func scanSnapshot(row pgx.Row) (Snapshot, error) {
	var item Snapshot
	var metrics, payload []byte
	if err := row.Scan(&item.ID, &item.AssetID, &item.StatementType, &item.FiscalPeriod, &item.ReportPeriodEnd,
		&item.PublishedAt, &item.AvailableAt, &item.RevisionAt, &item.Currency, &item.Unit, &item.AccountingStd,
		&item.SourceName, &item.SourceURL, &item.SourceDocumentID, &metrics, &payload, &item.RetrievedAt, &item.CreatedAt); err != nil {
		return Snapshot{}, fmt.Errorf("scan fundamental snapshot: %w", err)
	}
	if err := json.Unmarshal(metrics, &item.Metrics); err != nil {
		return Snapshot{}, fmt.Errorf("decode fundamental metrics: %w", err)
	}
	if err := json.Unmarshal(payload, &item.SourcePayload); err != nil {
		return Snapshot{}, fmt.Errorf("decode fundamental source payload: %w", err)
	}
	return item, nil
}
