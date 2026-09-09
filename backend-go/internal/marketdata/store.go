package marketdata

import (
	"context"
	"encoding/json"
	"fmt"

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
