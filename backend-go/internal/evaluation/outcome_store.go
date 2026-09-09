package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PersistedOutcome struct {
	PredictionRunID      string         `json:"prediction_run_id"`
	AssetID              string         `json:"asset_id,omitempty"`
	SignalAvailableAt    time.Time      `json:"signal_available_at,omitempty"`
	Label                OutcomeLabel   `json:"label"`
	BenchmarkAssetID     string         `json:"benchmark_asset_id,omitempty"`
	BenchmarkMappingID   string         `json:"benchmark_mapping_id,omitempty"`
	ExclusionReason      string         `json:"exclusion_reason,omitempty"`
	ExecutionAssumptions map[string]any `json:"execution_assumptions"`
	DataQuality          map[string]any `json:"data_quality"`
	CreatedAt            time.Time      `json:"created_at,omitempty"`
}

type OutcomeStore struct{ db *pgxpool.Pool }

func NewOutcomeStore(db *pgxpool.Pool) *OutcomeStore { return &OutcomeStore{db: db} }

func (s *OutcomeStore) Save(ctx context.Context, record PersistedOutcome) (bool, error) {
	if s.db == nil || strings.TrimSpace(record.PredictionRunID) == "" {
		return false, fmt.Errorf("prediction outcome store and prediction_run_id are required")
	}
	if record.Label.Status != "mature" && record.Label.Status != "unavailable" && record.Label.Status != "excluded" {
		return false, fmt.Errorf("prediction outcome status is invalid")
	}
	if record.ExecutionAssumptions == nil {
		record.ExecutionAssumptions = map[string]any{}
	}
	if record.DataQuality == nil {
		record.DataQuality = map[string]any{}
	}
	execution, _ := json.Marshal(record.ExecutionAssumptions)
	quality, _ := json.Marshal(record.DataQuality)
	labelAvailableAt := record.Label.LabelAvailableAt
	if labelAvailableAt == nil && record.Label.Status != "mature" {
		now := time.Now().UTC()
		labelAvailableAt = &now
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO outcome_records(
        prediction_run_id,entry_at,exit_at,label_available_at,raw_return,benchmark_return,excess_return,net_return,execution_cost_bps,status,exclusion_reason,data_quality,
        label_definition_version,objective,horizon_sessions,entry_price,exit_price,price_field,time_precision,benchmark_asset_id,benchmark_mapping_id,alpha_definition,
		absolute_label,relative_label,objective_label,risk_adjusted_residual,risk_adjustment_status,gross_strategy_return,simulation_status,research_result_only,execution_assumptions)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31)
        ON CONFLICT(prediction_run_id) DO NOTHING`, record.PredictionRunID, record.Label.EntryAt, record.Label.ExitAt, labelAvailableAt,
		record.Label.RawReturn, record.Label.BenchmarkReturn, record.Label.ExcessReturn, record.Label.NetReturn, record.Label.ExecutionCostBPS,
		record.Label.Status, record.ExclusionReason, quality, record.Label.DefinitionVersion, record.Label.Objective, record.Label.HorizonSessions,
		record.Label.EntryPrice, record.Label.ExitPrice, record.Label.PriceField, record.Label.TimePrecision, record.BenchmarkAssetID, record.BenchmarkMappingID,
		record.Label.AlphaDefinition, record.Label.AbsoluteLabel, record.Label.RelativeLabel, record.Label.ObjectiveLabel,
		record.Label.RiskAdjustedResidual, record.Label.RiskAdjustmentStatus, record.Label.GrossStrategyReturn, record.Label.SimulationStatus, record.Label.ResearchResultOnly, execution)
	if err != nil {
		return false, fmt.Errorf("save prediction outcome: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *OutcomeStore) ListByAsset(ctx context.Context, assetID string, limit int) ([]PersistedOutcome, error) {
	assetID = strings.TrimSpace(assetID)
	if s.db == nil || assetID == "" || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid prediction outcome query")
	}
	rows, err := s.db.Query(ctx, `SELECT o.prediction_run_id,p.asset_id,p.signal_available_at,o.status,o.exclusion_reason,o.label_definition_version,o.objective,o.horizon_sessions,
        o.entry_at,o.exit_at,o.label_available_at,o.entry_price,o.exit_price,o.raw_return,o.benchmark_return,o.excess_return,o.risk_adjusted_residual,
        o.gross_strategy_return,o.net_return,o.execution_cost_bps,o.absolute_label,o.relative_label,o.objective_label,o.simulation_status,o.research_result_only,
		o.price_field,o.time_precision,o.alpha_definition,o.risk_adjustment_status,o.benchmark_asset_id,o.benchmark_mapping_id,
		o.execution_assumptions::jsonb,o.data_quality::jsonb,o.created_at
        FROM outcome_records o JOIN prediction_runs p ON p.id=o.prediction_run_id WHERE p.asset_id=$1
        ORDER BY p.signal_available_at DESC,o.horizon_sessions,o.prediction_run_id LIMIT $2`, assetID, limit)
	if err != nil {
		return nil, fmt.Errorf("list prediction outcomes: %w", err)
	}
	defer rows.Close()
	items := []PersistedOutcome{}
	for rows.Next() {
		var item PersistedOutcome
		var execution, quality any
		if err := rows.Scan(&item.PredictionRunID, &item.AssetID, &item.SignalAvailableAt, &item.Label.Status, &item.ExclusionReason,
			&item.Label.DefinitionVersion, &item.Label.Objective, &item.Label.HorizonSessions, &item.Label.EntryAt, &item.Label.ExitAt, &item.Label.LabelAvailableAt,
			&item.Label.EntryPrice, &item.Label.ExitPrice, &item.Label.RawReturn, &item.Label.BenchmarkReturn, &item.Label.ExcessReturn, &item.Label.RiskAdjustedResidual,
			&item.Label.GrossStrategyReturn, &item.Label.NetReturn, &item.Label.ExecutionCostBPS, &item.Label.AbsoluteLabel, &item.Label.RelativeLabel,
			&item.Label.ObjectiveLabel, &item.Label.SimulationStatus, &item.Label.ResearchResultOnly, &item.Label.PriceField, &item.Label.TimePrecision,
			&item.Label.AlphaDefinition, &item.Label.RiskAdjustmentStatus, &item.BenchmarkAssetID, &item.BenchmarkMappingID,
			&execution, &quality, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan prediction outcome: %w", err)
		}
		if err := decodeOutcomeJSON(execution, &item.ExecutionAssumptions); err != nil {
			return nil, err
		}
		if err := decodeOutcomeJSON(quality, &item.DataQuality); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func decodeOutcomeJSON(raw, target any) error {
	body, err := json.Marshal(raw)
	if bytes, ok := raw.([]byte); ok {
		body = bytes
	} else if value, ok := raw.(string); ok {
		body = []byte(value)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}
