package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/evaluation"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
)

type predictionOutcomeCandidate struct {
	ID, AssetID, AssetClass, Market, Symbol, Currency string
	SignalAvailableAt                                 time.Time
	HorizonSessions                                   int
	Objective, PredictionStatus                       string
	RawScore                                          *float64
	ModelScope                                        map[string]any
}

func (runtime *outcomeRuntime) evaluatePredictionOutcomes(ctx context.Context, now time.Time, cache map[string][]outcomePricePoint) (map[string]any, error) {
	rows, err := runtime.db.Query(ctx, `SELECT p.id,p.asset_id,p.asset_class,a.market,a.symbol,a.currency,p.signal_available_at,p.horizon_sessions,
        p.objective,p.status,p.raw_score,m.scope::jsonb FROM prediction_runs p JOIN assets a ON a.id=p.asset_id
        JOIN prediction_models m ON m.version=p.model_version LEFT JOIN outcome_records o ON o.prediction_run_id=p.id
        WHERE o.prediction_run_id IS NULL ORDER BY p.signal_available_at,p.id LIMIT 500`)
	if err != nil {
		return nil, fmt.Errorf("query prediction outcome candidates: %w", err)
	}
	candidates := []predictionOutcomeCandidate{}
	for rows.Next() {
		var item predictionOutcomeCandidate
		var scope []byte
		if err := rows.Scan(&item.ID, &item.AssetID, &item.AssetClass, &item.Market, &item.Symbol, &item.Currency, &item.SignalAvailableAt,
			&item.HorizonSessions, &item.Objective, &item.PredictionStatus, &item.RawScore, &scope); err != nil {
			rows.Close()
			return nil, err
		}
		item.ModelScope = map[string]any{}
		_ = json.Unmarshal(scope, &item.ModelScope)
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	summary := map[string]any{"selected": len(candidates), "matured": 0, "pending": 0, "unavailable": 0, "excluded": 0, "failed": 0, "failures": []string{}}
	store := evaluation.NewOutcomeStore(runtime.db)
	failures := []string{}
	for _, item := range candidates {
		policy, policyErr := evaluation.ResolveHorizonPolicy(item.Objective, item.HorizonSessions)
		if policyErr == nil && strings.TrimSpace(fmt.Sprint(item.ModelScope["outcome_label_definition_version"])) != evaluation.OutcomeLabelDefinitionVersion {
			policyErr = fmt.Errorf("outcome_label_definition_not_pre_registered")
		}
		if policyErr == nil {
			policy, policyErr = predictionExecutionPolicy(policy, item.ModelScope)
		}
		if !strings.EqualFold(item.AssetClass, "equity") {
			policyErr = fmt.Errorf("asset_class_label_policy_not_implemented")
		}
		if policyErr != nil || item.RawScore == nil {
			reason := "prediction_has_no_score"
			if policyErr != nil {
				reason = policyErr.Error()
			}
			label := excludedPredictionLabel(item, policy, reason)
			created, saveErr := store.Save(ctx, evaluation.PersistedOutcome{PredictionRunID: item.ID, Label: label, ExclusionReason: reason,
				DataQuality: map[string]any{"prediction_status": item.PredictionStatus, "label_policy_status": "excluded_before_price_fetch"}})
			if saveErr != nil {
				failures = appendPredictionOutcomeFailure(failures, item.ID, saveErr)
				continue
			}
			if created {
				summary["excluded"] = summary["excluded"].(int) + 1
			}
			continue
		}

		asset := map[string]any{"asset_id": item.AssetID, "asset_class": item.AssetClass, "market": item.Market, "symbol": item.Symbol, "currency": item.Currency}
		points, fetchErr := runtime.cachedPrices(ctx, asset, item.SignalAvailableAt, now, cache)
		if fetchErr != nil {
			failures = appendPredictionOutcomeFailure(failures, item.ID, fetchErr)
			continue
		}
		window := outcomeWindow(points, item.SignalAvailableAt, "trading_sessions", item.HorizonSessions)
		if len(window) < item.HorizonSessions+1 {
			summary["pending"] = summary["pending"].(int) + 1
			continue
		}
		if !window[0].Adjusted || !window[len(window)-1].Adjusted {
			label := unavailablePredictionLabel(policy, "adjusted_close_missing", time.Now().UTC())
			created, saveErr := store.Save(ctx, evaluation.PersistedOutcome{PredictionRunID: item.ID, Label: label, ExclusionReason: label.Reason,
				DataQuality: map[string]any{"asset_price_status": "unadjusted_rejected", "price_field": policy.PriceField}})
			if saveErr != nil {
				failures = appendPredictionOutcomeFailure(failures, item.ID, saveErr)
			} else if created {
				summary["unavailable"] = summary["unavailable"].(int) + 1
			}
			continue
		}

		benchmarkStatus, benchmarkReason, mappingID, benchmarkAssetID := "unavailable", "missing_point_in_time_mapping", "", ""
		benchmarkCurrency := item.Currency
		benchmarkPoints := []outcomePricePoint{}
		resolution, resolutionErr := marketdata.NewStore(runtime.db).ResolveBenchmark(ctx, marketdata.BenchmarkResolutionRequest{
			AssetID: item.AssetID, Market: item.Market, Currency: item.Currency, EffectiveAt: item.SignalAvailableAt, AvailableAsOf: item.SignalAvailableAt,
		})
		if resolutionErr != nil {
			failures = appendPredictionOutcomeFailure(failures, item.ID, resolutionErr)
			continue
		}
		if resolution.Status == "available" && resolution.Mapping != nil {
			mapping := resolution.Mapping
			mappingID, benchmarkAssetID = mapping.ID, mapping.BenchmarkAssetID
			benchmarkCurrency = mapping.BenchmarkCurrency
			benchmark := map[string]any{"asset_id": mapping.BenchmarkAssetID, "asset_class": mapping.BenchmarkClass, "market": mapping.BenchmarkMarket, "symbol": mapping.BenchmarkSymbol, "currency": mapping.BenchmarkCurrency}
			benchmarkPoints, fetchErr = runtime.cachedPrices(ctx, benchmark, item.SignalAvailableAt, now, cache)
			if fetchErr != nil {
				if policy.Objective == "excess_up" {
					failures = appendPredictionOutcomeFailure(failures, item.ID, fetchErr)
					continue
				}
				benchmarkReason = "benchmark_price_fetch_failed"
				benchmarkPoints = nil
			} else {
				benchmarkStatus, benchmarkReason = "available", ""
			}
		} else {
			benchmarkReason = resolution.Reason
		}

		// The label must become available after both immutable price series have
		// been acquired and persisted, never at the task's earlier start time.
		labelAvailableAt := time.Now().UTC()
		label := evaluation.BuildOutcomeLabel(item.SignalAvailableAt, evaluationPricePoints(points, labelAvailableAt, item.Currency), evaluationPricePoints(benchmarkPoints, labelAvailableAt, benchmarkCurrency), policy)
		if label.Reason == "label_not_mature" {
			summary["pending"] = summary["pending"].(int) + 1
			continue
		}
		if label.Status != "mature" {
			benchmarkStatus = "unavailable"
			if benchmarkReason == "" {
				benchmarkReason = label.Reason
			}
		}
		execution := objectValue(item.ModelScope["execution_assumptions"])
		quality := map[string]any{
			"asset_price_status": "adjusted_close", "benchmark_status": benchmarkStatus, "benchmark_reason": benchmarkReason,
			"entry_policy": policy.EntryPolicy, "exit_policy": policy.ExitPolicy, "neutral_band": policy.NeutralBand,
			"risk_adjustment_status": label.RiskAdjustmentStatus, "simulation_status": label.SimulationStatus,
		}
		created, saveErr := store.Save(ctx, evaluation.PersistedOutcome{PredictionRunID: item.ID, Label: label, BenchmarkAssetID: benchmarkAssetID,
			BenchmarkMappingID: mappingID, ExclusionReason: label.Reason, ExecutionAssumptions: execution, DataQuality: quality})
		if saveErr != nil {
			failures = appendPredictionOutcomeFailure(failures, item.ID, saveErr)
			continue
		}
		if created {
			if label.Status == "mature" {
				summary["matured"] = summary["matured"].(int) + 1
			} else {
				summary["unavailable"] = summary["unavailable"].(int) + 1
			}
		}
	}
	summary["failed"], summary["failures"] = len(failures), failures
	return summary, nil
}

func predictionExecutionPolicy(policy evaluation.HorizonPolicy, scope map[string]any) (evaluation.HorizonPolicy, error) {
	raw, exists := scope["execution_assumptions"]
	if !exists || raw == nil {
		return policy, nil
	}
	body, err := json.Marshal(raw)
	if err != nil {
		return evaluation.HorizonPolicy{}, fmt.Errorf("invalid_execution_assumptions")
	}
	assumptions := evaluation.ExecutionAssumptions{}
	if json.Unmarshal(body, &assumptions) != nil {
		return evaluation.HorizonPolicy{}, fmt.Errorf("invalid_execution_assumptions")
	}
	return evaluation.WithExecutionAssumptions(policy, assumptions)
}

func evaluationPricePoints(points []outcomePricePoint, availableAt time.Time, currency string) []evaluation.PricePoint {
	result := make([]evaluation.PricePoint, 0, len(points))
	for _, point := range points {
		value := point.Close
		item := evaluation.PricePoint{SessionDate: point.ObservedAt, AvailableAt: availableAt.UTC(), Currency: strings.ToUpper(strings.TrimSpace(currency)), Close: &value, CorporateActionAdjusted: point.Adjusted}
		if point.Adjusted {
			item.AdjustedClose = &value
		}
		result = append(result, item)
	}
	return result
}

func unavailablePredictionLabel(policy evaluation.HorizonPolicy, reason string, availableAt time.Time) evaluation.OutcomeLabel {
	return evaluation.OutcomeLabel{Status: "unavailable", Reason: reason, DefinitionVersion: policy.Version, Objective: policy.Objective,
		HorizonSessions: policy.HorizonSessions, PriceField: policy.PriceField, TimePrecision: "daily_close", AlphaDefinition: policy.AlphaDefinition,
		LabelAvailableAt: &availableAt, RelativeLabel: "unavailable", SimulationStatus: "not_configured",
		ResearchResultOnly: true, RiskAdjustmentStatus: "not_configured"}
}

func excludedPredictionLabel(item predictionOutcomeCandidate, policy evaluation.HorizonPolicy, reason string) evaluation.OutcomeLabel {
	label := evaluation.OutcomeLabel{Status: "excluded", Reason: reason, DefinitionVersion: evaluation.OutcomeLabelDefinitionVersion,
		Objective: item.Objective, HorizonSessions: item.HorizonSessions, RelativeLabel: "unavailable", SimulationStatus: "not_configured",
		ResearchResultOnly: true, RiskAdjustmentStatus: "not_configured"}
	if policy.Version != "" {
		label.DefinitionVersion, label.PriceField, label.TimePrecision, label.AlphaDefinition = policy.Version, policy.PriceField, "daily_close", policy.AlphaDefinition
	}
	return label
}

func appendPredictionOutcomeFailure(values []string, id string, cause error) []string {
	if len(values) >= 10 {
		return values
	}
	return append(values, id+": "+truncateRunes(cause.Error(), 240))
}
