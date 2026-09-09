package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/prediction"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

// collectRuleBaselinePrediction creates only forward, uncalibrated prediction
// records from a recommendation produced by the current evidence contract.
// The task is enqueued atomically with that recommendation, so it never scans
// or backfills historical recommendations after their outcomes are visible.
func (runtime *outcomeRuntime) collectRuleBaselinePrediction(ctx context.Context, job Job) (any, error) {
	envelope := taskEnvelope{}
	if err := json.Unmarshal(job.Payload, &envelope); err != nil {
		return nil, err
	}
	recommendationID := ""
	if len(envelope.Args) > 0 {
		recommendationID = strings.TrimSpace(stringValue(envelope.Args[0]))
	}
	if recommendationID == "" {
		return nil, fmt.Errorf("rule baseline collection requires recommendation_id")
	}
	var assetID, assetClass, market, eventID, eventType string
	var score int
	var body []byte
	err := runtime.db.QueryRow(ctx, `SELECT recommendation.asset_id,asset.asset_class,asset.market,recommendation.score,recommendation.payload::jsonb,
		coalesce(run.event_id,''),coalesce(event.event_type,'')
		FROM recommendations recommendation JOIN assets asset ON asset.id=recommendation.asset_id
		LEFT JOIN research_runs run ON run.id=recommendation.run_id LEFT JOIN news_events event ON event.id=run.event_id
		WHERE recommendation.id=$1`, recommendationID).Scan(&assetID, &assetClass, &market, &score, &body, &eventID, &eventType)
	if err != nil {
		return nil, err
	}
	recommendation := map[string]any{}
	if err = json.Unmarshal(body, &recommendation); err != nil {
		return nil, err
	}
	if stringValue(recommendation["scoring_version"]) != "llm-direction-v3" {
		return map[string]any{"status": "skipped", "reason": "recommendation_contract_not_eligible", "recommendation_id": recommendationID}, nil
	}
	if !strings.EqualFold(assetClass, "equity") || !strings.EqualFold(market, "US") {
		return map[string]any{"status": "skipped", "reason": "first_market_collection_is_us_equity_only", "recommendation_id": recommendationID}, nil
	}
	signalAt := parseTime(recommendation["signal_available_at"])
	if signalAt.IsZero() {
		signalAt = parseTime(objectValue(recommendation["event_signal"])["signal_available_at"])
	}
	if signalAt.IsZero() || signalAt.After(time.Now().UTC()) {
		return map[string]any{"status": "skipped", "reason": "valid_signal_available_at_required", "recommendation_id": recommendationID}, nil
	}
	service := prediction.New(runtime.db)
	models, err := service.EnsureRuleCollectionBaselines(ctx, signalAt)
	if err != nil {
		return nil, err
	}
	value := mathMax(-1, mathMin(1, float64(score)/100))
	feature := signals.Feature{Name: signals.FeatureLLMDirectionScore, Value: &value, AvailableAt: signalAt, SourceIDs: []string{"recommendation:" + recommendationID}}
	created, existing, skipped := 0, 0, 0
	runs := make([]prediction.Run, 0, len(models))
	for _, model := range models {
		if signalAt.Before(model.TrainingCutoff) {
			skipped++
			continue
		}
		run, predictErr := service.Predict(ctx, prediction.Input{AssetID: assetID, AssetClass: assetClass, EventID: eventID,
			SignalAvailableAt: signalAt, ModelVersion: model.Version, Market: market, EventType: eventType, Features: []signals.Feature{feature}})
		if predictErr != nil {
			return nil, predictErr
		}
		if run.Created {
			created++
		} else {
			existing++
		}
		runs = append(runs, run)
	}
	return map[string]any{
		"status": "completed", "contract_version": prediction.RuleCollectionBaselineVersion, "recommendation_id": recommendationID,
		"created": created, "existing": existing, "skipped": skipped, "runs": runs,
		"trained_sample_count": 0, "calibrated_probability": false, "promotion_eligible": false, "historical_backfill": false,
	}, nil
}

func mathMin(left, right float64) float64 {
	if left < right {
		return left
	}
	return right
}

func mathMax(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
