package prediction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

const RuleCollectionBaselineVersion = "llm-direction-rule-baseline-v1"

// EnsureRuleCollectionBaselines registers frozen, untrained identity rules for
// forward data collection. They preserve the existing LLM direction score as
// an uncalibrated score, never as a probability, and are permanently excluded
// from the production promotion path.
func (s *Service) EnsureRuleCollectionBaselines(ctx context.Context, effectiveFrom time.Time) ([]signals.BinaryModel, error) {
	if s.db == nil || effectiveFrom.IsZero() {
		return nil, fmt.Errorf("prediction store and baseline effective time are required")
	}
	models := make([]signals.BinaryModel, 0, 3)
	for _, horizon := range []int{1, 5, 20} {
		version := fmt.Sprintf("%s-h%d", RuleCollectionBaselineVersion, horizon)
		model, err := s.loadCollectionModel(ctx, version)
		if err == nil {
			if validateCollectionModel(model, version, horizon) != nil {
				return nil, fmt.Errorf("collection baseline version is bound to an incompatible artifact")
			}
			models = append(models, model)
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		model = signals.BinaryModel{
			Kind: signals.ModelKindFixedRule, Version: version, Objective: "absolute_up", HorizonSessions: horizon,
			TrainingCutoff: effectiveFrom.UTC(), FeatureNames: []string{signals.FeatureLLMDirectionScore},
			Means: map[string]float64{signals.FeatureLLMDirectionScore: 0}, Scales: map[string]float64{signals.FeatureLLMDirectionScore: 1},
			Coefficients: map[string]float64{signals.FeatureLLMDirectionScore: 1}, Intercept: 0, SampleCount: 0,
		}
		scope := map[string]any{
			"asset_class": "equity", "deployment_role": "forward_data_collection_only", "promotion_eligible": false,
			"source_contract": "llm-direction-v3", "probability_status": "unavailable",
			"promotion_policy": PreregisteredPromotionPolicy{MinimumSamples: 100, MinimumShadowDays: 30, MaximumECE: .1,
				ShadowStartedAt: effectiveFrom.UTC(), RollbackTriggers: []string{"source_failure", "forward_quality_decline", "direction_bias"}},
		}
		if err = s.RegisterModel(ctx, ModelRegistration{Model: model, Market: "US", Status: "shadow", ArtifactDigest: ModelArtifactDigest(model), Scope: scope}); err != nil {
			// Another worker may have won registration. Accept only the exact
			// frozen collection kind and horizon on the next read.
			stored, loadErr := s.loadCollectionModel(ctx, version)
			if loadErr != nil || validateCollectionModel(stored, version, horizon) != nil {
				return nil, err
			}
			model = stored
		}
		models = append(models, model)
	}
	return models, nil
}

func validateCollectionModel(model signals.BinaryModel, version string, horizon int) error {
	if model.Version != version || model.Kind != signals.ModelKindFixedRule || model.HorizonSessions != horizon || model.TrainingCutoff.IsZero() {
		return fmt.Errorf("collection baseline identity is invalid")
	}
	return validateModel(model)
}

func (s *Service) loadCollectionModel(ctx context.Context, version string) (signals.BinaryModel, error) {
	var body []byte
	if err := s.db.QueryRow(ctx, `SELECT model_payload::jsonb FROM prediction_models WHERE version=$1`, version).Scan(&body); err != nil {
		return signals.BinaryModel{}, err
	}
	model := signals.BinaryModel{}
	if err := json.Unmarshal(body, &model); err != nil {
		return signals.BinaryModel{}, err
	}
	return model, nil
}
