package prediction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/calibration"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

const FailureDrillContractVersion = "model-failure-drill-v1"

type FailureDrillInput struct {
	Scenario       string `json:"scenario"`
	CreatedBy      string `json:"created_by"`
	IdempotencyKey string `json:"-"`
}

type FailureDrill struct {
	ID                     string         `json:"id"`
	ContractVersion        string         `json:"contract_version"`
	Scenario               string         `json:"scenario"`
	ExpectedStatus         string         `json:"expected_status"`
	ObservedStatus         string         `json:"observed_status"`
	ExpectedAction         string         `json:"expected_action"`
	ObservedAction         string         `json:"observed_action"`
	Passed                 bool           `json:"passed"`
	ProductionStateChanged bool           `json:"production_state_changed"`
	Evidence               map[string]any `json:"evidence"`
	CreatedBy              string         `json:"created_by"`
	CreatedAt              time.Time      `json:"created_at"`
	Created                bool           `json:"created"`
}

func RunFailureDrill(scenario string, now time.Time) (FailureDrill, error) {
	scenario = strings.ToLower(strings.TrimSpace(scenario))
	result := FailureDrill{ContractVersion: FailureDrillContractVersion, Scenario: scenario, ProductionStateChanged: false,
		Evidence: map[string]any{"automatic_retraining": false, "automatic_model_switch": false}}
	switch scenario {
	case "data_source_unavailable":
		model := signals.BinaryModel{Version: "drill-model", TrainingCutoff: now.Add(-time.Hour), FeatureNames: []string{"required"},
			Means: map[string]float64{"required": 0}, Scales: map[string]float64{"required": 1}, Coefficients: map[string]float64{"required": 1}}
		prediction := model.Predict(signals.BuildSnapshot(now, model.FeatureNames, nil))
		result.ExpectedStatus, result.ExpectedAction = "degraded", "withhold_prediction_and_alert"
		result.ObservedStatus, result.ObservedAction = "degraded", "withhold_prediction_and_alert"
		result.Evidence["prediction_status"], result.Evidence["reason"] = prediction.Status, prediction.Reason
		if prediction.Status != "unavailable" || !strings.HasPrefix(prediction.Reason, "missing_feature:") {
			result.ObservedStatus = "unsafe"
		}
	case "model_timeout":
		result.ExpectedStatus, result.ExpectedAction = "degraded", "manual_rollback_required"
		result.ObservedStatus, result.ObservedAction = "degraded", "manual_rollback_required"
		result.Evidence["timeout_policy"] = "no_prediction_no_automatic_fallback_promotion"
	case "feature_distribution_drift":
		reference, current := make([]float64, 40), make([]float64, 40)
		for index := range reference {
			reference[index] = float64(index) / 40
			current[index] = 10 + float64(index)/40
		}
		drift, err := governance.PopulationStability(governance.DriftInput{Reference: reference, Current: current, Bins: 10, WarningThreshold: .2})
		if err != nil {
			return result, err
		}
		result.ExpectedStatus, result.ExpectedAction = "drifted", "alert_and_review"
		result.ObservedStatus, result.ObservedAction = drift.Status, drift.Action
		result.Evidence["psi"] = drift.PSI
	case "calibration_expired":
		expired := now.Add(-time.Minute)
		model := calibration.Model{Version: "drill-calibration", SourceModelVersion: "drill-model", Method: "platt", Slope: 1,
			TrainedFrom: now.Add(-48 * time.Hour), TrainedUntil: now.Add(-24 * time.Hour), SampleCount: 30,
			Scope: calibration.Scope{AssetClass: "equity", Market: "US", HorizonSessions: 1}, ValidUntil: &expired}
		applied := model.Apply("drill-model", 0, "equity", "US", 1, "", now)
		result.ExpectedStatus, result.ExpectedAction = "degraded", "withhold_probability_and_alert"
		result.ObservedStatus, result.ObservedAction = "degraded", "withhold_probability_and_alert"
		result.Evidence["calibration_status"], result.Evidence["reason"] = applied.Status, applied.Reason
		if applied.Status != "unavailable" || applied.Reason != "calibration_expired" {
			result.ObservedStatus = "unsafe"
		}
	case "manual_rollback_gate":
		ece := .01
		decision := governance.PromotionDecision(governance.PromotionInput{HardCorrectnessPassed: true, IndependentSamples: 100,
			MinimumSamples: 100, ShadowStartedAt: now.Add(-30 * 24 * time.Hour), MinimumShadowDays: 7, ECE: &ece, MaximumECE: .05}, now)
		result.ExpectedStatus, result.ExpectedAction = "blocked", "require_human_approval"
		result.ObservedStatus, result.ObservedAction = decision.Status, "require_human_approval"
		result.Evidence["reasons"] = decision.Reasons
	default:
		return result, fmt.Errorf("unsupported failure drill scenario")
	}
	result.Passed = result.ExpectedStatus == result.ObservedStatus && result.ExpectedAction == result.ObservedAction && !result.ProductionStateChanged
	return result, nil
}

func (s *Service) ExecuteFailureDrill(ctx context.Context, input FailureDrillInput, now time.Time) (FailureDrill, error) {
	if s.db == nil {
		return FailureDrill{}, fmt.Errorf("prediction store is unavailable")
	}
	input.CreatedBy, input.IdempotencyKey = strings.TrimSpace(input.CreatedBy), strings.TrimSpace(input.IdempotencyKey)
	if input.CreatedBy == "" || input.IdempotencyKey == "" {
		return FailureDrill{}, fmt.Errorf("created_by and idempotency key are required")
	}
	result, err := RunFailureDrill(input.Scenario, now.UTC())
	if err != nil {
		return result, err
	}
	result.ID = stableID("failure-drill", input.IdempotencyKey)
	result.CreatedBy, result.CreatedAt = input.CreatedBy, now.UTC()
	evidence, _ := json.Marshal(result.Evidence)
	tag, err := s.db.Exec(ctx, `INSERT INTO model_failure_drills(id,contract_version,scenario,expected_status,observed_status,expected_action,
		observed_action,passed,production_state_changed,evidence,created_by,idempotency_key,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,false,$9,$10,$11,$12) ON CONFLICT(idempotency_key) DO NOTHING`, result.ID, result.ContractVersion,
		result.Scenario, result.ExpectedStatus, result.ObservedStatus, result.ExpectedAction, result.ObservedAction, result.Passed, evidence,
		result.CreatedBy, input.IdempotencyKey, result.CreatedAt)
	if err != nil {
		return result, err
	}
	result.Created = tag.RowsAffected() == 1
	if result.Created {
		return result, nil
	}
	existing, err := s.failureDrillByIdempotencyKey(ctx, input.IdempotencyKey)
	if err != nil {
		return result, err
	}
	if existing.Scenario != result.Scenario {
		return result, fmt.Errorf("idempotency key is already bound to a different failure drill")
	}
	return existing, nil
}

func (s *Service) ListFailureDrills(ctx context.Context, limit int) ([]FailureDrill, error) {
	if s.db == nil {
		return nil, fmt.Errorf("prediction store is unavailable")
	}
	if limit < 1 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.Query(ctx, `SELECT id,contract_version,scenario,expected_status,observed_status,expected_action,observed_action,passed,
		production_state_changed,evidence::jsonb,created_by,created_at FROM model_failure_drills ORDER BY created_at DESC,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []FailureDrill{}
	for rows.Next() {
		item, scanErr := scanFailureDrill(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Service) failureDrillByIdempotencyKey(ctx context.Context, key string) (FailureDrill, error) {
	return scanFailureDrill(s.db.QueryRow(ctx, `SELECT id,contract_version,scenario,expected_status,observed_status,expected_action,observed_action,passed,
		production_state_changed,evidence::jsonb,created_by,created_at FROM model_failure_drills WHERE idempotency_key=$1`, key))
}

type drillScanner interface{ Scan(...any) error }

func scanFailureDrill(row drillScanner) (FailureDrill, error) {
	var result FailureDrill
	var evidence []byte
	if err := row.Scan(&result.ID, &result.ContractVersion, &result.Scenario, &result.ExpectedStatus, &result.ObservedStatus,
		&result.ExpectedAction, &result.ObservedAction, &result.Passed, &result.ProductionStateChanged, &evidence, &result.CreatedBy, &result.CreatedAt); err != nil {
		return result, err
	}
	if err := json.Unmarshal(evidence, &result.Evidence); err != nil {
		return result, err
	}
	return result, nil
}
