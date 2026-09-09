package prediction

import (
	"testing"
	"time"
)

func TestFailureDrillsDegradeOrAlertWithoutProductionMutation(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	for _, scenario := range []string{"data_source_unavailable", "model_timeout", "feature_distribution_drift", "calibration_expired", "manual_rollback_gate"} {
		result, err := RunFailureDrill(scenario, now)
		if err != nil {
			t.Fatalf("%s: %v", scenario, err)
		}
		if !result.Passed || result.ProductionStateChanged {
			t.Fatalf("unsafe %s drill: %#v", scenario, result)
		}
		if automatic, _ := result.Evidence["automatic_model_switch"].(bool); automatic {
			t.Fatalf("%s drill enabled an automatic model switch", scenario)
		}
	}
}

func TestFailureDrillRejectsUnknownScenario(t *testing.T) {
	if _, err := RunFailureDrill("unknown", time.Now().UTC()); err == nil {
		t.Fatal("unknown failure drill scenario was accepted")
	}
}

func TestPromotionPolicyMustBePreregisteredWithRollbackTriggers(t *testing.T) {
	if _, err := promotionPolicyFromScope(map[string]any{}); err == nil {
		t.Fatal("missing preregistered promotion policy was accepted")
	}
	policy := PreregisteredPromotionPolicy{MinimumSamples: 30, MinimumShadowDays: 7, MaximumECE: .1,
		ShadowStartedAt: time.Now().UTC(), RollbackTriggers: []string{"forward_quality_decline"}}
	parsed, err := promotionPolicyFromScope(map[string]any{"promotion_policy": policy})
	if err != nil || parsed.MinimumSamples != 30 || len(parsed.RollbackTriggers) != 1 {
		t.Fatalf("valid preregistered promotion policy rejected: %#v err=%v", parsed, err)
	}
}
