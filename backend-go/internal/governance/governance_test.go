package governance

import (
	"testing"
	"time"
)

func TestPromotionRequiresIndependentEvidenceAndHumanApproval(t *testing.T) {
	now := time.Now().UTC()
	ece := .03
	input := PromotionInput{HardCorrectnessPassed: true, IndependentSamples: 200, MinimumSamples: 100, ShadowStartedAt: now.Add(-30 * 24 * time.Hour), MinimumShadowDays: 14, ECE: &ece, MaximumECE: .08}
	if decision := PromotionDecision(input, now); decision.Status != "blocked" || !has(decision.Reasons, "human_approval_required") {
		t.Fatalf("decision=%#v", decision)
	}
	input.ApprovedBy = "reviewer"
	if decision := PromotionDecision(input, now); decision.Status != "approved" {
		t.Fatalf("decision=%#v", decision)
	}
}

func TestDriftOnlyRequestsReview(t *testing.T) {
	reference, current := []float64{}, []float64{}
	for index := 0; index < 40; index++ {
		reference = append(reference, float64(index%5))
		current = append(current, float64(index%5)+10)
	}
	result, err := PopulationStability(DriftInput{Reference: reference, Current: current, WarningThreshold: .1})
	if err != nil || result.Status != "drifted" || result.Action != "alert_and_review" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestPromotionRejectsInvalidThresholdsAndWhitespaceApproval(t *testing.T) {
	now := time.Now().UTC()
	ece := .01
	decision := PromotionDecision(PromotionInput{HardCorrectnessPassed: true, IndependentSamples: 100, MinimumSamples: 30, ShadowStartedAt: now.AddDate(0, 0, -30), MinimumShadowDays: 0, ECE: &ece, MaximumECE: 0, ApprovedBy: "   "}, now)
	for _, reason := range []string{"invalid_governance_thresholds", "human_approval_required"} {
		if !has(decision.Reasons, reason) {
			t.Fatalf("missing %s in %#v", reason, decision)
		}
	}
}

func has(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
