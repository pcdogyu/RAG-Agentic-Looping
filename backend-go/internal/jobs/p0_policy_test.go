package jobs

import (
	"testing"
	"time"
)

func TestP0ContractDoesNotPublishProbabilitiesOrFundamentalRating(t *testing.T) {
	asOf := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	value := p0ResultContract(45, "bullish", "directional", 90, asOf, asOf, .8, draftVerification{StructurallyValid: true, EvidenceComplete: true})
	if objectValue(value["fundamental_rating"])["status"] != "unavailable" || objectValue(value["short_term_prediction"])["probabilities"] != nil {
		t.Fatalf("P0 contract exposed unavailable fields: %#v", value)
	}
	signal := objectValue(value["event_signal"])
	if int(numberValue(signal["direction_score"])) != 45 || stringValue(signal["rating"]) != "bullish" {
		t.Fatalf("event signal contract lost its semantic fields: %#v", signal)
	}
}

func TestImpactEligibilityAllowsVerifiedNegativeResearch(t *testing.T) {
	asset := map[string]any{"asset_class": "equity"}
	value := impactEligibility(asset, eventImpactDraft{ConclusionStatus: "directional", DirectionScore: -35}, true)
	if !boolValue(value["research_eligible"]) || !boolValue(value["short_eligible"]) || boolValue(value["long_eligible"]) {
		t.Fatalf("negative verified impact was not eligible for research: %#v", value)
	}
}

func TestPolicyInputSnapshotKeepsEvidenceButNotPrompts(t *testing.T) {
	run := map[string]any{"as_of": "2026-09-06T00:00:00Z", "research_profile": "deep", "evidence": []any{map[string]any{"claim": "issuer statement"}}, "messages": []any{"must-not-be-copied"}}
	value := policyInputSnapshot("event-1", "asset-1", run, "p0-evidence-v1", "qwen2.5:7b")
	if len(anySlice(value["evidence"])) != 1 || value["messages"] != nil || stringValue(value["model"]) != "qwen2.5:7b" {
		t.Fatalf("policy input snapshot was not bounded to structured audit data: %#v", value)
	}
}
