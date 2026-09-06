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

func TestSignalAvailableAtWaitsForEventObservationAndAllEvidence(t *testing.T) {
	generated := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	event := map[string]any{
		"published_at": iso(generated.Add(-10 * time.Minute)),
		"observed_at":  iso(generated.Add(5 * time.Minute)),
		"as_of":        iso(generated.Add(-10 * time.Minute)),
	}
	evidence := []researchEvidence{{ID: "e-1", PublishedAt: generated.Add(-10 * time.Minute), ObservedAt: generated.Add(3 * time.Minute), AsOf: generated.Add(-10 * time.Minute)}}
	available := signalAvailableAt(event, evidence, generated)
	if !available.Equal(generated.Add(5 * time.Minute)) {
		t.Fatalf("signal availability=%s want event observation %s", available, generated.Add(5*time.Minute))
	}
	contract := eventSignalContract(45, "bullish", "directional", 90, generated, available)
	if contract["time_contract_version"] != p0TimeContractVersion {
		t.Fatalf("event signal did not expose the time contract version: %#v", contract)
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
