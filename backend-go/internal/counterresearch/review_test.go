package counterresearch

import (
	"testing"
	"time"
)

func TestCounterResearchCountsOriginsNotAgents(t *testing.T) {
	now := time.Now().UTC()
	review := Assess(
		now,
		[]string{"old"},
		[]Evidence{
			{ID: "new-1", OriginID: "filing", AvailableAt: now},
			{ID: "new-2", OriginID: "filing", AvailableAt: now},
			{ID: "future", OriginID: "wire", AvailableAt: now.Add(time.Hour)},
		},
		[]string{"demand may shift"},
		1,
		time.Minute,
		2,
	)
	if review.Status != "error_found" || review.IndependentOriginCount != 1 || len(review.NewEvidenceIDs) != 2 {
		t.Fatalf("review=%#v", review)
	}
}

func TestAssessFindingsRequiresExactClaimNewIndependentAndPITEvidence(t *testing.T) {
	now := time.Date(2026, 9, 9, 2, 0, 0, 0, time.UTC)
	review := AssessFindings(now,
		[]string{"margin expands"}, []string{"baseline"},
		[]Evidence{
			{ID: "baseline", OriginID: "filing", AvailableAt: now.Add(-time.Hour)},
			{ID: "new", OriginID: "supplier", AvailableAt: now.Add(-time.Minute)},
			{ID: "future", OriginID: "wire", AvailableAt: now.Add(time.Minute)},
		},
		[]Finding{
			{ChallengedClaim: "margin expands", CompetingMechanism: "input costs offset revenue", EvidenceIDs: []string{"new"}},
			{ChallengedClaim: "unknown claim", CompetingMechanism: "invalid", EvidenceIDs: []string{"new"}},
			{ChallengedClaim: "margin expands", CompetingMechanism: "future leak", EvidenceIDs: []string{"future"}},
		}, time.Second, 0)
	if review.Status != "candidate_error_found" || review.CandidateErrorsFound != 1 || review.ErrorsFound != 0 || review.IndependentOriginCount != 1 {
		t.Fatalf("review=%#v", review)
	}
	if len(review.NewEvidenceIDs) != 1 || review.NewEvidenceIDs[0] != "new" || len(review.ChallengedClaims) != 1 {
		t.Fatalf("review=%#v", review)
	}
}

func TestSummarizeAblationUsesOnlyLabelledOutcomes(t *testing.T) {
	result := SummarizeAblation([]AblationObservation{
		{BaselineWrong: true, ErrorFound: true, Latency: 2 * time.Second, Cost: 1},
		{BaselineWrong: true, ErrorFound: false, Latency: time.Second, Cost: 3},
		{BaselineWrong: false, ErrorFound: true, Latency: 3 * time.Second, Cost: 2},
	})
	if result.ErrorDiscoveryRate == nil || *result.ErrorDiscoveryRate != .5 || result.FalseChallenges != 1 || result.MeanAddedLatencyMS != 2000 || result.MeanAddedCost != 2 {
		t.Fatalf("ablation=%#v", result)
	}
}
