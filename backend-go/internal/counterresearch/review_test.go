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
