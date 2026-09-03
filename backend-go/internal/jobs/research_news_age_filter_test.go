package jobs

import (
	"testing"
	"time"
)

func TestResearchNewsExpiredUsesTwentyFourHourBoundary(t *testing.T) {
	now := time.Date(2026, time.September, 3, 8, 0, 0, 0, time.UTC)
	filter := DefaultResearchNewsAgeFilter()
	if ResearchNewsExpired(filter, now.Add(-23*time.Hour), now) {
		t.Fatal("23 hour old news should remain eligible")
	}
	if !ResearchNewsExpired(filter, now.Add(-24*time.Hour), now) {
		t.Fatal("24 hour old news should be filtered")
	}
	filter.Enabled = false
	if ResearchNewsExpired(filter, now.Add(-48*time.Hour), now) {
		t.Fatal("disabled filter should not expire news")
	}
}

func TestMarkResearchNewsAgeFilteredKeepsManualRetryPath(t *testing.T) {
	published := time.Date(2026, time.September, 2, 7, 0, 0, 0, time.UTC)
	run := map[string]any{"analysis_steps": []any{}}
	markResearchNewsAgeFiltered(run, published)
	if run["status"] != "filtered" || run["retryable_reason"] != "news_age_filtered" {
		t.Fatalf("unexpected filtered run: %#v", run)
	}
	if researchNewsAgeFilterBypass(run) {
		t.Fatalf("automatic filtered run must not gain a bypass: %#v", run)
	}
}
