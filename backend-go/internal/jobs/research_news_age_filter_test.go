package jobs

import (
	"testing"
	"time"
)

func TestResearchNewsExpiredUsesFortyEightHourBoundary(t *testing.T) {
	now := time.Date(2026, time.September, 3, 8, 0, 0, 0, time.UTC)
	filter := DefaultResearchNewsAgeFilter()
	if ResearchNewsExpired(filter, now.Add(-47*time.Hour), now) {
		t.Fatal("47 hour old news should remain eligible")
	}
	if ResearchNewsExpired(filter, now.Add(-48*time.Hour), now) {
		t.Fatal("exactly 48 hour old news should remain eligible")
	}
	if !ResearchNewsExpired(filter, now.Add(-48*time.Hour-time.Nanosecond), now) {
		t.Fatal("news older than 48 hours should be filtered")
	}
	filter.Enabled = false
	if !ResearchNewsExpired(filter, now.Add(-49*time.Hour), now) {
		t.Fatal("legacy disabled setting must not bypass news expiry")
	}
}

func TestMarkResearchNewsAgeFiltered(t *testing.T) {
	published := time.Date(2026, time.September, 2, 7, 0, 0, 0, time.UTC)
	run := map[string]any{"analysis_steps": []any{}}
	markResearchNewsAgeFiltered(run, published)
	if run["status"] != "filtered" || run["retryable_reason"] != "news_age_filtered" {
		t.Fatalf("unexpected filtered run: %#v", run)
	}
	if run["error"] != newsAgeFilteredMessage {
		t.Fatalf("filtered reason must be recorded: %#v", run)
	}
}
