package httpapi

import (
	"testing"
	"time"
)

func TestCeleryQueueKeyUsesKombuPrioritySteps(t *testing.T) {
	tests := map[int]string{
		-1: "research", 0: "research", 1: "research", 2: "research",
		3: "research" + celeryPrioritySeparator + "3", 5: "research" + celeryPrioritySeparator + "3",
		6: "research" + celeryPrioritySeparator + "6", 8: "research" + celeryPrioritySeparator + "6",
		9: "research" + celeryPrioritySeparator + "9", 99: "research" + celeryPrioritySeparator + "9",
	}
	for priority, expected := range tests {
		if actual := celeryQueueKey("research", priority); actual != expected {
			t.Fatalf("priority %d: got %q, want %q", priority, actual, expected)
		}
	}
}

func TestBatchThreeCanonicalTargetAndDualHorizonTrend(t *testing.T) {
	canonical := canonicalizeGoTarget("Global Cryptocurrency Market Sentiment", "economy", nil, nil)
	if canonical.Key != "sector:digital_assets" || canonical.Label != "数字资产" || canonical.TargetType != "sector" {
		t.Fatalf("unexpected canonical target: %+v", canonical)
	}
	now := time.Now().UTC()
	values := make([]targetObservation, 0, 6)
	for _, age := range []int{25, 20, 15, 10, 8} {
		values = append(values, targetObservation{
			OccurredAt: now.Add(-time.Duration(age) * 24 * time.Hour), Score: 69,
			RatingConfidence: .8, NewsConfidence: .85, Persistence: .8, RealizationProbability: .8,
		})
	}
	values = append(values, targetObservation{
		OccurredAt: now, Score: -50, RatingConfidence: .3, NewsConfidence: .85,
		Persistence: .8, RealizationProbability: .8, Insufficient: true, Provisional: true,
	})
	trend := aggregateTargetTrend(values, now)
	if trend.Long.EventCount != 6 || trend.Long.EligibleCount != 5 || trend.Long.IgnoredCount != 1 {
		t.Fatalf("unexpected long-horizon counts: %+v", trend.Long)
	}
	if trend.Long.Rating != "bullish" || trend.Short.Rating != "bearish" || trend.Combined.Rating != "bullish" || !trend.Short.Provisional {
		t.Fatalf("unexpected dual-horizon trend: %+v", trend)
	}
}

func TestMergeConcreteTargetChangesIncludesCommoditiesAndKeepsLatestKey(t *testing.T) {
	older := time.Date(2026, 8, 29, 7, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	asset := map[string]any{"key": "equity:NASDAQ:MRNA", "changed_at": older, "change_detail_id": "00000000-0000-0000-0000-000000000001"}
	oldCommodity := map[string]any{"key": "commodity_price:oil", "changed_at": older, "change_detail_id": "00000000-0000-0000-0000-000000000002"}
	newCommodity := map[string]any{"key": "commodity_price:oil", "changed_at": newer, "change_detail_id": "00000000-0000-0000-0000-000000000003"}

	changes := mergeConcreteTargetChanges(
		[]map[string]any{asset, oldCommodity},
		[]map[string]any{newCommodity},
	)
	if len(changes) != 2 {
		t.Fatalf("got %d concrete targets, want 2", len(changes))
	}
	if changes[0]["change_detail_id"] != newCommodity["change_detail_id"] || changes[1]["change_detail_id"] != asset["change_detail_id"] {
		t.Fatalf("unexpected concrete target order: %+v", changes)
	}
}
