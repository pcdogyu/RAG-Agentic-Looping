package httpapi

import (
	"reflect"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
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

func TestNormalizeEventRestoresLegacyAssetDefaults(t *testing.T) {
	event := map[string]any{"candidates": []any{map[string]any{"asset": map[string]any{
		"asset_id": "equity:NASDAQ:TEST", "symbol": "TEST",
	}}}}
	normalizeEvent(event)
	asset := event["candidates"].([]any)[0].(map[string]any)["asset"].(map[string]any)
	for _, key := range []string{"sector_id", "industry_id", "raw_sector", "raw_industry", "instrument_type", "market_cap", "market_cap_rank", "last_synced_at"} {
		if _, ok := asset[key]; !ok {
			t.Fatalf("asset default %s is missing", key)
		}
	}
}

func TestNormalizeNewsExtractionItemMatchesPythonResponseModel(t *testing.T) {
	item := map[string]any{
		"instance_id": "extract-0",
		"queued_at":   "2026-08-31T01:16:10.031826+00:00",
		"updated_at":  "2026-08-31T01:21:06.032377+00:00",
	}
	normalized := normalizeNewsExtractionItem(item)
	if _, ok := normalized["instance_id"]; ok {
		t.Fatal("instance_id must be excluded by the Python response model")
	}
	if normalized["queued_at"] != "2026-08-31T01:16:10.031826Z" || normalized["updated_at"] != "2026-08-31T01:21:06.032377Z" {
		t.Fatalf("timestamps were not normalized: %+v", normalized)
	}
}

func TestResearchQueueOrderingMatchesPython(t *testing.T) {
	items := []map[string]any{
		{"asset_id": "queued-new", "status": "queued", "queued_at": "2026-08-31T02:00:00Z", "updated_at": "2026-08-31T02:00:00Z"},
		{"asset_id": "running", "status": "running", "queued_at": "2026-08-31T00:00:00Z", "updated_at": "2026-08-31T03:00:00Z"},
		{"asset_id": "queued-old", "status": "queued", "queued_at": "2026-08-31T01:00:00Z", "updated_at": "2026-08-31T01:00:00Z"},
		{"asset_id": "verifying", "status": "verifying", "queued_at": "2026-08-30T23:00:00Z", "updated_at": "2026-08-31T04:00:00Z"},
	}
	sortResearchQueueItems(items, map[string]int{"queued": 1, "running": 2, "verifying": 3})
	got := []string{stringValue(items[0]["asset_id"]), stringValue(items[1]["asset_id"]), stringValue(items[2]["asset_id"]), stringValue(items[3]["asset_id"])}
	want := []string{"queued-old", "queued-new", "verifying", "running"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCNNewsDefaultsEncodeEmptyLists(t *testing.T) {
	server := &Server{cfg: config.Config{}}
	value := server.defaultFactGroupConfig("cn_news")
	for _, key := range []string{"rss_feed_urls", "official_rss_feed_urls"} {
		items, ok := value[key].([]string)
		if !ok || items == nil || len(items) != 0 {
			t.Fatalf("%s must be a non-nil empty list, got %#v", key, value[key])
		}
	}
}
