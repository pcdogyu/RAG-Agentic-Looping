package httpapi

import (
	"reflect"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

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

func TestCanonicalTargetMergesUnitedStatesAliases(t *testing.T) {
	for _, name := range []string{"美国股市", "美国", "美国经济", "US Economy", "US Equity Market"} {
		got := canonicalizeGoTarget(name, "economy", nil, nil)
		if got.Key != "economy:us" || got.Label != "美国经济" || got.TargetType != "economy" {
			t.Fatalf("%q was not canonicalized to the US economy: %+v", name, got)
		}
	}
}

func TestPublishedSecurityResolverUsesSelectedMasterAssetsAndDeduplicates(t *testing.T) {
	assets := []map[string]any{
		{"asset_id": "equity:NASDAQ:NVDA", "asset_class": "equity", "symbol": "NVDA", "name": "NVIDIA Corporation", "aliases": []any{"NVIDIA"}, "association_tier": "standard", "instrument_type": "common_stock", "market_cap": float64(100), "active": true},
		{"asset_id": "crypto:coingecko:spacex-prestocks-2", "asset_class": "crypto", "symbol": "SPACEX", "name": "SpaceX PreStocks", "aliases": []any{"SpaceX"}, "association_tier": "exact_only", "market_cap": float64(10), "active": true},
		{"asset_id": "crypto:other:spacex", "asset_class": "crypto", "symbol": "SPCX", "name": "SpaceX Token", "association_tier": "manual_only", "active": true},
	}
	impacts := []any{
		map[string]any{"target_name": "NVIDIA", "target_type": "economy", "rating": "bullish", "direction_score": float64(40), "rating_confidence": .7},
		map[string]any{"target_name": "NVIDIA 股价", "target_type": "tradable_asset", "rating": "bearish", "direction_score": float64(-70), "rating_confidence": .8},
		map[string]any{"target_name": "SPACEX", "target_type": "economy", "rating": "bullish", "direction_score": float64(20), "rating_confidence": .6},
	}
	got := resolvePublishedSecurityImpacts(impacts, assets)
	if len(got) != 2 {
		t.Fatalf("expected deduplicated NVIDIA and SpaceX impacts, got %#v", got)
	}
	first, second := objectValue(got[0]), objectValue(got[1])
	if stringValue(objectValue(first["asset"])["asset_id"]) != "equity:NASDAQ:NVDA" || stringValue(first["target_type"]) != "tradable_asset" || stringValue(first["rating"]) != "bearish" {
		t.Fatalf("NVIDIA was not rebound/deduplicated to NVDA: %#v", first)
	}
	if stringValue(objectValue(second["asset"])["asset_id"]) != "crypto:coingecko:spacex-prestocks-2" || stringValue(second["target_type"]) != "tradable_asset" {
		t.Fatalf("SpaceX was not rebound to the selected exact asset: %#v", second)
	}
}

func TestRoundPlacesUsesTiesToEven(t *testing.T) {
	if got, want := roundPlaces(0.44325, 4), 0.4432; got != want {
		t.Fatalf("roundPlaces ties-to-even = %v, want %v", got, want)
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

func TestMergeConcreteTargetChangesPreservesInputOrderForExactTies(t *testing.T) {
	stamp := time.Date(2026, 8, 31, 1, 2, 13, 0, time.UTC)
	id := "00000000-0000-0000-0000-000000000001"
	crude := map[string]any{"key": "commodity:fmp:CLUSD", "changed_at": stamp, "change_detail_id": id}
	brent := map[string]any{"key": "commodity:fmp:BZUSD", "changed_at": stamp, "change_detail_id": id}

	changes := mergeConcreteTargetChanges([]map[string]any{crude, brent})
	if len(changes) != 2 || changes[0]["key"] != crude["key"] || changes[1]["key"] != brent["key"] {
		t.Fatalf("exact ties must preserve source order: %+v", changes)
	}
}

func TestNormalizeEventRestoresLegacyAssetDefaults(t *testing.T) {
	event := map[string]any{
		"actions":      nil,
		"published_at": "2026-08-31T01:16:10.031826987Z",
		"candidates": []any{map[string]any{"asset": map[string]any{
			"asset_id": "equity:NASDAQ:TEST", "symbol": "TEST",
		}}},
		"analysis_steps": []any{map[string]any{
			"occurred_at": "2026-08-31T01:21:06.032377654Z",
		}},
	}
	normalizeEvent(event)
	asset := event["candidates"].([]any)[0].(map[string]any)["asset"].(map[string]any)
	for _, key := range []string{"sector_id", "industry_id", "raw_sector", "raw_industry", "instrument_type", "market_cap", "market_cap_rank", "last_synced_at"} {
		if _, ok := asset[key]; !ok {
			t.Fatalf("asset default %s is missing", key)
		}
	}
	if industries, ok := event["industry_ids"].([]any); !ok || industries == nil || len(industries) != 0 {
		t.Fatalf("industry_ids must be a non-nil empty list, got %#v", event["industry_ids"])
	}
	if actions, ok := event["actions"].([]any); !ok || actions == nil || len(actions) != 0 {
		t.Fatalf("actions must be a non-nil empty list, got %#v", event["actions"])
	}
	if event["published_at"] != "2026-08-31T01:16:10.031826Z" {
		t.Fatalf("event timestamp was not normalized: %#v", event["published_at"])
	}
	step := event["analysis_steps"].([]any)[0].(map[string]any)
	if step["occurred_at"] != "2026-08-31T01:21:06.032377Z" {
		t.Fatalf("analysis timestamp was not normalized: %#v", step["occurred_at"])
	}
}

func TestNormalizeRunTimestampsUsesMicrosecondPrecision(t *testing.T) {
	run := map[string]any{
		"as_of":      "2026-08-31T07:00:26.528478612Z",
		"created_at": "2026-08-31T07:00:48.439129883Z",
		"analysis_steps": []any{map[string]any{
			"occurred_at": "2026-08-31T07:00:48.400694566Z",
			"metrics": map[string]any{
				"published_at": "2026-08-31T07:00:48.123456789+00:00",
			},
		}},
	}

	normalizeRunTimestamps(run)
	if run["as_of"] != "2026-08-31T07:00:26.528478Z" || run["created_at"] != "2026-08-31T07:00:48.439129Z" {
		t.Fatalf("run timestamps were not normalized: %+v", run)
	}
	step := run["analysis_steps"].([]any)[0].(map[string]any)
	if step["occurred_at"] != "2026-08-31T07:00:48.400694Z" {
		t.Fatalf("step timestamp was not normalized: %+v", step)
	}
	metrics := step["metrics"].(map[string]any)
	if metrics["published_at"] != "2026-08-31T07:00:48.123456Z" {
		t.Fatalf("nested timestamp was not normalized: %+v", metrics)
	}
}

func TestNormalizeNewsExtractionItemMatchesAPIResponse(t *testing.T) {
	item := map[string]any{
		"instance_id": "extract-0",
		"queued_at":   "2026-08-31T01:16:10.031826+00:00",
		"updated_at":  "2026-08-31T01:21:06.032377+00:00",
	}
	normalized := normalizeNewsExtractionItem(item)
	if _, ok := normalized["instance_id"]; ok {
		t.Fatal("instance_id must be excluded by the API response")
	}
	if normalized["queued_at"] != "2026-08-31T01:16:10.031826Z" || normalized["updated_at"] != "2026-08-31T01:21:06.032377Z" {
		t.Fatalf("timestamps were not normalized: %+v", normalized)
	}
}

func TestResearchQueueOrderingIsStable(t *testing.T) {
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

func TestEventUpdatedAtUsesLatestAnalysisStep(t *testing.T) {
	event := map[string]any{
		"observed_at": "2026-08-31T01:00:00Z",
		"analysis_steps": []any{
			map[string]any{"occurred_at": "2026-08-31T01:05:00Z"},
			map[string]any{"occurred_at": "2026-08-31T01:03:00Z"},
		},
	}
	got := eventUpdatedAt(event)
	if got == nil || !got.Equal(time.Date(2026, 8, 31, 1, 5, 0, 0, time.UTC)) {
		t.Fatalf("unexpected latest event timestamp: %v", got)
	}
}
