package jobs

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestOutcomeHandlersCoverMigrationManifest(t *testing.T) {
	lane, err := RequireWorkerLane("outcomes")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewOutcomeHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatalf("outcome handlers are incomplete: %v", err)
	}
}

func TestOutcomeSchedulerIsEnabledForGoRuntime(t *testing.T) {
	if !NewOutcomeScheduler(config.Config{}, nil, nil).Enabled() {
		t.Fatal("outcome scheduler must be enabled for the Go runtime")
	}
}

func TestOutcomeScheduleMatchesLegacyCadence(t *testing.T) {
	want := map[string]time.Duration{
		evaluateOutcomesTask:          24 * time.Hour,
		refreshEventMarketFactorsTask: 24 * time.Hour,
	}
	if len(outcomeSchedules) != len(want) {
		t.Fatalf("got %d schedules, want %d", len(outcomeSchedules), len(want))
	}
	for _, spec := range outcomeSchedules {
		if want[spec.task] != spec.interval {
			t.Fatalf("unexpected schedule for %s: %s", spec.task, spec.interval)
		}
	}
}

func TestOutcomeWindowUsesExactCalendarBoundary(t *testing.T) {
	base := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	points := []outcomePricePoint{
		{ObservedAt: time.Date(2026, 1, 2, 16, 0, 0, 0, time.UTC), Close: 100},
		{ObservedAt: time.Date(2026, 1, 3, 16, 0, 0, 0, time.UTC), Close: 105},
		{ObservedAt: time.Date(2026, 1, 4, 16, 0, 0, 0, time.UTC), Close: 110},
	}
	window := outcomeWindow(points, base, "calendar_days", 2)
	if len(window) != 3 || window[0].Close != 100 || window[2].Close != 110 {
		t.Fatalf("unexpected calendar window: %#v", window)
	}
}

func TestOutcomeWindowUsesSubsequentTradingCloses(t *testing.T) {
	base := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	points := []outcomePricePoint{
		{ObservedAt: time.Date(2026, 1, 2, 16, 0, 0, 0, time.UTC), Close: 100},
		{ObservedAt: time.Date(2026, 1, 5, 16, 0, 0, 0, time.UTC), Close: 102},
		{ObservedAt: time.Date(2026, 1, 6, 16, 0, 0, 0, time.UTC), Close: 104},
		{ObservedAt: time.Date(2026, 1, 7, 16, 0, 0, 0, time.UTC), Close: 106},
	}
	window := outcomeWindow(points, base, "trading_sessions", 3)
	if len(window) != 4 || window[3].Close != 106 {
		t.Fatalf("unexpected trading-session window: %#v", window)
	}
}

func TestNormalizeOutcomePricesAcceptsProviderShapesAndDeduplicates(t *testing.T) {
	boundary := time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)
	payload := map[string]any{"items": []any{
		map[string]any{"日期": "2026-01-02", "收盘": "100"},
		map[string]any{"date": "2026-01-02", "close": 101.0},
		map[string]any{"date": "2026-01-05", "close": 999.0},
	}}
	points := normalizeOutcomePrices(payload, boundary)
	if len(points) != 1 || points[0].Close != 101 {
		t.Fatalf("unexpected normalized points: %#v", points)
	}
}

func TestDueMarketFactorSessionOnlyAdvances(t *testing.T) {
	if dueMarketFactorSession(1.9, 0) != 0 || dueMarketFactorSession(2, 0) != 1 || dueMarketFactorSession(8, 1) != 5 || dueMarketFactorSession(30, 5) != 20 || dueMarketFactorSession(40, 20) != 0 {
		t.Fatal("market factor maturity boundaries changed")
	}
}

func TestOutcomeSignalStatusPreservesNewAndLegacyRules(t *testing.T) {
	if got := outcomeSignalStatus(map[string]any{"signal_status": "directional"}); got != "directional" {
		t.Fatalf("explicit status changed: %s", got)
	}
	if got := outcomeSignalStatus(map[string]any{"scoring_version": "short-term-impact-v1", "score": 14.0}); got != "neutral" {
		t.Fatalf("short-term neutral boundary changed: %s", got)
	}
	if got := outcomeSignalStatus(map[string]any{"scoring_version": "llm-direction-v3", "direction_score": -30.0}); got != "directional" {
		t.Fatalf("direction-v3 boundary changed: %s", got)
	}
	if got := outcomeSignalStatus(map[string]any{"score": 80.0, "evidence_complete": false}); got != "insufficient_evidence" {
		t.Fatalf("legacy evidence gate changed: %s", got)
	}
}

func TestEvaluateRecommendationOutcomeMath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		prices := []map[string]any{
			{"date": "2026-01-02", "close": 100.0},
			{"date": "2026-01-05", "close": 105.0},
			{"date": "2026-01-06", "close": 90.0},
			{"date": "2026-01-07", "close": 110.0},
		}
		if request["symbol"] == "000300" {
			prices = []map[string]any{
				{"date": "2026-01-02", "close": 100.0},
				{"date": "2026-01-05", "close": 101.0},
				{"date": "2026-01-06", "close": 102.0},
				{"date": "2026-01-07", "close": 103.0},
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": prices})
	}))
	defer server.Close()

	runtime := &outcomeRuntime{
		cfg:    config.Config{MarketAdapterURL: server.URL},
		client: server.Client(),
	}
	start := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	recommendation := map[string]any{
		"signal_status": "directional", "horizon_days": 3.0, "horizon_unit": "trading_sessions",
		"as_of": iso(start), "scoring_version": "short-term-impact-v1", "score": 20.0, "direction_score": 20.0,
		"bull_probability": .5, "base_probability": .3, "bear_probability": .2,
		"asset": map[string]any{"asset_id": "equity:XSHG:600001", "asset_class": "equity", "market": "CN", "symbol": "600001"},
	}
	outcome, state, err := runtime.evaluateRecommendation(context.Background(), uuid.New(), start, recommendation, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), map[string][]outcomePricePoint{})
	if err != nil || state != "completed" {
		t.Fatalf("evaluation failed: state=%s err=%v", state, err)
	}
	assertClose := func(name string, actual, expected float64) {
		t.Helper()
		if math.Abs(actual-expected) > 1e-12 {
			t.Fatalf("%s=%.12f want %.12f", name, actual, expected)
		}
	}
	assertClose("raw_return", outcome["raw_return"].(float64), .10)
	assertClose("benchmark_return", *outcome["benchmark_return"].(*float64), .03)
	assertClose("alpha", *outcome["alpha"].(*float64), .07)
	assertClose("brier", outcome["brier_score"].(float64), (.25+.09+.04)/3)
	assertClose("max_drawdown", outcome["max_drawdown"].(float64), 90.0/105.0-1)
	if outcome["direction_correct"] != true || outcome["benchmark_status"] != "available" {
		t.Fatalf("unexpected direction/benchmark outcome: %#v", outcome)
	}
}
