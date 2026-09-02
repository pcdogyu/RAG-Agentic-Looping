package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestResearchHandlersCoverLaneManifest(t *testing.T) {
	handlers := NewResearchHandlers(config.Config{}, nil, nil)
	if handlers[researchEventTask] == nil || handlers[researchAssetTask] == nil {
		t.Fatalf("research handlers are incomplete: %#v", handlers)
	}
}

func TestEventResearchTrackingLabelsPreserveNewsHeadline(t *testing.T) {
	title, subtitle := eventResearchTrackingLabels(map[string]any{
		"headline":   "美国防部与通用动力和洛克希德达成七年期协议",
		"event_type": "security",
	})
	if title != "美国防部与通用动力和洛克希德达成七年期协议" || subtitle != "security" {
		t.Fatalf("unexpected research tracking labels: %q / %q", title, subtitle)
	}
}

func TestRatingForScoreUsesFiveStableBands(t *testing.T) {
	cases := map[int]string{-100: "strongly_bearish", -70: "strongly_bearish", -69: "bearish", -30: "bearish", -29: "watch", 29: "watch", 30: "bullish", 69: "bullish", 70: "strongly_bullish", 100: "strongly_bullish"}
	for score, expected := range cases {
		if actual := ratingForScore(score); actual != expected {
			t.Fatalf("score %d: expected %s, got %s", score, expected, actual)
		}
	}
}

func TestSanitizeEventImpactsRejectsVolumeAsMacroTarget(t *testing.T) {
	event := map[string]any{"candidates": []any{map[string]any{"asset": map[string]any{"asset_id": "US:HOOD", "name": "Robinhood", "asset_class": "equity"}}}}
	values := []eventImpactDraft{
		{TargetType: "economy", TargetName: "成交量增加", DirectionScore: 80},
		{TargetType: "other", TargetName: "Robinhood", AssetID: "US:HOOD", DirectionScore: 40},
		{TargetType: "tradable_asset", TargetName: "Unknown", AssetID: "US:NOPE", DirectionScore: 50},
	}
	actual := sanitizeEventImpacts(values, event)
	if len(actual) != 1 || actual[0].TargetType != "tradable_asset" || actual[0].TargetName != "Robinhood" {
		t.Fatalf("unexpected sanitized impacts: %#v", actual)
	}
}

func TestPermanentResearchErrorIsTerminal(t *testing.T) {
	value := permanentJobError{context.DeadlineExceeded}
	var marker interface{ Permanent() bool }
	if !errors.As(value, &marker) || !marker.Permanent() {
		t.Fatal("research deadline must be a permanent queue failure")
	}
}

func TestResearchLimitsUseThirtyFourAndThirtyFiveMinutes(t *testing.T) {
	cfg := config.Config{ResearchSoftLimit: 34 * time.Minute, ResearchHardLimit: 35 * time.Minute}
	if cfg.ResearchSoftLimit >= cfg.ResearchHardLimit {
		t.Fatal("soft research limit must be lower than hard limit")
	}
}

func TestResearchModelRequestUsesConfiguredThinkingMode(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode research request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":{"content":"{\"answer\":\"ok\"}"}}`))
	}))
	defer server.Close()

	runtime := &researchRuntime{
		cfg: config.Config{
			ResearchModel: "qwen3:4b-thinking",
			ResearchURLs:  []string{server.URL},
			ResearchThink: true,
		},
		client: server.Client(),
	}
	var result map[string]any
	if err := runtime.callResearchModel(
		context.Background(),
		[16]byte{},
		"research_run",
		"report_drafting",
		"system",
		"prompt",
		map[string]any{"type": "object"},
		"research-0",
		&result,
	); err != nil {
		t.Fatalf("call research model: %v", err)
	}
	if thinking, ok := request["think"].(bool); !ok || !thinking {
		t.Fatalf("expected think=true, got %#v", request["think"])
	}
}

func TestResearchModelRequestDisablesThinkingByDefault(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode research request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":{"content":"{}"}}`))
	}))
	defer server.Close()

	runtime := &researchRuntime{
		cfg: config.Config{
			ResearchModel: "qwen3:4b-thinking",
			ResearchURLs:  []string{server.URL},
		},
		client: server.Client(),
	}
	var result map[string]any
	if err := runtime.callResearchModel(
		context.Background(),
		[16]byte{},
		"research_run",
		"report_drafting",
		"system",
		"prompt",
		map[string]any{"type": "object"},
		"research-0",
		&result,
	); err != nil {
		t.Fatalf("call research model: %v", err)
	}
	if thinking, ok := request["think"].(bool); !ok || thinking {
		t.Fatalf("expected think=false, got %#v", request["think"])
	}
}

func TestResearchModelDoesNotRetryAfterTimeout(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"message":{"content":"{}"}}`))
	}))
	defer server.Close()

	runtime := &researchRuntime{
		cfg: config.Config{
			ResearchModel: "qwen3:4b-thinking",
			ResearchURLs:  []string{server.URL},
		},
		client: &http.Client{Timeout: 10 * time.Millisecond},
	}
	var result map[string]any
	err := runtime.callResearchModel(
		context.Background(),
		[16]byte{},
		"research_run",
		"report_drafting",
		"system",
		"prompt",
		map[string]any{"type": "object"},
		"research-0",
		&result,
	)
	if !isResearchRequestTimeoutOrCancellation(err) {
		t.Fatalf("expected timeout or cancellation, got %v", err)
	}
	if actual := requests.Load(); actual != 1 {
		t.Fatalf("expected one timed-out request without retry, got %d", actual)
	}
}

func TestResearchRuntimeHoldsInstanceUntilHandlerReleasesIt(t *testing.T) {
	runtime := newResearchRuntime(config.Config{ResearchURLs: []string{"http://one", "http://two"}}, nil, nil)
	first, releaseFirst, err := runtime.acquireResearchInstance(context.Background(), "research-7")
	if err != nil || first != "research-0" {
		t.Fatalf("expected first slot research-0, got %q / %v", first, err)
	}
	defer releaseFirst()
	second, releaseSecond, err := runtime.acquireResearchInstance(context.Background(), "research-7")
	if err != nil || second != "research-1" {
		t.Fatalf("expected second slot research-1, got %q / %v", second, err)
	}

	next := make(chan string, 1)
	go func() {
		instanceID, release, acquireErr := runtime.acquireResearchInstance(context.Background(), "research-7")
		if acquireErr == nil {
			release()
			next <- instanceID
		}
	}()
	select {
	case instanceID := <-next:
		t.Fatalf("third claim unexpectedly reused busy slot %s", instanceID)
	case <-time.After(20 * time.Millisecond):
	}
	releaseSecond()
	select {
	case instanceID := <-next:
		if instanceID != "research-1" {
			t.Fatalf("expected released slot research-1, got %s", instanceID)
		}
	case <-time.After(time.Second):
		t.Fatal("third claim did not receive the released slot")
	}
}

func TestResearchRuntimePreservesFallbackWithoutEndpoints(t *testing.T) {
	runtime := newResearchRuntime(config.Config{}, nil, nil)
	actual, release, err := runtime.acquireResearchInstance(context.Background(), "research-7")
	defer release()
	if err != nil || actual != "research-7" {
		t.Fatalf("expected fallback instance, got %s / %v", actual, err)
	}
}

func TestResearchEndpointIndexUsesConfiguredPosition(t *testing.T) {
	values := []string{"http://one", "http://two", "http://three"}
	if actual := researchEndpointIndex(values, "http://three"); actual != 2 {
		t.Fatalf("expected endpoint 2, got %d", actual)
	}
}
