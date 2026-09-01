package jobs

import (
	"encoding/json"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestExtractLaneRegistersEveryRequiredHandler(t *testing.T) {
	lane, err := RequireWorkerLane("extract")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewExtractHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeTaskEnvelopeUsesPersistentJobShape(t *testing.T) {
	payload := json.RawMessage(`{"args":["news-1"],"kwargs":{"force_asset_mapping":true}}`)
	value, err := decodeTaskEnvelope(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Args) != 1 || value.Args[0] != "news-1" || !boolValue(value.Kwargs["force_asset_mapping"]) {
		t.Fatalf("unexpected envelope: %#v", value)
	}
}

func TestFallbackExtractionMatchesGoldenRules(t *testing.T) {
	news := newsRecord{Title: "Company reports quarterly earnings", Summary: "Revenue and profit increased", Symbols: []string{"TEST"}}
	value := fallbackExtraction(news)
	if value.EventType != "earnings" || value.HorizonDays != 30 || value.Novelty != .4 || value.Priority != .45 {
		t.Fatalf("unexpected fallback: %#v", value)
	}
	if len(value.Actions) != 1 || stringValue(value.Actions[0]["action_stage"]) != "unknown" {
		t.Fatalf("fallback actions must remain researchable: %#v", value.Actions)
	}

	sanctions := fallbackExtraction(newsRecord{Title: "Sanctions formally take effect", Summary: "制裁正式生效"})
	if sanctions.EventType != "other" || stringValue(sanctions.Actions[0]["action_stage"]) != "effective" {
		t.Fatalf("unexpected sanctions fallback: %#v", sanctions)
	}
}

func TestFallbackExtractionUsesDeterministicRulePrecedence(t *testing.T) {
	news := newsRecord{Title: "机器人（300024）发布年度业绩预告", Symbols: []string{"300024"}}
	for attempt := 0; attempt < 100; attempt++ {
		if got := fallbackExtraction(news).EventType; got != "earnings" {
			t.Fatalf("multi-keyword event type=%q want earnings", got)
		}
	}
}

func TestEventHorizonIgnoresModelSuppliedHorizon(t *testing.T) {
	tests := map[string]int{"earnings": 30, "security": 30, "m_and_a": 180, "product": 90, "macro": 90, "other": 90}
	for eventType, want := range tests {
		if got := eventHorizonDays(eventType); got != want {
			t.Fatalf("eventHorizonDays(%q)=%d want %d", eventType, got, want)
		}
	}
}

func TestPreferredExtractEndpointHonorsAssignedInstance(t *testing.T) {
	values := []string{"http://one", "http://two", "http://three"}
	got := preferredEndpoints(values, "extract-1", "extract")
	if got[0] != "http://two" || len(got) != 3 {
		t.Fatalf("unexpected endpoint order: %#v", got)
	}
	if got := preferredEndpoints(values, "research-1", "extract"); got[0] != "http://one" {
		t.Fatalf("foreign lane changed endpoint order: %#v", got)
	}
}

func TestOllamaKeepAlivePreservesNumericJSONType(t *testing.T) {
	if got := ollamaKeepAliveValue("-1"); got != int64(-1) {
		t.Fatalf("numeric keep_alive must remain numeric, got %#v", got)
	}
	if got := ollamaKeepAliveValue("5m"); got != "5m" {
		t.Fatalf("duration keep_alive must remain a string, got %#v", got)
	}
}

func TestSecuritySymbolAndProductMatchingSafety(t *testing.T) {
	if explicitSymbol("AI infrastructure spending grows", "AI", false) {
		t.Fatal("ambiguous short ticker matched ordinary text")
	}
	if !explicitSymbol("NYSE: AI reports results", "AI", false) {
		t.Fatal("exchange-qualified ticker did not match")
	}
	if !explicitSymbol("Alibaba (9988) rises", "09988", false) {
		t.Fatal("Hong Kong leading-zero variant did not match")
	}
	if meaningfulProduct("云服务") || !meaningfulProduct("阿里云") {
		t.Fatal("generic and branded products were not distinguished")
	}
	if meaningfulIssuerTerm("机器人") || !meaningfulIssuerTerm("沈阳新松机器人自动化股份有限公司") {
		t.Fatal("generic and explicit issuer names were not distinguished")
	}
}

func TestScanCountsKeepRetryAndTerminalStatesSeparate(t *testing.T) {
	payload := map[string]any{"items": []any{
		map[string]any{"status": "completed"}, map[string]any{"status": "retrying"},
		map[string]any{"status": "failed"}, map[string]any{"status": "cancelled"},
	}}
	counts := scanCounts(payload)
	if counts["completed"] != 1 || counts["retrying"] != 1 || counts["failed"] != 1 || counts["queued"] != 0 {
		t.Fatalf("unexpected scan counts: %#v", counts)
	}
}
