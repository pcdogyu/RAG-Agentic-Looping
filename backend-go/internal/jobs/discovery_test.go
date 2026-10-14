package jobs

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/sourcefilter"
)

func TestDiscoveryHandlersCoverMigrationManifest(t *testing.T) {
	handlers := NewDiscoveryHandlers(config.Config{}, nil, nil)
	lane, err := RequireWorkerLane("discovery")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, handlers); err != nil {
		t.Fatalf("discovery handlers are incomplete: %v", err)
	}
}

func TestDiscoveryRSSDocumentParsesRSSAndAtomDates(t *testing.T) {
	for _, fixture := range []struct {
		body string
		date func(rssDocument) string
	}{
		{`<rss><channel><title>Example RSS</title><item><title>One</title><link>https://example.com/one</link><description>Summary</description><pubDate>Mon, 01 Sep 2026 01:02:03 GMT</pubDate></item></channel></rss>`, func(value rssDocument) string { return value.Channel.Items[0].PubDate }},
		{`<feed xmlns="http://www.w3.org/2005/Atom"><title>Example Atom</title><entry><title>Two</title><summary>Summary</summary><updated>2026-09-01T01:02:03Z</updated><link href="https://example.com/two"/></entry></feed>`, func(value rssDocument) string { return value.Entries[0].Updated }},
	} {
		var document rssDocument
		if err := xml.NewDecoder(strings.NewReader(fixture.body)).Decode(&document); err != nil {
			t.Fatal(err)
		}
		if discoveryTime(fixture.date(document)).IsZero() {
			t.Fatalf("feed date was not decoded: %#v", document)
		}
	}
}

func TestDiscoveryIntAcceptsJSONNumbers(t *testing.T) {
	if actual := discoveryInt(float64(24)); actual != 24 {
		t.Fatalf("discoveryInt=%d want 24", actual)
	}
}

func TestDiscoveryTitleFilterSemantics(t *testing.T) {
	cfg := sourcefilter.Config{Enabled: true, Whitelist: []string{"英伟达"}, Blacklist: []string{"天气"}}
	if decision := sourcefilter.Evaluate("英伟达发布新产品", cfg); decision.Blocked || decision.Profile != "deep" {
		t.Fatalf("whitelist was not routed deep: %#v", decision)
	}
	if decision := sourcefilter.Evaluate("英伟达天气影响", cfg); !decision.Blocked || decision.Profile != "blocked" {
		t.Fatalf("blacklist must override whitelist: %#v", decision)
	}
	if decision := sourcefilter.Evaluate("其他公司新闻", cfg); decision.Blocked || !decision.GateBlocked || decision.Profile != "whitelist_miss" {
		t.Fatalf("whitelist miss must be gated out of the queue: %#v", decision)
	}
	if decision := sourcefilter.Evaluate("ＮＶＩＤＩＡ", sourcefilter.Config{Enabled: true, Whitelist: []string{"nvidia"}}); decision.Profile != "deep" {
		t.Fatal("NFKC and case-fold matching must be preserved")
	}
}

func TestDiscoveryLineageDoesNotTreatPublisherDomainAsOriginalSource(t *testing.T) {
	unknown := enrichDiscoveryLineage(discoveredNews{Source: "FMP Stock News", SourceQuality: "professional", Title: "Company update", URL: "https://example-news.test/story"})
	unknownLineage := objectValue(unknown.Metadata["source_lineage"])
	if stringValue(unknownLineage["original_source"]) != "" || stringValue(unknownLineage["syndication_group"]) != "" {
		t.Fatalf("publisher domain was incorrectly treated as a proven origin: %#v", unknownLineage)
	}

	firstParty := enrichDiscoveryLineage(discoveredNews{Source: "Issuer IR", SourceQuality: "official", Title: "Company update", URL: "https://investor.example.com/news"})
	firstPartyLineage := objectValue(firstParty.Metadata["source_lineage"])
	if stringValue(firstPartyLineage["original_source"]) != "investor.example.com" || stringValue(firstPartyLineage["syndication_group"]) != "origin:investorexamplecom" {
		t.Fatalf("first-party origin was not retained: %#v", firstPartyLineage)
	}

	wire := enrichDiscoveryLineage(discoveredNews{Source: "FMP Stock News", SourceQuality: "professional", Title: "Reuters: Company update", URL: "https://example-news.test/repost"})
	wireLineage := objectValue(wire.Metadata["source_lineage"])
	if stringValue(wireLineage["original_source"]) != "reuters" || stringValue(wireLineage["syndication_group"]) != "origin:reuters" {
		t.Fatalf("explicit wire origin was not retained: %#v", wireLineage)
	}
}

func TestDiscoveryWatermarkUsesPerSourceOverlap(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	items := []discoveredNews{
		{Source: "one", PublishedAt: base.Add(51 * time.Minute)},
		{Source: "one", PublishedAt: base.Add(49 * time.Minute)},
		{Source: "two", PublishedAt: base.Add(time.Minute)},
	}
	actual := filterBySourceWatermark(items, map[string]time.Time{"one": base.Add(time.Hour)}, base, 10*time.Minute)
	if len(actual) != 2 || actual[0].Source != "one" || actual[1].Source != "two" {
		t.Fatalf("unexpected watermark result: %#v", actual)
	}
}

func TestNormalizeMCPNewsStopsAtBoundaryAndBuildsHeadline(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	payload := map[string]any{"data": map[string]any{"has_more": true, "next_cursor": "next", "items": []any{
		map[string]any{"content": "黄金ETF持仓下降。后续内容", "time": "2026-09-01T01:00:00Z", "url": "https://example.com/a?utm_source=x"},
		map[string]any{"content": "旧消息", "time": "2026-08-31T23:00:00Z", "url": "https://example.com/b"},
	}}}
	items, next, more, reached := normalizeMCPNews(payload, "金十", "jin10_flash_v1", since)
	if len(items) != 1 || items[0].Title != "黄金ETF持仓下降。后续内容" || items[0].Summary != "黄金ETF持仓下降。后续内容" || items[0].URL != "https://example.com/a" || next != "next" || !more || !reached {
		t.Fatalf("unexpected MCP normalization: items=%#v next=%q more=%v reached=%v", items, next, more, reached)
	}
}

func TestNormalizeMCPNewsReadsCompleteNuxtHTMLContent(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	html := `<!----></div></div></div><script>window.__NUXT__=(function(a,b,c){return {data:[{flash:{data:{content:"\u003Cb\u003E博通(AVGO.O)2026财年Q4展望营收为348亿美元。第二句仍属于正文。\u003C\u002Fb\u003E",title:a}}}]}}(null));</script>`
	payload := map[string]any{"data": map[string]any{"items": []any{
		map[string]any{"content": html, "time": "2026-09-01T01:00:00Z", "url": "https://example.com/avgo"},
	}}}

	items, _, _, _ := normalizeMCPNews(payload, "金十", "jin10_flash_v1", since)
	want := "博通(AVGO.O)2026财年Q4展望营收为348亿美元。第二句仍属于正文。"
	if len(items) != 1 || items[0].Title != want || items[0].Summary != want {
		t.Fatalf("NUXT content was not preserved: %#v", items)
	}
}

func TestNormalizeMCPNewsRestoresJin10DecimalAndQualifiedSymbolHeadlines(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		title   string
		content string
		want    string
	}{
		{name: "decimal", title: "美国非农录得16.", content: "美国非农录得16.2万人，远高于预期。", want: "美国非农录得16.2万人，远高于预期。"},
		{name: "percentage", title: "黄金日内跌幅1.", content: "黄金日内跌幅1.66%。", want: "黄金日内跌幅1.66%。"},
		{name: "qualified symbol", title: "摩根士丹利(MS.", content: "摩根士丹利(MS.N)担任承销商。", want: "摩根士丹利(MS.N)担任承销商。"},
		{name: "ordinary chinese title", title: "独立标题。", content: "独立标题。后续正文", want: "独立标题。"},
		{name: "period followed by whitespace", title: "Independent title.", content: "Independent title. Next sentence", want: "Independent title."},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := map[string]any{"data": map[string]any{"items": []any{map[string]any{
				"title": test.title, "content": test.content, "time": "2026-09-01T01:00:00Z", "url": fmt.Sprintf("https://example.com/%d", index),
			}}}}
			items, _, _, _ := normalizeMCPNews(payload, "金十", "jin10_flash_v1", since)
			if len(items) != 1 || items[0].Title != test.want {
				t.Fatalf("headline=%q want %q", items[0].Title, test.want)
			}
		})
	}
}

func TestRestoreTruncatedMCPHeadlineCapsReconstructedContent(t *testing.T) {
	content := "数值1." + strings.Repeat("2", 140)
	actual := restoreTruncatedMCPHeadline("数值1.", content, "jin10_flash_v1")
	if len([]rune(actual)) != 120 || !strings.HasPrefix(actual, "数值1.2") {
		t.Fatalf("reconstructed headline was not capped at 120 runes: %q", actual)
	}
}

func TestDiscoverySchedulerIsEnabledForGoRuntime(t *testing.T) {
	if !NewDiscoveryScheduler(config.Config{}, nil, nil).Enabled() {
		t.Fatal("discovery scheduler must be enabled for the Go runtime")
	}
}

func TestDiscoverNewsLiquidParsesRestFeed(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "total": 4, "data": []map[string]any{
			{"id": 1, "text": "Bitcoin ETF inflows accelerate.", "link": "https://example.com/a?utm_source=x", "ts": "2026-10-13T08:00:00Z", "newsType": "Bloomberg", "engineType": "news", "coins": []map[string]any{{"symbol": "BTC"}}, "aiRating": map[string]any{"score": 88, "signal": "long", "status": "done"}},
			{"id": 2, "text": "黄金储备增持。", "link": "https://example.com/b", "ts": float64(1791878400000)},
			{"id": 3, "text": "旧消息。", "link": "https://example.com/old", "ts": "2026-10-12T01:00:00Z"},
			{"id": 4, "text": "缺少链接。", "link": "", "ts": "2026-10-13T08:30:00Z"},
		}})
	}))
	defer upstream.Close()
	runtime := &discoveryRuntime{cfg: config.Config{OpenNewsBaseURL: upstream.URL, OpenNewsToken: "token"}, client: upstream.Client()}
	batch := runtime.discoverNewsLiquid(context.Background(), time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC), 40)
	if gotPath != "/open/news_search" || gotAuth != "Bearer token" {
		t.Fatalf("unexpected request path=%q auth=%q", gotPath, gotAuth)
	}
	if gotBody["limit"] != float64(40) || gotBody["page"] != float64(1) {
		t.Fatalf("unexpected upstream body: %v", gotBody)
	}
	if len(batch.Items) != 2 {
		t.Fatalf("expected stale and link-less items filtered, got %d items: %#v", len(batch.Items), batch.Items)
	}
	first := batch.Items[0]
	if first.Source != "newsliquid" || first.Provider != "opennews-rest" || first.Language != "en" || first.URL != "https://example.com/a" {
		t.Fatalf("unexpected first item: %#v", first)
	}
	if len(first.Symbols) != 1 || first.Symbols[0] != "BTC" {
		t.Fatalf("coin symbols were not extracted: %#v", first.Symbols)
	}
	if rating := objectValue(first.Metadata["ai_rating"]); rating == nil || rating["score"] != float64(88) {
		t.Fatalf("AI rating was not preserved: %#v", first.Metadata)
	}
	if first.ContentHash == "" || first.ContentHash == newDiscoveredNews(first.Source, first.Provider, first.SourceQuality, first.Title, first.Summary, first.URL, first.Language, first.PublishedAt, nil, nil).ContentHash {
		t.Fatalf("content fingerprint was not applied: %#v", first)
	}
	second := batch.Items[1]
	if second.Language != "zh" || !second.PublishedAt.Equal(time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("epoch-millisecond timestamp was not parsed: %#v", second)
	}
	if len(batch.Reports) != 1 || batch.Reports[0].Status != "healthy" || batch.Reports[0].Source != "newsliquid" || batch.Reports[0].DiscoveredCount != 2 {
		t.Fatalf("unexpected reports: %#v", batch.Reports)
	}
	if len(batch.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", batch.Errors)
	}
}

func TestDiscoverNewsLiquidDisabledWithoutToken(t *testing.T) {
	runtime := &discoveryRuntime{cfg: config.Config{}, client: http.DefaultClient}
	batch := runtime.discoverNewsLiquid(context.Background(), time.Now().UTC().Add(-time.Hour), 40)
	if len(batch.Items) != 0 || len(batch.Reports) != 0 || len(batch.Errors) != 0 {
		t.Fatalf("source must stay silent without a token: %#v", batch)
	}
}

func TestDiscoverNewsLiquidReportsQuotaErrorDetail(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":"insufficient quota","success":false}`))
	}))
	defer upstream.Close()
	runtime := &discoveryRuntime{cfg: config.Config{OpenNewsBaseURL: upstream.URL, OpenNewsToken: "token"}, client: upstream.Client()}
	batch := runtime.discoverNewsLiquid(context.Background(), time.Now().UTC().Add(-time.Hour), 40)
	if len(batch.Reports) != 1 || batch.Reports[0].Status != "error" {
		t.Fatalf("expected one error report: %#v", batch.Reports)
	}
	if !strings.Contains(batch.Reports[0].Error, "HTTP 402") || !strings.Contains(batch.Reports[0].Error, "insufficient quota") {
		t.Fatalf("quota detail was not surfaced: %q", batch.Reports[0].Error)
	}
	if !strings.Contains(batch.Reports[0].Error, "Available Messages") {
		t.Fatalf("quota guidance was not surfaced: %q", batch.Reports[0].Error)
	}
}

func TestDiscoverNewsLiquidReportsRejectedTokenHint(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid token","success":false}`))
	}))
	defer upstream.Close()
	runtime := &discoveryRuntime{cfg: config.Config{OpenNewsBaseURL: upstream.URL, OpenNewsToken: "token"}, client: upstream.Client()}
	batch := runtime.discoverNewsLiquid(context.Background(), time.Now().UTC().Add(-time.Hour), 40)
	if len(batch.Reports) != 1 || batch.Reports[0].Status != "error" {
		t.Fatalf("expected one error report: %#v", batch.Reports)
	}
	if !strings.Contains(batch.Reports[0].Error, "HTTP 401") || !strings.Contains(batch.Reports[0].Error, "OPENNEWS_TOKEN") {
		t.Fatalf("token guidance was not surfaced: %q", batch.Reports[0].Error)
	}
}

func TestNewsLiquidErrorHintOnlyCoversBillingAndAuth(t *testing.T) {
	if newsLiquidErrorHint(http.StatusInternalServerError) != "" {
		t.Fatalf("unexpected hint for a server-side failure")
	}
	if newsLiquidErrorHint(http.StatusPaymentRequired) == "" || newsLiquidErrorHint(http.StatusForbidden) == "" {
		t.Fatalf("billing and auth statuses must carry guidance")
	}
}

func TestNewsLiquidTimeParsesEpochSeconds(t *testing.T) {
	if actual := newsLiquidTime(float64(1791878400)); !actual.Equal(time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("epoch seconds were not parsed: %v", actual)
	}
	if actual := newsLiquidTime("not-a-time"); !actual.IsZero() {
		t.Fatalf("invalid timestamp must stay zero: %v", actual)
	}
}
