package newsmcp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func rpcCall(t *testing.T, handler http.Handler, body string) map[string]any {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v (%s)", err, recorder.Body.String())
	}
	return response
}

func initialize(t *testing.T, handler http.Handler) {
	t.Helper()
	response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`)
	if response["error"] != nil {
		t.Fatalf("initialize failed: %v", response["error"])
	}
}

func TestInitializeAndToolsList(t *testing.T) {
	server := NewServer(Config{})
	initialize(t, server)
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	result, _ := response["result"].(map[string]any)
	tools, _ := result["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(tools))
	}
	first, _ := tools[0].(map[string]any)
	if first["name"] != "get_latest_news" || tools[1].(map[string]any)["name"] != "search_news" {
		t.Fatalf("unexpected tool names: %v", tools)
	}
}

func TestToolsCallBeforeInitialize(t *testing.T) {
	server := NewServer(Config{})
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_latest_news","arguments":{}}}`)
	if response["error"] == nil {
		t.Fatal("expected not-initialized error")
	}
}

func TestGetLatestNewsMapsUpstream(t *testing.T) {
	var gotBody map[string]any
	var gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/open/news_search" {
			t.Errorf("unexpected upstream path %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "total": 100, "data": []map[string]any{
			{"id": 4438580, "text": "英国至10月13日LME铜库存变化将公布。", "link": "https://example.com/a", "ts": "2026-10-13T15:50:00+08:00"},
			{"id": 4438581, "text": "", "link": "", "ts": ""},
		}})
	}))
	defer upstream.Close()
	server := NewServer(Config{BaseURL: upstream.URL, DefaultToken: "env-token"})
	initialize(t, server)
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_latest_news","arguments":{"limit":2,"page":"3"}}}`)
	result, _ := response["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %v", result["content"])
	}
	if gotBody["limit"] != float64(2) || gotBody["page"] != float64(3) || gotBody["query"] != nil {
		t.Fatalf("unexpected upstream body: %v", gotBody)
	}
	if gotAuth != "Bearer env-token" {
		t.Fatalf("expected env token fallback, got %q", gotAuth)
	}
	structured, _ := result["structuredContent"].(map[string]any)
	data, _ := structured["data"].(map[string]any)
	items, _ := data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected empty-text item filtered out, got %d items", len(items))
	}
	item, _ := items[0].(map[string]any)
	if item["content"] != "英国至10月13日LME铜库存变化将公布。" || item["url"] != "https://example.com/a" || item["time"] != "2026-10-13T15:50:00+08:00" {
		t.Fatalf("unexpected mapped item: %v", item)
	}
	if data["next_cursor"] != "4" || data["has_more"] != true {
		t.Fatalf("unexpected pagination: %v / %v", data["next_cursor"], data["has_more"])
	}
}

func TestSearchNewsForwardsRequestAuthorization(t *testing.T) {
	var gotBody map[string]any
	var gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "total": 1, "data": []map[string]any{
			{"id": 1, "text": "hello", "link": "https://example.com/b", "ts": "2026-10-13T00:00:00+08:00"},
		}})
	}))
	defer upstream.Close()
	server := NewServer(Config{BaseURL: upstream.URL})
	initialize(t, server)
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search_news","arguments":{"keyword":"bitcoin","limit":5}}}`))
	request.Header.Set("Authorization", "Bearer user-token")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	if gotAuth != "Bearer user-token" {
		t.Fatalf("expected forwarded authorization, got %q", gotAuth)
	}
	if gotBody["query"] != "bitcoin" || gotBody["limit"] != float64(5) || gotBody["page"] != float64(1) {
		t.Fatalf("unexpected upstream body: %v", gotBody)
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	result, _ := response["result"].(map[string]any)
	structured, _ := result["structuredContent"].(map[string]any)
	data, _ := structured["data"].(map[string]any)
	if data["has_more"] != false || data["next_cursor"] != "" {
		t.Fatalf("expected terminal page for short result set: %v", data)
	}
}

func TestSearchNewsRequiresKeyword(t *testing.T) {
	server := NewServer(Config{BaseURL: "http://127.0.0.1:1"})
	initialize(t, server)
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"search_news","arguments":{}}}`)
	result, _ := response["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("expected isError for missing keyword: %v", result)
	}
}

func TestUpstreamAuthFailureBecomesToolError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "denied", http.StatusUnauthorized)
	}))
	defer upstream.Close()
	server := NewServer(Config{BaseURL: upstream.URL})
	initialize(t, server)
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"get_latest_news","arguments":{}}}`)
	result, _ := response["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("expected isError for upstream 401: %v", result)
	}
	content, _ := result["content"].([]any)
	text, _ := content[0].(map[string]any)
	if !strings.Contains(stringValue(text["text"]), "authentication") {
		t.Fatalf("expected auth failure message, got %v", text)
	}
}

func TestLimitOutOfRange(t *testing.T) {
	server := NewServer(Config{BaseURL: "http://127.0.0.1:1"})
	initialize(t, server)
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"get_latest_news","arguments":{"limit":500}}}`)
	result, _ := response["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("expected isError for out-of-range limit: %v", result)
	}
}

func TestUnknownMethodAndNonPost(t *testing.T) {
	server := NewServer(Config{})
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":8,"method":"bad/method","params":{}}`)
	if response["error"] == nil {
		t.Fatal("expected method-not-found error")
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/mcp", bytes.NewReader(nil)))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET, got %d", recorder.Code)
	}
}

func TestUpstreamQuotaErrorIncludesDetail(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":"insufficient quota","success":false}`))
	}))
	defer upstream.Close()
	server := NewServer(Config{BaseURL: upstream.URL, DefaultToken: "env-token"})
	initialize(t, server)
	response := rpcCall(t, server, `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"get_latest_news","arguments":{}}}`)
	result, _ := response["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("expected isError for upstream 402: %v", result)
	}
	content, _ := result["content"].([]any)
	text, _ := content[0].(map[string]any)
	message := stringValue(text["text"])
	if !strings.Contains(message, "HTTP 402") || !strings.Contains(message, "insufficient quota") {
		t.Fatalf("expected quota detail in message, got %q", message)
	}
}
