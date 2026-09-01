package searchmcp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMCPInitializeListAndSearch(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.URL.Query().Get("q") != "market news" || r.URL.Query().Get("time_range") != "day" {
			t.Fatalf("unexpected upstream request: %s", r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{
			map[string]any{"title": "Valid", "url": "https://example.com/news", "content": "Evidence", "engine": "bing"},
			map[string]any{"title": "Invalid", "url": "javascript:alert(1)", "content": "Ignored"},
		}})
	}))
	defer upstream.Close()
	handler := NewHandler(upstream.URL, upstream.Client())

	initialize := rpcCall(t, handler, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{}})
	if initialize.Code != http.StatusOK || stringValue(initialize.Body["jsonrpc"]) != "2.0" {
		t.Fatalf("unexpected initialize response: %#v", initialize)
	}
	listed := rpcCall(t, handler, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": map[string]any{}})
	tools := listed.Body["result"].(map[string]any)["tools"].([]any)
	if len(tools) != 1 || tools[0].(map[string]any)["name"] != "web_search" {
		t.Fatalf("unexpected tools response: %#v", listed.Body)
	}
	searched := rpcCall(t, handler, map[string]any{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": map[string]any{"name": "web_search", "arguments": map[string]any{"query": "market news", "limit": 5, "language": "en", "time_range": "day"}}})
	result := searched.Body["result"].(map[string]any)
	structured := result["structuredContent"].(map[string]any)
	items := structured["results"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["url"] != "https://example.com/news" || result["isError"] != false {
		t.Fatalf("unexpected search response: %#v", searched.Body)
	}
}

func TestMCPRejectsInvalidToolInput(t *testing.T) {
	handler := NewHandler("http://unused", nil)
	response := rpcCall(t, handler, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": "web_search", "arguments": map[string]any{"query": ""}}})
	if response.Body["result"].(map[string]any)["isError"] != true {
		t.Fatalf("invalid input was accepted: %#v", response.Body)
	}
}

type rpcResponse struct {
	Code int
	Body map[string]any
}

func rpcCall(t *testing.T, handler http.Handler, payload any) rpcResponse {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	decoded := map[string]any{}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	return rpcResponse{Code: response.Code, Body: decoded}
}
