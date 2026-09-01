package searchmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxRequestBytes = 1 << 20

type Server struct {
	searxURL string
	client   *http.Client
}

type rpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id,omitempty"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

type SearchResult struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	Snippet     string `json:"snippet"`
	PublishedAt any    `json:"published_at"`
	Engine      any    `json:"engine"`
}

func NewHandler(searxURL string, client *http.Client) http.Handler {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	server := &Server{searxURL: strings.TrimRight(searxURL, "/"), client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.health)
	mux.HandleFunc("/healthz", server.health)
	mux.HandleFunc("/mcp", server.mcp)
	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"detail": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "search-mcp", "implementation": "go"})
}

func (s *Server) mcp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"detail": "method not allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	var request rpcRequest
	if err := decoder.Decode(&request); err != nil {
		writeRPCError(w, nil, -32700, "invalid JSON")
		return
	}
	if request.JSONRPC != "2.0" || request.Method == "" {
		writeRPCError(w, request.ID, -32600, "invalid request")
		return
	}
	if request.Method == "notifications/initialized" {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	switch request.Method {
	case "initialize":
		writeRPCResult(w, request.ID, map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]string{"name": "Market Loop Search", "version": "3"},
		})
	case "ping":
		writeRPCResult(w, request.ID, map[string]any{})
	case "tools/list":
		writeRPCResult(w, request.ID, map[string]any{"tools": []any{webSearchTool()}})
	case "tools/call":
		s.callTool(w, r.Context(), request)
	default:
		writeRPCError(w, request.ID, -32601, "method not found")
	}
}

func (s *Server) callTool(w http.ResponseWriter, ctx context.Context, request rpcRequest) {
	name, _ := request.Params["name"].(string)
	arguments, _ := request.Params["arguments"].(map[string]any)
	if name != "web_search" || arguments == nil {
		writeRPCError(w, request.ID, -32602, "invalid tool call")
		return
	}
	query := strings.TrimSpace(stringValue(arguments["query"]))
	if query == "" || len([]rune(query)) > 500 {
		writeToolError(w, request.ID, "query is required and must not exceed 500 characters")
		return
	}
	limit := intValue(arguments["limit"], 5)
	limit = min(max(limit, 1), 20)
	language := strings.TrimSpace(stringValue(arguments["language"]))
	if language == "" {
		language = "zh-CN"
	}
	timeRange := strings.TrimSpace(stringValue(arguments["time_range"]))
	if !map[string]bool{"day": true, "week": true, "month": true, "year": true}[timeRange] {
		timeRange = ""
	}
	results, err := s.search(ctx, query, limit, language, timeRange)
	if err != nil {
		writeToolError(w, request.ID, fmt.Sprintf("%T: search upstream failed", err))
		return
	}
	structured := map[string]any{"results": results}
	text, _ := json.Marshal(structured)
	writeRPCResult(w, request.ID, map[string]any{
		"content":           []any{map[string]string{"type": "text", "text": string(text)}},
		"structuredContent": structured,
		"isError":           false,
	})
}

func (s *Server) search(ctx context.Context, query string, limit int, language, timeRange string) ([]SearchResult, error) {
	parameters := url.Values{"q": {query}, "format": {"json"}, "language": {language}, "safesearch": {"0"}}
	if timeRange != "" {
		parameters.Set("time_range", timeRange)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.searxURL+"/search?"+parameters.Encode(), nil)
	if err != nil {
		return nil, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("SearXNG returned %s", response.Status)
	}
	var payload struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&payload); err != nil {
		return nil, err
	}
	results := make([]SearchResult, 0, min(limit, len(payload.Results)))
	for _, item := range payload.Results {
		title := strings.TrimSpace(stringValue(item["title"]))
		target := strings.TrimSpace(stringValue(item["url"]))
		snippet := strings.TrimSpace(stringValue(item["content"]))
		parsed, parseErr := url.Parse(target)
		if title == "" || snippet == "" || parseErr != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			continue
		}
		results = append(results, SearchResult{Title: title, URL: target, Snippet: snippet, PublishedAt: item["publishedDate"], Engine: item["engine"]})
		if len(results) == limit {
			break
		}
	}
	return results, nil
}

func webSearchTool() map[string]any {
	return map[string]any{
		"name":        "web_search",
		"description": "Search the public web through SearXNG and return original HTTP(S) links.",
		"inputSchema": map[string]any{
			"type": "object", "additionalProperties": false, "required": []string{"query"},
			"properties": map[string]any{
				"query":      map[string]any{"type": "string", "minLength": 1, "maxLength": 500},
				"limit":      map[string]any{"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
				"language":   map[string]any{"type": "string", "default": "zh-CN"},
				"time_range": map[string]any{"type": "string", "enum": []string{"", "day", "week", "month", "year"}, "default": ""},
			},
		},
		"outputSchema": map[string]any{"type": "object", "properties": map[string]any{"results": map[string]any{"type": "array"}}},
	}
}

func writeRPCResult(w http.ResponseWriter, id, result any) {
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func writeRPCError(w http.ResponseWriter, id any, code int, message string) {
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
}

func writeToolError(w http.ResponseWriter, id any, message string) {
	writeRPCResult(w, id, map[string]any{"content": []any{map[string]string{"type": "text", "text": message}}, "isError": true})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if result, ok := value.(string); ok {
		return result
	}
	return fmt.Sprint(value)
}

func intValue(value any, fallback int) int {
	switch current := value.(type) {
	case json.Number:
		result, err := current.Int64()
		if err == nil {
			return int(result)
		}
	case float64:
		return int(current)
	case int:
		return current
	}
	return fallback
}

var _ = errors.New
