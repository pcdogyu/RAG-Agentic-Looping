// Package newsmcp exposes the NewsLiquid/OpenNews (6551.io) free news REST
// API as a Streamable HTTP MCP server so the platform can register it through
// the shared MCP source registry.
package newsmcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	protocolVersion = "2025-06-18"
	serverName      = "news-mcp"
	serverTitle     = "NewsLiquid OpenNews MCP"
	serverVersion   = "1.0.0"
	upstreamTimeout = 30 * time.Second
	maxLimit        = 100
	defaultLimit    = 20
	maxUpstreamBody = 8 << 20
)

type Config struct {
	APIClient *http.Client
	BaseURL   string
	// DefaultToken is used when the MCP request carries no Authorization
	// header, so deployments may keep the upstream credential in the env.
	DefaultToken string
}

type Server struct {
	mu         sync.Mutex
	configured bool
	client     *http.Client
	baseURL    string
	token      string
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e rpcError) Error() string { return e.Message }

type toolCall struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type newsItem struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Time    string `json:"time"`
	URL     string `json:"url"`
	ID      any    `json:"id,omitempty"`
}

func NewServer(cfg Config) *Server {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://ai.6551.io"
	}
	client := cfg.APIClient
	if client == nil {
		client = &http.Client{Timeout: upstreamTimeout}
	}
	return &Server{client: client, baseURL: baseURL, token: strings.TrimSpace(cfg.DefaultToken)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request rpcRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&request); err != nil {
		writeError(w, nil, -32700, "invalid JSON")
		return
	}
	switch request.Method {
	case "initialize":
		if _, ok := decodeObject(request.Params); !ok {
			writeError(w, request.ID, -32602, "initialize params must be an object")
			return
		}
		s.mu.Lock()
		s.configured = true
		s.mu.Unlock()
		writeResult(w, request.ID, map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": serverName, "title": serverTitle, "version": serverVersion},
		})
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
	case "ping":
		writeResult(w, request.ID, map[string]any{})
	case "tools/list":
		if !s.Configured() {
			writeError(w, request.ID, -32002, "server not initialized")
			return
		}
		writeResult(w, request.ID, map[string]any{"tools": toolDefinitions()})
	case "tools/call":
		if !s.Configured() {
			writeError(w, request.ID, -32002, "server not initialized")
			return
		}
		params, ok := decodeObject(request.Params)
		if !ok {
			writeError(w, request.ID, -32602, "tools/call params must be an object")
			return
		}
		rawCall, _ := json.Marshal(params)
		var call toolCall
		if err := json.Unmarshal(rawCall, &call); err != nil || call.Name == "" {
			writeError(w, request.ID, -32602, "tool name is required")
			return
		}
		s.callTool(w, request.ID, r.Header.Get("Authorization"), call)
	default:
		writeError(w, request.ID, -32601, "method not found")
	}
}

func (s *Server) Configured() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configured
}

func toolDefinitions() []map[string]any {
	numberSchema := func(description string) map[string]any {
		return map[string]any{"type": "integer", "description": description}
	}
	return []map[string]any{
		{
			"name":        "get_latest_news",
			"title":       "Get latest news",
			"description": "Get the newest NewsLiquid/OpenNews flash items, newest first.",
			"inputSchema": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"limit": numberSchema(fmt.Sprintf("Maximum items to return (default: %d, max: %d).", defaultLimit, maxLimit)),
					"page":  numberSchema("One-based result page (default: 1)."),
				},
			},
			"outputSchema": newsResultSchema(),
			"annotations":  map[string]any{"title": "Get latest news", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": true},
		},
		{
			"name":        "search_news",
			"title":       "Search news",
			"description": "Search NewsLiquid/OpenNews flash items by keyword, newest first.",
			"inputSchema": map[string]any{
				"type":     "object",
				"required": []string{"keyword"},
				"properties": map[string]any{
					"keyword": map[string]any{"type": "string", "minLength": 1, "description": "Search keyword."},
					"limit":   numberSchema(fmt.Sprintf("Maximum items to return (default: %d, max: %d).", defaultLimit, maxLimit)),
					"page":    numberSchema("One-based result page (default: 1)."),
				},
				"additionalProperties": false,
			},
			"outputSchema": newsResultSchema(),
			"annotations":  map[string]any{"title": "Search news", "readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": true},
		},
	}
}

func newsResultSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"data": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"items": map[string]any{"type": "array", "items": map[string]any{
						"type":       "object",
						"properties": map[string]any{"title": map[string]any{"type": "string"}, "content": map[string]any{"type": "string"}, "time": map[string]any{"type": "string"}, "url": map[string]any{"type": "string"}, "id": map[string]any{}},
					}},
					"next_cursor": map[string]any{"type": "string"},
					"has_more":    map[string]any{"type": "boolean"},
				},
			},
		},
	}
}

func (s *Server) callTool(w http.ResponseWriter, id json.RawMessage, authorization string, call toolCall) {
	var upstream map[string]any
	switch call.Name {
	case "get_latest_news":
		limit, page, err := paging(call.Arguments)
		if err != nil {
			writeToolValidation(w, id, err)
			return
		}
		upstream = map[string]any{"limit": limit, "page": page}
	case "search_news":
		keyword := strings.TrimSpace(stringValue(call.Arguments["keyword"]))
		if keyword == "" {
			writeToolValidation(w, id, fmt.Errorf("keyword is required"))
			return
		}
		limit, page, err := paging(call.Arguments)
		if err != nil {
			writeToolValidation(w, id, err)
			return
		}
		upstream = map[string]any{"query": keyword, "limit": limit, "page": page}
	default:
		writeError(w, id, -32602, "unknown tool: "+call.Name)
		return
	}
	payload, err := s.search(context.Background(), authorization, upstream)
	if err != nil {
		respondToolError(w, id, err)
		return
	}
	items := make([]newsItem, 0)
	for _, raw := range payload.Data {
		content := strings.TrimSpace(raw.Text)
		if content == "" {
			content = strings.TrimSpace(raw.Description)
		}
		if content == "" {
			continue
		}
		items = append(items, newsItem{Title: content, Content: content, Time: strings.TrimSpace(raw.TS), URL: strings.TrimSpace(raw.Link), ID: raw.ID})
	}
	nextCursor, hasMore := "", false
	limit, page := upstreamLimit(upstream), upstreamPage(upstream)
	if len(items) > 0 && len(payload.Data) == limit {
		nextCursor = strconv.Itoa(page + 1)
		hasMore = payload.Total == 0 || page*limit < payload.Total
	}
	structured := map[string]any{"data": map[string]any{"items": items, "next_cursor": nextCursor, "has_more": hasMore}}
	text, _ := json.Marshal(structured)
	writeResult(w, id, map[string]any{"content": []map[string]any{{"type": "text", "text": string(text)}}, "structuredContent": structured, "isError": false})
}

func upstreamLimit(arguments map[string]any) int {
	limit, err := intValue(arguments["limit"])
	if err != nil || limit <= 0 {
		return defaultLimit
	}
	return limit
}

func upstreamPage(arguments map[string]any) int {
	page, err := intValue(arguments["page"])
	if err != nil || page <= 0 {
		return 1
	}
	return page
}

func paging(arguments map[string]any) (int, int, error) {
	limit, err := intValue(arguments["limit"])
	if err != nil {
		return 0, 0, err
	}
	if limit == 0 {
		limit = defaultLimit
	}
	if limit < 0 || limit > maxLimit {
		return 0, 0, fmt.Errorf("limit must be between 1 and %d", maxLimit)
	}
	page, err := intValue(arguments["page"])
	if err != nil {
		return 0, 0, err
	}
	if page < 0 {
		return 0, 0, fmt.Errorf("page must be positive")
	}
	if page == 0 {
		page = 1
	}
	return limit, page, nil
}

func writeToolValidation(w http.ResponseWriter, id json.RawMessage, err error) {
	writeResult(w, id, map[string]any{"content": []map[string]string{{"type": "text", "text": err.Error()}}, "isError": true})
}

func respondToolError(w http.ResponseWriter, id json.RawMessage, err error) {
	message := "upstream request failed"
	if status := statusCode(err); status == http.StatusUnauthorized || status == http.StatusForbidden {
		message = "upstream authentication failed - check the configured API token"
	} else if status > 0 {
		message = fmt.Sprintf("upstream request failed - HTTP %d", status)
	}
	if detail := statusDetail(err); detail != "" && !strings.Contains(message, detail) {
		message += ": " + detail
	}
	writeResult(w, id, map[string]any{"content": []map[string]string{{"type": "text", "text": message}}, "isError": true})
}

type upstreamNews struct {
	ID          any    `json:"id"`
	Text        string `json:"text"`
	Description string `json:"description"`
	Link        string `json:"link"`
	TS          string `json:"ts"`
}

type upstreamResponse struct {
	Data    []upstreamNews `json:"data"`
	Total   int            `json:"total"`
	Success bool           `json:"success"`
}

type statusError struct {
	status int
	body   string
}

func (e *statusError) Error() string {
	if e.body != "" {
		return fmt.Sprintf("upstream returned HTTP %d: %s", e.status, e.body)
	}
	return fmt.Sprintf("upstream returned HTTP %d", e.status)
}

func statusCode(err error) int {
	for current := err; current != nil; {
		if typed, ok := current.(*statusError); ok {
			return typed.status
		}
		unwrapped, ok := current.(interface{ Unwrap() error })
		if !ok {
			return 0
		}
		current = unwrapped.Unwrap()
	}
	return 0
}

func statusDetail(err error) string {
	for current := err; current != nil; {
		if typed, ok := current.(*statusError); ok {
			return typed.body
		}
		unwrapped, ok := current.(interface{ Unwrap() error })
		if !ok {
			return ""
		}
		current = unwrapped.Unwrap()
	}
	return ""
}

func (s *Server) search(ctx context.Context, authorization string, payload map[string]any) (upstreamResponse, error) {
	token := s.token
	if authorization != "" {
		token = strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return upstreamResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/open/news_search", bytes.NewReader(body))
	if err != nil {
		return upstreamResponse{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return upstreamResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(response.Body, 300))
		detail := strings.TrimSpace(string(snippet))
		if runes := []rune(detail); len(runes) > 200 {
			detail = string(runes[:200])
		}
		return upstreamResponse{}, &statusError{status: response.StatusCode, body: detail}
	}
	var result upstreamResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, maxUpstreamBody)).Decode(&result); err != nil {
		return upstreamResponse{}, err
	}
	return result, nil
}

func intValue(value any) (int, error) {
	switch typed := value.(type) {
	case nil:
		return 0, nil
	case float64:
		if typed != float64(int64(typed)) {
			return 0, fmt.Errorf("value must be an integer")
		}
		return int(typed), nil
	case int:
		return typed, nil
	case json.Number:
		return strconv.Atoi(typed.String())
	case string:
		if strings.TrimSpace(typed) == "" {
			return 0, nil
		}
		return strconv.Atoi(strings.TrimSpace(typed))
	default:
		return 0, fmt.Errorf("value must be an integer")
	}
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func decodeObject(raw json.RawMessage) (map[string]any, bool) {
	if len(raw) == 0 {
		return map[string]any{}, true
	}
	var value map[string]any
	return value, json.Unmarshal(raw, &value) == nil && value != nil
}

func writeResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func writeError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id, "error": rpcError{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
