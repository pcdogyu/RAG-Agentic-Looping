package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

type searchInput struct {
	Query     string  `json:"query"`
	SourceID  *string `json:"source_id"`
	Language  string  `json:"language"`
	TimeRange string  `json:"time_range"`
	Limit     int     `json:"limit"`
}

func (s *Server) adminSearch(w http.ResponseWriter, r *http.Request) {
	input := searchInput{Language: "zh-CN", Limit: 10}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.Query = strings.TrimSpace(input.Query)
	if input.Query == "" || len([]rune(input.Query)) > 500 || len(input.Language) > 20 || len(input.TimeRange) > 30 || input.Limit < 1 || input.Limit > 20 {
		writeError(w, http.StatusUnprocessableEntity, "invalid search request")
		return
	}
	if input.SourceID != nil && strings.TrimSpace(*input.SourceID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(*input.SourceID)); err != nil {
			writeError(w, http.StatusUnprocessableEntity, "invalid source_id")
			return
		}
	}
	query := mcpSelect + ` WHERE s.enabled=true`
	args := []any{}
	if input.SourceID != nil && strings.TrimSpace(*input.SourceID) != "" {
		query += ` AND s.id=$1`
		args = append(args, strings.TrimSpace(*input.SourceID))
	}
	query += ` ORDER BY s.priority DESC`
	rows, err := s.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, 500, "search source query failed")
		return
	}
	defer rows.Close()
	sources := make([]mcpSourceRow, 0)
	for rows.Next() {
		row, scanErr := scanMCPRow(rows)
		if scanErr != nil {
			continue
		}
		mappings := decodeMappings(row.ToolMappings)
		if mappings["web_search"] != nil || mappings["news_search"] != nil {
			sources = append(sources, row)
		}
	}
	perSource := min(input.Limit, 5)
	if input.SourceID != nil {
		perSource = input.Limit
	}
	buckets := make([][]map[string]any, len(sources))
	errorsOut := make([]map[string]string, 0)
	for index, source := range sources {
		items, callErr := s.searchMCPSource(r, source, input, perSource)
		if callErr != nil {
			errorsOut = append(errorsOut, map[string]string{"source": source.Name, "error": truncateText(fmt.Sprintf("%T: %v", callErr, callErr), 500)})
			continue
		}
		buckets[index] = items
	}
	interleaved := make([]map[string]any, 0)
	for offset := 0; ; offset++ {
		added := false
		for _, bucket := range buckets {
			if offset < len(bucket) {
				interleaved = append(interleaved, bucket[offset])
				added = true
			}
		}
		if !added {
			break
		}
	}
	deduped := make([]map[string]any, 0, min(len(interleaved), input.Limit))
	byURL := map[string]map[string]any{}
	for _, item := range interleaved {
		key := stringValue(item["url"])
		if existing := byURL[key]; existing != nil {
			sourcesValue := stringSlice(existing["sources"])
			for _, source := range stringSlice(item["sources"]) {
				if !contains(sourcesValue, source) {
					sourcesValue = append(sourcesValue, source)
				}
			}
			existing["sources"] = sourcesValue
			continue
		}
		byURL[key] = item
		deduped = append(deduped, item)
		if len(deduped) == input.Limit {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": deduped, "errors": errorsOut})
}

func (s *Server) searchMCPSource(r *http.Request, row mcpSourceRow, input searchInput, limit int) ([]map[string]any, error) {
	mappings := decodeMappings(row.ToolMappings)
	purpose := "web_search"
	mapping := mappings[purpose]
	if mapping == nil {
		purpose, mapping = "news_search", mappings["news_search"]
	}
	if mapping == nil {
		return nil, fmt.Errorf("source has no search mapping")
	}
	arguments := map[string]any{}
	if defaults, ok := mapping["defaults"].(map[string]any); ok {
		for key, value := range defaults {
			arguments[key] = value
		}
	}
	canonical := map[string]any{"query": input.Query, "source_id": stringPointerValue(input.SourceID), "language": input.Language, "time_range": input.TimeRange, "limit": limit}
	if bindings, ok := mapping["input_bindings"].(map[string]any); ok {
		for sourceKey, targetRaw := range bindings {
			target := stringValue(targetRaw)
			if value := canonical[sourceKey]; value != nil && fmt.Sprint(value) != "" {
				arguments[target] = value
			}
		}
	}
	payload, err := s.callMCPTool(r, row, stringValue(mapping["tool_name"]), arguments)
	if err != nil {
		return nil, err
	}
	adapter := defaultValue(stringValue(mapping["output_adapter"]), "search_results_v1")
	return normalizeSearchPayload(payload, row.Name, adapter, limit), nil
}

func (s *Server) callMCPTool(r *http.Request, row mcpSourceRow, tool string, arguments map[string]any) (any, error) {
	headers := map[string]string{}
	if row.AuthType != "none" && row.EncryptedSecret != nil {
		secret, err := s.decryptSecret(*row.EncryptedSecret)
		if err != nil {
			return nil, err
		}
		if row.AuthType == "bearer" {
			headers["Authorization"] = "Bearer " + secret
		} else {
			key := "X-API-Key"
			if row.AuthHeader != nil && *row.AuthHeader != "" {
				key = *row.AuthHeader
			}
			headers[key] = secret
		}
	}
	client := &http.Client{Timeout: s.cfg.WebSearchTimeout}
	initialize := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "rag-agentic-looping-go", "version": "3"}}}
	_, session, err := mcpRequest(r.Context(), client, row.URL, headers, "", initialize)
	if err != nil {
		return nil, err
	}
	_, _, _ = mcpRequest(r.Context(), client, row.URL, headers, session, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{}})
	response, _, err := mcpRequest(r.Context(), client, row.URL, headers, session, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": tool, "arguments": arguments}})
	if err != nil {
		return nil, err
	}
	result, _ := response["result"].(map[string]any)
	if boolValue(result["isError"]) {
		return nil, fmt.Errorf("MCP tool returned an error")
	}
	if structured := result["structuredContent"]; structured != nil {
		return structured, nil
	}
	values := make([]any, 0)
	for _, raw := range anySlice(result["content"]) {
		item, _ := raw.(map[string]any)
		textValue := stringValue(item["text"])
		if textValue == "" {
			continue
		}
		var decoded any
		if json.Unmarshal([]byte(textValue), &decoded) == nil {
			values = append(values, decoded)
		} else {
			values = append(values, textValue)
		}
	}
	if len(values) == 1 {
		return values[0], nil
	}
	return values, nil
}

func normalizeSearchPayload(payload any, source, adapter string, limit int) []map[string]any {
	items := adapterItems(payload, adapter)
	result := make([]map[string]any, 0, min(limit, len(items)))
	for _, item := range items {
		content := strings.TrimSpace(stringValue(item["content"]))
		title := strings.TrimSpace(stringValue(item["title"]))
		if title == "" {
			title = flashHeadline(content, 120)
		}
		urlValue := strings.TrimSpace(defaultValue(stringValue(item["url"]), stringValue(item["link"])))
		snippet := strings.TrimSpace(firstString(item, "snippet", "content", "summary", "introduction"))
		parsed, err := url.Parse(urlValue)
		if title == "" || snippet == "" || err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			continue
		}
		var published any
		if stamp := parseAnyTime(firstAny(item, "published_at", "publishedDate", "date", "time")); stamp != nil {
			published = jsonTime(*stamp)
		}
		result = append(result, map[string]any{"title": title, "url": canonicalSearchURL(parsed), "snippet": truncateText(snippet, 2000), "source": source, "sources": []string{source}, "domain": strings.ToLower(parsed.Host), "published_at": published})
		if len(result) == limit {
			break
		}
	}
	return result
}

func adapterItems(payload any, adapter string) []map[string]any {
	var raw any = payload
	if object, ok := payload.(map[string]any); ok {
		if adapter == "jin10_flash_v1" {
			data, _ := object["data"].(map[string]any)
			raw = data["items"]
		} else {
			raw = firstAny(object, "results", "items", "data")
		}
	}
	if object, ok := raw.(map[string]any); ok {
		raw = []any{object}
	}
	result := make([]map[string]any, 0)
	for _, item := range anySlice(raw) {
		if object, ok := item.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

func canonicalSearchURL(parsed *url.URL) string {
	parsed.Scheme, parsed.Host, parsed.Fragment = strings.ToLower(parsed.Scheme), strings.ToLower(parsed.Host), ""
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	query := parsed.Query()
	for key := range query {
		if strings.HasPrefix(strings.ToLower(key), "utm_") {
			query.Del(key)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func flashHeadline(value string, limit int) string {
	compact := strings.Join(strings.Fields(value), " ")
	if compact == "" {
		return ""
	}
	for _, delimiter := range []string{"。", "！", "？", "；", ".", "!", "?"} {
		if index := strings.Index(compact, delimiter); index >= 0 {
			compact = compact[:index+len(delimiter)]
			break
		}
	}
	runes := []rune(compact)
	if len(runes) > limit {
		return string(runes[:limit-1]) + "…"
	}
	return compact
}

func firstAny(value map[string]any, keys ...string) any {
	for _, key := range keys {
		if item := value[key]; item != nil && fmt.Sprint(item) != "" {
			return item
		}
	}
	return nil
}

func firstString(value map[string]any, keys ...string) string {
	item := firstAny(value, keys...)
	if item == nil {
		return ""
	}
	return fmt.Sprint(item)
}

func stringSlice(value any) []string {
	result := make([]string, 0)
	for _, item := range anySlice(value) {
		result = append(result, fmt.Sprint(item))
	}
	if typed, ok := value.([]string); ok {
		result = append(result, typed...)
	}
	return result
}
