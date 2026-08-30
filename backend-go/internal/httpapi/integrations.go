package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

type sourceFilterInput struct {
	Enabled           *bool     `json:"enabled"`
	WhitelistKeywords *[]string `json:"whitelist_keywords"`
	BlacklistKeywords *[]string `json:"blacklist_keywords"`
}

func defaultSourceFilter() map[string]any {
	return map[string]any{"enabled": true, "whitelist_keywords": []string{}, "blacklist_keywords": []string{"天气"}}
}

func (s *Server) sourceFilter(w http.ResponseWriter, r *http.Request) { s.writeSourceFilter(w, r) }

func (s *Server) writeSourceFilter(w http.ResponseWriter, r *http.Request) {
	payload := defaultSourceFilter()
	var body []byte
	var updated *time.Time
	err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb,updated_at FROM integration_settings WHERE key='source-filter'`).Scan(&body, &updated)
	if err == nil {
		var stored map[string]any
		if json.Unmarshal(body, &stored) == nil {
			for key, value := range stored {
				payload[key] = value
			}
		}
	}
	var count int
	var last *time.Time
	if err := s.db.QueryRow(r.Context(), `SELECT count(*)::int,max(last_filtered_at) FROM news_filter_logs`).Scan(&count, &last); err != nil {
		writeError(w, http.StatusInternalServerError, "source filter query failed")
		return
	}
	payload["retained_log_count"], payload["last_filtered_at"], payload["updated_at"] = count, timeOrNil(last), timeOrNil(updated)
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) updateSourceFilter(w http.ResponseWriter, r *http.Request) {
	var input sourceFilterInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	whitelist, blacklist := []string{}, []string{"天气"}
	if input.WhitelistKeywords != nil {
		whitelist = normalizeKeywords(*input.WhitelistKeywords)
	}
	if input.BlacklistKeywords != nil {
		blacklist = normalizeKeywords(*input.BlacklistKeywords)
	}
	if len(whitelist) > 200 || len(blacklist) > 200 {
		writeError(w, 422, "keyword lists must not exceed 200 items")
		return
	}
	for _, value := range append(append([]string{}, whitelist...), blacklist...) {
		if len([]rune(value)) > 80 {
			writeError(w, 422, "keywords must not exceed 80 characters")
			return
		}
	}
	body, _ := json.Marshal(map[string]any{"enabled": enabled, "whitelist_keywords": whitelist, "blacklist_keywords": blacklist})
	_, err := s.db.Exec(r.Context(), `INSERT INTO integration_settings(key,payload,updated_at) VALUES('source-filter',$1,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, body)
	if err != nil {
		writeError(w, 500, "source filter update failed")
		return
	}
	s.writeSourceFilter(w, r)
}

func (s *Server) resetSourceFilter(w http.ResponseWriter, r *http.Request) {
	if _, err := s.db.Exec(r.Context(), `DELETE FROM integration_settings WHERE key='source-filter'`); err != nil {
		writeError(w, 500, "source filter reset failed")
		return
	}
	s.writeSourceFilter(w, r)
}

func (s *Server) sourceFilterLogs(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT id,source,title,url,matched_keyword,published_at,first_filtered_at,last_filtered_at,hit_count FROM news_filter_logs ORDER BY last_filtered_at DESC,id DESC LIMIT $1`, limit)
	if err != nil {
		writeError(w, 500, "source filter log query failed")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var id, source, title, urlValue, keyword string
		var published, first, last time.Time
		var hits int
		if rows.Scan(&id, &source, &title, &urlValue, &keyword, &published, &first, &last, &hits) != nil {
			continue
		}
		items = append(items, map[string]any{"id": id, "source": source, "title": title, "url": urlValue, "matched_keyword": keyword, "published_at": jsonTime(published), "first_filtered_at": jsonTime(first), "last_filtered_at": jsonTime(last), "hit_count": hits, "rescan_allowed": keyword == "未命中白名单"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) weknora(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM integration_settings WHERE key='weknora'`).Scan(&body); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(body)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": s.cfg.WeknoraURL})
}

type urlInput struct {
	URL string `json:"url"`
}

func (s *Server) updateWeknora(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var input urlInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if !validHTTPURL(input.URL) {
		writeError(w, 422, "invalid URL")
		return
	}
	body, _ := json.Marshal(map[string]string{"url": input.URL})
	if _, err := s.db.Exec(r.Context(), `INSERT INTO integration_settings(key,payload,updated_at) VALUES('weknora',$1,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, body); err != nil {
		writeError(w, 500, "Weknora update failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": input.URL})
}

func (s *Server) testWeknora(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var input urlInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if !validHTTPURL(input.URL) {
		writeError(w, 422, "invalid URL")
		return
	}
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, input.URL, nil)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": "invalid URL"})
		return
	}
	response, err := client.Do(request)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": fmt.Sprintf("%T: request failed", err)})
		return
	}
	defer response.Body.Close()
	writeJSON(w, 200, map[string]any{"ok": response.StatusCode < 500, "status_code": response.StatusCode})
}

type factGroupMeta struct{ ID, Badge, Name, Description, Tone string }

var factGroups = []factGroupMeta{
	{"fmp", "US", "FMP 美股数据", "美股行情、财务报表、估值指标与公司基础数据", "amber"},
	{"sec", "OFFICIAL", "SEC 官方文件", "SEC EDGAR 监管文件与公司申报记录", "cyan"},
	{"cn_news", "CN / NEWS", "A股与新闻", "AkShare 主数据、市场新闻、公告与 RSS 事实来源", "amber"},
	{"crypto", "CRYPTO", "数字资产", "CoinGecko、DeFiLlama 与 CCXT Kraken 交叉验证", "cyan"},
	{"search", "WEB / SEARCH", "网络搜索与交叉验证", "跨市场网页搜索、独立来源验证与实时补充证据", "mint"},
}
var otherGroup = factGroupMeta{"other", "OTHER", "其他数据源", "尚未归入固定事实领域的自定义 MCP 来源", "neutral"}

func (s *Server) factSourceGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.loadFactSourceGroups(r)
	if err != nil {
		writeError(w, 500, "fact source query failed")
		return
	}
	writeJSON(w, 200, groups)
}

func (s *Server) loadFactSourceGroups(r *http.Request) ([]map[string]any, error) {
	sources, err := s.loadMCPSources(r.Context())
	if err != nil {
		return nil, err
	}
	grouped := map[string][]map[string]any{}
	for _, source := range sources {
		group := stringValue(source["group_id"])
		grouped[group] = append(grouped[group], source)
	}
	metadata := append(append([]factGroupMeta{}, factGroups...), otherGroup)
	result := make([]map[string]any, 0, len(metadata))
	for _, meta := range metadata {
		members := grouped[meta.ID]
		if members == nil {
			members = make([]map[string]any, 0)
		}
		if meta.ID == "other" && len(members) == 0 {
			continue
		}
		config, source, err := s.factGroupConfig(r, meta.ID)
		if err != nil {
			return nil, err
		}
		status := factGroupStatus(meta.ID, config, members)
		item := map[string]any{"id": meta.ID, "badge": meta.Badge, "name": meta.Name, "description": meta.Description, "tone": meta.Tone, "config": config, "config_source": source, "status": status, "configured_count": configuredCount(config), "mcp_count": len(members), "mcp_sources": members}
		result = append(result, item)
	}
	return result, nil
}

func (s *Server) factGroupConfig(r *http.Request, groupID string) (map[string]any, string, error) {
	base := s.defaultFactGroupConfig(groupID)
	if groupID == "other" {
		return base, "environment", nil
	}
	var body []byte
	err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM integration_settings WHERE key=$1`, "fact-source:"+groupID).Scan(&body)
	if err != nil {
		return base, "environment", nil
	}
	var stored map[string]any
	if json.Unmarshal(body, &stored) != nil {
		return base, "environment", nil
	}
	for key, value := range stored {
		if key == "encrypted_access_token" || key == "access_token_disabled" {
			continue
		}
		base[key] = value
	}
	if groupID == "fmp" {
		if boolValue(stored["access_token_disabled"]) {
			base["access_token_configured"] = false
			base["access_token_source"] = "disabled"
		} else if stringValue(stored["encrypted_access_token"]) != "" {
			base["access_token_configured"] = true
			base["access_token_source"] = "database"
		}
	}
	return base, "database", nil
}

func (s *Server) defaultFactGroupConfig(groupID string) map[string]any {
	switch groupID {
	case "fmp":
		source := "unconfigured"
		if s.cfg.FMPAccessToken != "" {
			source = "environment"
		}
		return map[string]any{"base_url": s.cfg.FMPBaseURL, "rate_limit_per_minute": s.cfg.FMPRateLimit, "news_lookback_hours": s.cfg.FMPNewsLookback, "access_token_configured": s.cfg.FMPAccessToken != "", "access_token_source": source, "mcp_upstream_token_management": "environment"}
	case "sec":
		return map[string]any{"identity": s.cfg.SECIdentity}
	case "cn_news":
		return map[string]any{"akshare_asset_master_enabled": s.cfg.AkshareEnabled, "akshare_ipv4_only": s.cfg.AkshareIPv4Only, "rss_feed_urls": s.cfg.RSSFeeds, "official_rss_feed_urls": s.cfg.OfficialRSSFeeds}
	case "crypto":
		return map[string]any{"coingecko_base_url": s.cfg.CoinGeckoURL, "defillama_base_url": s.cfg.DefiLlamaURL, "ccxt_exchange": "kraken"}
	case "search":
		return map[string]any{"timeout_seconds": int(s.cfg.WebSearchTimeout.Seconds())}
	default:
		return map[string]any{}
	}
}

func (s *Server) updateFactSourceGroup(w http.ResponseWriter, r *http.Request) {
	group := chi.URLParam(r, "groupID")
	if !validFactGroup(group) || group == "other" {
		writeError(w, 422, "unsupported fact source group")
		return
	}
	var input map[string]any
	if !decodeJSONBody(w, r, &input) {
		return
	}
	stored, err := s.validateFactGroupInput(r.Context(), group, input)
	if err != nil {
		writeError(w, 422, err.Error())
		return
	}
	body, _ := json.Marshal(stored)
	if _, err = s.db.Exec(r.Context(), `INSERT INTO integration_settings(key,payload,updated_at) VALUES($1,$2,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, "fact-source:"+group, body); err != nil {
		writeError(w, 500, "fact source update failed")
		return
	}
	s.writeFactSourceGroup(w, r, group)
}
func (s *Server) resetFactSourceGroup(w http.ResponseWriter, r *http.Request) {
	group := chi.URLParam(r, "groupID")
	if !validFactGroup(group) || group == "other" {
		writeError(w, 422, "unsupported fact source group")
		return
	}
	if _, err := s.db.Exec(r.Context(), `DELETE FROM integration_settings WHERE key=$1`, "fact-source:"+group); err != nil {
		writeError(w, 500, "fact source reset failed")
		return
	}
	s.writeFactSourceGroup(w, r, group)
}
func (s *Server) writeFactSourceGroup(w http.ResponseWriter, r *http.Request, group string) {
	groups, err := s.loadFactSourceGroups(r)
	if err != nil {
		writeError(w, 500, "fact source query failed")
		return
	}
	for _, item := range groups {
		if item["id"] == group {
			writeJSON(w, 200, item)
			return
		}
	}
	writeError(w, 404, "fact source group not found")
}

func (s *Server) testFactSourceGroup(w http.ResponseWriter, r *http.Request) {
	group := chi.URLParam(r, "groupID")
	if !validFactGroup(group) {
		writeError(w, 422, "unsupported fact source group")
		return
	}
	native := s.probeNativeFactGroup(r, group)
	sources, err := s.loadMCPSources(r.Context())
	if err != nil {
		writeError(w, 500, "fact source query failed")
		return
	}
	results := make([]map[string]any, 0)
	ok := boolValue(native["ok"])
	for _, source := range sources {
		if stringValue(source["group_id"]) != group || !boolValue(source["enabled"]) {
			continue
		}
		probe := s.probeMCPSource(r.Context(), stringValue(source["id"]), false)
		status := "failed"
		if probe != nil {
			if public, _ := probe["source"].(map[string]any); public != nil {
				status = stringValue(public["last_status"])
			}
		}
		if status != "healthy" {
			ok = false
		}
		results = append(results, map[string]any{"id": source["id"], "name": source["name"], "status": status})
	}
	writeJSON(w, 200, map[string]any{"ok": ok, "native": native, "mcp_sources": results})
}

func (s *Server) validateFactGroupInput(ctx context.Context, group string, input map[string]any) (map[string]any, error) {
	allowed := map[string]map[string]bool{"fmp": {"base_url": true, "access_token": true, "clear_access_token": true, "rate_limit_per_minute": true, "news_lookback_hours": true}, "sec": {"identity": true}, "cn_news": {"akshare_asset_master_enabled": true, "akshare_ipv4_only": true, "rss_feed_urls": true, "official_rss_feed_urls": true}, "crypto": {"coingecko_base_url": true, "defillama_base_url": true}, "search": {"timeout_seconds": true}}[group]
	for key := range input {
		if !allowed[key] {
			return nil, fmt.Errorf("unexpected field: %s", key)
		}
	}
	current := map[string]any{}
	var body []byte
	if s.db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key=$1`, "fact-source:"+group).Scan(&body) == nil {
		_ = json.Unmarshal(body, &current)
	}
	stored := cloneMap(input)
	if group == "fmp" {
		token := stringValue(input["access_token"])
		clear := boolValue(input["clear_access_token"])
		delete(stored, "access_token")
		delete(stored, "clear_access_token")
		if token != "" {
			encrypted, err := s.encryptSecret(token)
			if err != nil {
				return nil, err
			}
			stored["encrypted_access_token"] = encrypted
			stored["access_token_disabled"] = false
		} else if clear {
			stored["encrypted_access_token"] = ""
			stored["access_token_disabled"] = true
		} else {
			for _, key := range []string{"encrypted_access_token", "access_token_disabled"} {
				if value, ok := current[key]; ok {
					stored[key] = value
				}
			}
		}
	}
	for _, key := range []string{"base_url", "coingecko_base_url", "defillama_base_url"} {
		if value, ok := stored[key]; ok && !validHTTPURL(stringValue(value)) {
			return nil, fmt.Errorf("%s must be an HTTP URL", key)
		}
	}
	return stored, nil
}

func (s *Server) probeNativeFactGroup(r *http.Request, group string) map[string]any {
	config, _, _ := s.factGroupConfig(r, group)
	client := &http.Client{Timeout: minDuration(s.cfg.WebSearchTimeout, 20*time.Second)}
	target := ""
	headers := map[string]string{}
	switch group {
	case "fmp":
		if !boolValue(config["access_token_configured"]) {
			return map[string]any{"ok": false, "status": "pending", "detail": "FMP REST Token 未配置"}
		}
		target = strings.TrimRight(stringValue(config["base_url"]), "/") + "/quote?symbol=AAPL"
		headers["apikey"] = s.cfg.FMPAccessToken
	case "sec":
		if stringValue(config["identity"]) == "" {
			return map[string]any{"ok": false, "status": "pending", "detail": "SEC Identity 未配置"}
		}
		target = "https://data.sec.gov/submissions/CIK0000320193.json"
		headers["User-Agent"] = stringValue(config["identity"])
	case "crypto":
		target = strings.TrimRight(stringValue(config["coingecko_base_url"]), "/") + "/ping"
	case "cn_news":
		if boolValue(config["akshare_asset_master_enabled"]) {
			return map[string]any{"ok": true, "status": "healthy", "detail": "AkShare 已启用"}
		}
		return map[string]any{"ok": false, "status": "pending", "detail": "未配置 RSS"}
	default:
		return map[string]any{"ok": true, "status": "healthy", "detail": "使用所属 MCP 执行连接测试"}
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
	if err != nil {
		return map[string]any{"ok": false, "status": "failed", "detail": "invalid URL"}
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return map[string]any{"ok": false, "status": "failed", "detail": "upstream request failed"}
	}
	defer response.Body.Close()
	ok := response.StatusCode < 400
	status := "failed"
	if ok {
		status = "healthy"
	}
	return map[string]any{"ok": ok, "status": status, "status_code": response.StatusCode}
}

func factGroupStatus(group string, config map[string]any, sources []map[string]any) string {
	for _, source := range sources {
		if boolValue(source["enabled"]) && (source["last_status"] == "failed" || source["last_error"] != nil) {
			return "failed"
		}
	}
	ready := false
	switch group {
	case "fmp":
		ready = boolValue(config["access_token_configured"])
	case "sec":
		ready = stringValue(config["identity"]) != ""
	case "cn_news":
		ready = boolValue(config["akshare_asset_master_enabled"]) || sliceLen(config["rss_feed_urls"]) > 0 || sliceLen(config["official_rss_feed_urls"]) > 0
	case "crypto":
		ready = stringValue(config["coingecko_base_url"]) != "" && stringValue(config["defillama_base_url"]) != ""
	case "search":
		ready = int64Value(config["timeout_seconds"]) > 0
	case "other":
		ready = true
	}
	if !ready {
		return "pending"
	}
	for _, source := range sources {
		if boolValue(source["enabled"]) && source["last_status"] != "healthy" {
			return "pending"
		}
	}
	if group == "other" && len(sources) == 0 {
		return "pending"
	}
	return "healthy"
}
func configuredCount(config map[string]any) int {
	ignored := map[string]bool{"access_token_source": true, "mcp_upstream_token_management": true, "ccxt_exchange": true}
	count := 0
	for key, value := range config {
		if !ignored[key] && !zeroValue(value) {
			count++
		}
	}
	return count
}
func validFactGroup(value string) bool {
	for _, item := range factGroups {
		if item.ID == value {
			return true
		}
	}
	return value == "other"
}
func validHTTPURL(value string) bool {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(value))
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}
func normalizeKeywords(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		key := strings.ToLower(value)
		if value == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}
func sliceLen(value any) int {
	switch typed := value.(type) {
	case []any:
		return len(typed)
	case []string:
		return len(typed)
	}
	return 0
}
func zeroValue(value any) bool {
	if value == nil {
		return true
	}
	switch typed := value.(type) {
	case string:
		return typed == ""
	case bool:
		return !typed
	case float64:
		return typed == 0
	case int:
		return typed == 0
	case []any:
		return len(typed) == 0
	case []string:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	}
	return false
}
func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}
