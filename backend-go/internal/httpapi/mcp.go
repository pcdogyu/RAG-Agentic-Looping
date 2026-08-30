package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fernet/fernet-go"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	mcpPurposes   = map[string]bool{"web_search": true, "news_search": true, "news_feed": true, "asset_search": true, "quote": true, "fundamentals": true, "filings": true}
	mcpAdapters   = map[string]bool{"search_results_v1": true, "news_items_v1": true, "asset_list_v1": true, "raw_records_v1": true, "filings_v1": true, "jin10_flash_v1": true}
	builtinGroups = map[string]string{"FMP": "fmp", "SearXNG": "search", "DuckDuckGo": "search", "金十数据": "cn_news"}
)

type mcpSourceInput struct {
	Name           string                    `json:"name"`
	URL            string                    `json:"url"`
	Description    string                    `json:"description"`
	Priority       *int                      `json:"priority"`
	Enabled        *bool                     `json:"enabled"`
	AuthType       string                    `json:"auth_type"`
	AuthHeaderName *string                   `json:"auth_header_name"`
	Secret         *string                   `json:"secret"`
	ClearSecret    bool                      `json:"clear_secret"`
	ToolMappings   map[string]map[string]any `json:"tool_mappings"`
	GroupID        string                    `json:"group_id"`
}

type mcpSourceRow struct {
	ID, Name, URL, Description, AuthType string
	Priority                             int
	Enabled, Managed                     bool
	AuthHeader, EncryptedSecret          *string
	DiscoveredTools                      []byte
	ToolMappings                         []byte
	LastStatus                           string
	LastError                            *string
	LastChecked                          *time.Time
	Created, Updated                     time.Time
	GroupID                              string
}

const mcpSelect = `SELECT s.id,s.name,s.url,s.description,s.priority,s.enabled,s.managed,s.auth_type,s.auth_header_name,s.encrypted_secret,
	s.discovered_tools::jsonb,s.tool_mappings::jsonb,s.last_status,s.last_error,s.last_checked_at,s.created_at,s.updated_at,
	coalesce(g.payload->>'group_id','') FROM mcp_sources s LEFT JOIN integration_settings g ON g.key='mcp-source-group:'||s.id`

func (s *Server) loadMCPSources(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, mcpSelect+` ORDER BY s.priority DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		row, err := scanMCPRow(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, mcpPayload(row))
	}
	return items, rows.Err()
}
func (s *Server) mcpSources(w http.ResponseWriter, r *http.Request) {
	items, err := s.loadMCPSources(r.Context())
	if err != nil {
		writeError(w, 500, "MCP source query failed")
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) createMCPSource(w http.ResponseWriter, r *http.Request) {
	var input mcpSourceInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	normalizeMCPInput(&input)
	if err := validateMCPInput(input, nil); err != nil {
		writeError(w, 422, err.Error())
		return
	}
	id := uuid.New().String()
	encrypted, err := s.newEncryptedSecret(input)
	if err != nil {
		writeError(w, 422, err.Error())
		return
	}
	enabled := *input.Enabled
	if input.AuthType != "none" {
		enabled = false
	}
	tools, _ := json.Marshal([]any{})
	mappings, _ := json.Marshal(input.ToolMappings)
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, "MCP source create failed")
		return
	}
	defer tx.Rollback(r.Context()) //nolint:errcheck
	_, err = tx.Exec(r.Context(), `INSERT INTO mcp_sources(id,name,url,description,priority,enabled,managed,auth_type,auth_header_name,encrypted_secret,discovered_tools,tool_mappings,last_status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,false,$7,$8,$9,$10,$11,'unchecked',now(),now())`, id, input.Name, input.URL, input.Description, *input.Priority, enabled, input.AuthType, input.AuthHeaderName, encrypted, tools, mappings)
	if err != nil {
		if strings.Contains(err.Error(), "unique") {
			writeError(w, 409, "source name already exists")
		} else {
			writeError(w, 500, "MCP source create failed")
		}
		return
	}
	if err = setSourceGroup(r.Context(), tx, id, input.GroupID); err != nil {
		writeError(w, 500, "MCP source create failed")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, "MCP source create failed")
		return
	}
	row, err := s.getMCPSource(r.Context(), id)
	if err != nil {
		writeError(w, 500, "MCP source create failed")
		return
	}
	writeJSON(w, 201, mcpPayload(row))
}

func (s *Server) updateMCPSource(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "sourceID")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, 422, "Input should be a valid UUID")
		return
	}
	existing, err := s.getMCPSource(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "MCP source not found")
		return
	}
	if err != nil {
		writeError(w, 500, "MCP source query failed")
		return
	}
	var input mcpSourceInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	normalizeMCPInput(&input)
	tools := decodeArray(existing.DiscoveredTools)
	if err = validateMCPInput(input, tools); err != nil {
		writeError(w, 422, err.Error())
		return
	}
	if existing.Managed && input.GroupID != existing.GroupID {
		writeError(w, 409, "managed source group cannot be changed")
		return
	}
	connectionChanged := existing.URL != input.URL || existing.AuthType != input.AuthType || stringPtrValue(existing.AuthHeader) != stringPtrValue(input.AuthHeaderName) || input.Secret != nil || input.ClearSecret
	mappings, _ := json.Marshal(input.ToolMappings)
	mappingsChanged := !jsonEqual(existing.ToolMappings, mappings)
	encrypted := existing.EncryptedSecret
	if input.ClearSecret || input.AuthType == "none" {
		encrypted = nil
	} else if input.Secret != nil && strings.TrimSpace(*input.Secret) != "" {
		value, encryptErr := s.encryptSecret(*input.Secret)
		if encryptErr != nil {
			writeError(w, 422, encryptErr.Error())
			return
		}
		encrypted = &value
	}
	enabled := *input.Enabled
	lastStatus := existing.LastStatus
	lastError := existing.LastError
	lastChecked := existing.LastChecked
	discovered := existing.DiscoveredTools
	if connectionChanged {
		discovered, _ = json.Marshal([]any{})
		lastStatus = "unchecked"
		lastError = nil
		lastChecked = nil
	} else if mappingsChanged {
		if len(tools) > 0 {
			lastStatus = "discovered"
		} else {
			lastStatus = "unchecked"
		}
		lastError = nil
		lastChecked = nil
	}
	if input.AuthType != "none" && (connectionChanged || mappingsChanged || (enabled && !existing.Enabled)) {
		enabled = false
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, "MCP source update failed")
		return
	}
	defer tx.Rollback(r.Context()) //nolint:errcheck
	_, err = tx.Exec(r.Context(), `UPDATE mcp_sources SET name=$2,url=$3,description=$4,priority=$5,enabled=$6,auth_type=$7,auth_header_name=$8,encrypted_secret=$9,discovered_tools=$10,tool_mappings=$11,last_status=$12,last_error=$13,last_checked_at=$14,updated_at=now() WHERE id=$1`, id, input.Name, input.URL, input.Description, *input.Priority, enabled, input.AuthType, input.AuthHeaderName, encrypted, discovered, mappings, lastStatus, lastError, lastChecked)
	if err != nil {
		if strings.Contains(err.Error(), "unique") {
			writeError(w, 409, "source name already exists")
		} else {
			writeError(w, 500, "MCP source update failed")
		}
		return
	}
	if err = setSourceGroup(r.Context(), tx, id, input.GroupID); err != nil {
		writeError(w, 500, "MCP source update failed")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, "MCP source update failed")
		return
	}
	updated, err := s.getMCPSource(r.Context(), id)
	if err != nil {
		writeError(w, 500, "MCP source update failed")
		return
	}
	writeJSON(w, 200, mcpPayload(updated))
}

func (s *Server) deleteMCPSource(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "sourceID")
	row, err := s.getMCPSource(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "MCP source not found")
		return
	}
	if err != nil {
		writeError(w, 500, "MCP source query failed")
		return
	}
	if row.Managed {
		writeError(w, 409, "managed source cannot be deleted")
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, "MCP source delete failed")
		return
	}
	defer tx.Rollback(r.Context()) //nolint:errcheck
	if _, err = tx.Exec(r.Context(), `DELETE FROM integration_settings WHERE key=$1`, `mcp-source-group:`+id); err == nil {
		_, err = tx.Exec(r.Context(), `DELETE FROM mcp_sources WHERE id=$1`, id)
	}
	if err != nil {
		writeError(w, 500, "MCP source delete failed")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, "MCP source delete failed")
		return
	}
	writeJSON(w, 200, map[string]bool{"deleted": true})
}

func (s *Server) enableMCPSource(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "sourceID")
	var input struct {
		Enabled *bool `json:"enabled"`
	}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.Enabled == nil {
		writeError(w, 422, "enabled is required")
		return
	}
	row, err := s.getMCPSource(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "MCP source not found")
		return
	}
	if err != nil {
		writeError(w, 500, "MCP source query failed")
		return
	}
	if *input.Enabled && row.AuthType != "none" {
		if row.EncryptedSecret == nil {
			writeError(w, 409, "请先配置 MCP 凭据")
			return
		}
		tools := decodeArray(row.DiscoveredTools)
		if len(tools) == 0 {
			writeError(w, 409, "请先完成 MCP 工具发现")
			return
		}
		if err = validateMappings(decodeMappings(row.ToolMappings), tools); err != nil {
			writeError(w, 409, err.Error())
			return
		}
		if row.LastStatus != "healthy" {
			writeError(w, 409, "请先完成 MCP 连接测试")
			return
		}
	}
	if _, err = s.db.Exec(r.Context(), `UPDATE mcp_sources SET enabled=$2,updated_at=now() WHERE id=$1`, id, *input.Enabled); err != nil {
		writeError(w, 500, "MCP source update failed")
		return
	}
	row, err = s.getMCPSource(r.Context(), id)
	if err != nil {
		writeError(w, 500, "MCP source query failed")
		return
	}
	writeJSON(w, 200, mcpPayload(row))
}

func (s *Server) discoverMCPSource(w http.ResponseWriter, r *http.Request) {
	result := s.probeMCPSource(r.Context(), chi.URLParam(r, "sourceID"), true)
	if result == nil {
		writeError(w, 404, "MCP source not found")
		return
	}
	writeJSON(w, 200, result)
}
func (s *Server) testMCPSource(w http.ResponseWriter, r *http.Request) {
	result := s.probeMCPSource(r.Context(), chi.URLParam(r, "sourceID"), false)
	if result == nil {
		writeError(w, 404, "MCP source not found")
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) probeMCPSource(ctx context.Context, id string, discover bool) map[string]any {
	row, err := s.getMCPSource(ctx, id)
	if err != nil {
		return nil
	}
	tools, probeErr := s.listMCPTools(ctx, row)
	status := "failed"
	var lastError any
	if probeErr == nil && len(tools) > 0 {
		if mappingErr := validateMappings(decodeMappings(row.ToolMappings), tools); mappingErr == nil {
			if discover {
				status = "discovered"
			} else {
				status = "healthy"
			}
		} else {
			probeErr = mappingErr
		}
	}
	if probeErr != nil {
		lastError = truncateText(fmt.Sprintf("%T: %v", probeErr, probeErr), 1000)
		if discover {
			tools = []map[string]any{}
		}
	}
	body, _ := json.Marshal(tools)
	_, _ = s.db.Exec(ctx, `UPDATE mcp_sources SET discovered_tools=CASE WHEN $2 OR $4 IS NOT NULL THEN $3 ELSE discovered_tools END,last_status=$5,last_error=$4,last_checked_at=now(),updated_at=now() WHERE id=$1`, id, discover, body, lastError, status)
	updated, err := s.getMCPSource(ctx, id)
	if err != nil {
		return nil
	}
	return map[string]any{"source": mcpPayload(updated), "tools": tools}
}

func (s *Server) listMCPTools(ctx context.Context, row mcpSourceRow) ([]map[string]any, error) {
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
	initialize := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "rag-agentic-looping-go", "version": "2"}}}
	_, session, err := mcpRequest(ctx, client, row.URL, headers, "", initialize)
	if err != nil {
		return nil, err
	}
	notification := map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{}}
	_, _, _ = mcpRequest(ctx, client, row.URL, headers, session, notification)
	response, _, err := mcpRequest(ctx, client, row.URL, headers, session, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": map[string]any{}})
	if err != nil {
		return nil, err
	}
	result, _ := response["result"].(map[string]any)
	rawTools, _ := result["tools"].([]any)
	tools := make([]map[string]any, 0, len(rawTools))
	for _, raw := range rawTools {
		tool, _ := raw.(map[string]any)
		if tool == nil {
			continue
		}
		tools = append(tools, map[string]any{"name": tool["name"], "description": defaultAny(tool["description"], ""), "input_schema": defaultAny(tool["inputSchema"], map[string]any{}), "output_schema": tool["outputSchema"]})
	}
	return tools, nil
}

func mcpRequest(ctx context.Context, client *http.Client, target string, headers map[string]string, session string, payload any) (map[string]any, string, error) {
	body, _ := json.Marshal(payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, session, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if session != "" {
		request.Header.Set("Mcp-Session-Id", session)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, session, err
	}
	defer response.Body.Close()
	if value := response.Header.Get("Mcp-Session-Id"); value != "" {
		session = value
	}
	if response.StatusCode == http.StatusAccepted || response.StatusCode == http.StatusNoContent {
		return map[string]any{}, session, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, session, fmt.Errorf("MCP HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	var output map[string]any
	if strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		scanner := bufio.NewScanner(io.LimitReader(response.Body, 4<<20))
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") {
				if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &output) == nil {
					break
				}
			}
		}
		if output == nil {
			return nil, session, errors.New("MCP stream returned no JSON-RPC response")
		}
	} else if json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&output) != nil {
		return nil, session, errors.New("MCP returned invalid JSON")
	}
	if rpcError := output["error"]; rpcError != nil {
		return nil, session, fmt.Errorf("MCP error: %v", rpcError)
	}
	return output, session, nil
}

func (s *Server) getMCPSource(ctx context.Context, id string) (mcpSourceRow, error) {
	row := s.db.QueryRow(ctx, mcpSelect+` WHERE s.id=$1`, id)
	return scanMCPRow(row)
}

type scanner interface{ Scan(...any) error }

func scanMCPRow(row scanner) (mcpSourceRow, error) {
	var value mcpSourceRow
	err := row.Scan(&value.ID, &value.Name, &value.URL, &value.Description, &value.Priority, &value.Enabled, &value.Managed, &value.AuthType, &value.AuthHeader, &value.EncryptedSecret, &value.DiscoveredTools, &value.ToolMappings, &value.LastStatus, &value.LastError, &value.LastChecked, &value.Created, &value.Updated, &value.GroupID)
	if value.GroupID == "" {
		value.GroupID = builtinGroups[value.Name]
		if value.GroupID == "" {
			value.GroupID = "other"
		}
	}
	return value, err
}
func mcpPayload(row mcpSourceRow) map[string]any {
	return map[string]any{"id": row.ID, "name": row.Name, "url": row.URL, "description": nullableString(row.Description), "priority": row.Priority, "enabled": row.Enabled, "managed": row.Managed, "auth_type": row.AuthType, "auth_header_name": nullableStringPointer(row.AuthHeader), "secret_configured": row.EncryptedSecret != nil && *row.EncryptedSecret != "", "discovered_tools": decodeDefault(row.DiscoveredTools, []any{}), "tool_mappings": decodeDefault(row.ToolMappings, map[string]any{}), "last_status": row.LastStatus, "last_error": nullableStringPointer(row.LastError), "last_checked_at": jsonTimeOrNil(row.LastChecked), "created_at": jsonTime(row.Created), "updated_at": jsonTime(row.Updated), "group_id": row.GroupID}
}

func normalizeMCPInput(input *mcpSourceInput) {
	input.Name = strings.TrimSpace(input.Name)
	input.URL = strings.TrimSpace(input.URL)
	input.Description = strings.TrimSpace(input.Description)
	if input.Priority == nil {
		value := 50
		input.Priority = &value
	}
	if input.Enabled == nil {
		value := true
		input.Enabled = &value
	}
	if input.AuthType == "" {
		input.AuthType = "none"
	}
	if input.ToolMappings == nil {
		input.ToolMappings = map[string]map[string]any{}
	}
	if input.GroupID == "" {
		input.GroupID = "other"
	}
}
func validateMCPInput(input mcpSourceInput, tools []map[string]any) error {
	if input.Name == "" || len([]rune(input.Name)) > 120 {
		return errors.New("name must contain 1 to 120 characters")
	}
	if !validHTTPURL(input.URL) {
		return errors.New("url must be an HTTP URL")
	}
	if input.Priority == nil || *input.Priority < 0 || *input.Priority > 1000 {
		return errors.New("priority must be between 0 and 1000")
	}
	if input.AuthType != "none" && input.AuthType != "bearer" && input.AuthType != "api_key_header" {
		return errors.New("auth_type must be none, bearer, or api_key_header")
	}
	if !validFactGroup(input.GroupID) {
		return errors.New("unsupported fact source group")
	}
	return validateMappings(input.ToolMappings, tools)
}
func validateMappings(mappings map[string]map[string]any, tools []map[string]any) error {
	names := map[string]bool{}
	for _, tool := range tools {
		names[stringValue(tool["name"])] = true
	}
	for purpose, mapping := range mappings {
		if !mcpPurposes[purpose] {
			return fmt.Errorf("unsupported MCP purpose: %s", purpose)
		}
		toolName := stringValue(mapping["tool_name"])
		if toolName == "" {
			return fmt.Errorf("%s mapping requires tool_name", purpose)
		}
		if len(tools) > 0 && !names[toolName] {
			return fmt.Errorf("mapped tool was not discovered: %s", toolName)
		}
		adapter := stringValue(mapping["output_adapter"])
		if adapter == "" {
			adapter = "raw_records_v1"
		}
		if !mcpAdapters[adapter] {
			return fmt.Errorf("unsupported output adapter: %s", adapter)
		}
		if value, ok := mapping["input_bindings"]; ok {
			if _, ok := value.(map[string]any); !ok {
				return errors.New("input_bindings and defaults must be objects")
			}
		}
		if value, ok := mapping["defaults"]; ok {
			if _, ok := value.(map[string]any); !ok {
				return errors.New("input_bindings and defaults must be objects")
			}
		}
	}
	return nil
}

func (s *Server) newEncryptedSecret(input mcpSourceInput) (*string, error) {
	if input.AuthType == "none" {
		return nil, nil
	}
	if input.Secret == nil || strings.TrimSpace(*input.Secret) == "" {
		return nil, errors.New("credential is required for the selected auth type")
	}
	value, err := s.encryptSecret(*input.Secret)
	if err != nil {
		return nil, err
	}
	return &value, nil
}
func (s *Server) encryptSecret(secret string) (string, error) {
	key, err := fernet.DecodeKey(s.cfg.MCPSecretKey)
	if err != nil || key == nil {
		return "", errors.New("MCP_SECRET_KEY is not configured or invalid")
	}
	token, err := fernet.EncryptAndSign([]byte(secret), key)
	return string(token), err
}
func (s *Server) decryptSecret(ciphertext string) (string, error) {
	key, err := fernet.DecodeKey(s.cfg.MCPSecretKey)
	if err != nil || key == nil {
		return "", errors.New("MCP_SECRET_KEY is not configured or invalid")
	}
	value := fernet.VerifyAndDecrypt([]byte(ciphertext), 0, []*fernet.Key{key})
	if value == nil {
		return "", errors.New("stored credential cannot be decrypted")
	}
	return string(value), nil
}

func setSourceGroup(ctx context.Context, tx pgx.Tx, id, group string) error {
	body, _ := json.Marshal(map[string]string{"group_id": group})
	_, err := tx.Exec(ctx, `INSERT INTO integration_settings(key,payload,updated_at) VALUES($1,$2,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, `mcp-source-group:`+id, body)
	return err
}
func decodeArray(body []byte) []map[string]any {
	var raw []map[string]any
	_ = json.Unmarshal(body, &raw)
	return raw
}
func decodeMappings(body []byte) map[string]map[string]any {
	var value map[string]map[string]any
	_ = json.Unmarshal(body, &value)
	if value == nil {
		value = map[string]map[string]any{}
	}
	return value
}
func jsonEqual(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return bytes.Equal(left, right)
	}
	encodedA, _ := json.Marshal(a)
	encodedB, _ := json.Marshal(b)
	return bytes.Equal(encodedA, encodedB)
}
func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func truncateText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
