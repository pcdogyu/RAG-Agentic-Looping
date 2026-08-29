package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type auditRow struct {
	ID, LogicalID, Provider, Model, Operation string
	EntityType, EntityID                      *string
	Attempt                                   int
	Status, Fidelity                          string
	Started, Completed                        time.Time
	Duration, Prompt, Completion              *int
	InputLanguage, OutputLanguage             string
}

func (s *Server) modelLogs(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 100)
	if !ok {
		return
	}
	filter, ok := auditFilterFromRequest(w, r)
	if !ok {
		return
	}
	var cursorTime any
	var cursorID any
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		stamp, id, err := decodeAuditCursor(raw)
		if err != nil {
			writeError(w, 400, "invalid model log cursor")
			return
		}
		cursorTime, cursorID = stamp, id
	}
	rows, err := s.db.Query(r.Context(), auditSelect+` WHERE
		($1::timestamptz IS NULL OR started_at >= $1) AND ($2::timestamptz IS NULL OR started_at <= $2)
		AND ($3='' OR model=$3) AND ($4='' OR provider=$4) AND ($5='' OR operation=$5)
		AND ($6='' OR status=$6) AND ($7='' OR input_language=$7 OR output_language=$7)
		AND ($8='' OR fidelity=$8)
		AND ($9::timestamptz IS NULL OR started_at<$9 OR (started_at=$9 AND id<$10::text))
		ORDER BY started_at DESC,id DESC LIMIT $11`, filter.start, filter.end, filter.model, filter.provider, filter.operation, filter.status, filter.language, filter.fidelity, cursorTime, cursorID, limit+1)
	if err != nil {
		writeError(w, 500, "model logs query failed")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	count := 0
	var last auditRow
	for rows.Next() {
		row, err := scanAudit(rows)
		if err != nil {
			writeError(w, 500, "model log is invalid")
			return
		}
		count++
		if len(items) == limit {
			continue
		}
		items = append(items, auditSummary(row))
		last = row
	}
	var next any
	if count > limit && len(items) > 0 {
		next = encodeAuditCursor(last.Started, last.ID)
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) modelLog(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "auditID")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, 422, "Input should be a valid UUID")
		return
	}
	row := s.db.QueryRow(r.Context(), auditSelect+` WHERE id=$1`, id)
	audit, err := scanAudit(row)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "model log not found")
		return
	}
	if err != nil {
		writeError(w, 500, "model log query failed")
		return
	}
	var messagesBody, schemaBody, parsedBody, metricsBody []byte
	var rawResponse string
	var errorValue *string
	err = s.db.QueryRow(r.Context(), `SELECT messages::jsonb,schema_payload::jsonb,parsed_response::jsonb,metrics::jsonb,raw_response,error FROM model_call_audits WHERE id=$1`, id).Scan(&messagesBody, &schemaBody, &parsedBody, &metricsBody, &rawResponse, &errorValue)
	if err != nil {
		writeError(w, 500, "model log detail query failed")
		return
	}
	result := auditSummary(audit)
	result["messages"] = decodeDefault(messagesBody, []any{})
	result["schema"] = decodeDefault(schemaBody, map[string]any{})
	result["raw_response"] = rawResponse
	result["parsed_response"] = decodeNullable(parsedBody)
	result["error"] = errorValue
	result["metrics"] = decodeDefault(metricsBody, map[string]any{})
	writeJSON(w, 200, result)
}

func (s *Server) modelUsage(w http.ResponseWriter, r *http.Request) {
	filter, ok := auditFilterFromRequest(w, r)
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT model,status,duration_ms,prompt_tokens,completion_tokens,operation,provider FROM model_call_audits WHERE
		($1::timestamptz IS NULL OR started_at >= $1) AND ($2::timestamptz IS NULL OR started_at <= $2)
		AND ($3='' OR model=$3) AND ($4='' OR provider=$4) AND ($5='' OR operation=$5)
		AND ($6='' OR status=$6) AND ($7='' OR input_language=$7 OR output_language=$7) AND ($8='' OR fidelity=$8)`, filter.start, filter.end, filter.model, filter.provider, filter.operation, filter.status, filter.language, filter.fidelity)
	if err != nil {
		writeError(w, 500, "model usage query failed")
		return
	}
	defer rows.Close()
	total := newUsage()
	byModel := map[string]*usage{}
	operations := map[string]struct{}{}
	providers := map[string]struct{}{}
	for rows.Next() {
		var model, status, operation, provider string
		var duration, prompt, completion *int
		if rows.Scan(&model, &status, &duration, &prompt, &completion, &operation, &provider) != nil {
			writeError(w, 500, "model usage row is invalid")
			return
		}
		total.add(status, duration, prompt, completion)
		if byModel[model] == nil {
			byModel[model] = newUsage()
		}
		byModel[model].add(status, duration, prompt, completion)
		operations[operation] = struct{}{}
		providers[provider] = struct{}{}
	}
	models := make([]string, 0, len(byModel))
	for model := range byModel {
		models = append(models, model)
	}
	sort.Strings(models)
	modelItems := make([]map[string]any, 0, len(models))
	for _, model := range models {
		item := byModel[model].result()
		item["model"] = model
		modelItems = append(modelItems, item)
	}
	result := total.result()
	result["models"] = modelItems
	result["operations"] = sortedKeys(operations)
	result["providers"] = sortedKeys(providers)
	writeJSON(w, 200, result)
}

const auditSelect = `SELECT id,logical_call_id,provider,model,operation,entity_type,entity_id,attempt,status,fidelity,started_at,completed_at,duration_ms,prompt_tokens,completion_tokens,input_language,output_language FROM model_call_audits`

type rowScanner interface{ Scan(...any) error }

func scanAudit(row rowScanner) (auditRow, error) {
	var value auditRow
	err := row.Scan(&value.ID, &value.LogicalID, &value.Provider, &value.Model, &value.Operation, &value.EntityType, &value.EntityID, &value.Attempt, &value.Status, &value.Fidelity, &value.Started, &value.Completed, &value.Duration, &value.Prompt, &value.Completion, &value.InputLanguage, &value.OutputLanguage)
	return value, err
}
func auditSummary(row auditRow) map[string]any {
	return map[string]any{"id": row.ID, "logical_call_id": row.LogicalID, "provider": row.Provider, "model": row.Model, "operation": row.Operation, "entity_type": row.EntityType, "entity_id": row.EntityID, "attempt": row.Attempt, "status": row.Status, "fidelity": row.Fidelity, "started_at": iso(row.Started), "completed_at": iso(row.Completed), "duration_ms": row.Duration, "prompt_tokens": row.Prompt, "completion_tokens": row.Completion, "input_language": row.InputLanguage, "output_language": row.OutputLanguage}
}

type auditFilter struct {
	start, end                                             any
	model, provider, operation, status, language, fidelity string
}

func auditFilterFromRequest(w http.ResponseWriter, r *http.Request) (auditFilter, bool) {
	q := r.URL.Query()
	start, ok := optionalTimeQuery(w, r, "start")
	if !ok {
		return auditFilter{}, false
	}
	end, ok := optionalTimeQuery(w, r, "end")
	if !ok {
		return auditFilter{}, false
	}
	return auditFilter{start, end, q.Get("model"), q.Get("provider"), q.Get("operation"), q.Get("status"), q.Get("language"), q.Get("fidelity")}, true
}
func decodeAuditCursor(raw string) (time.Time, string, error) {
	body, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(body), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	stamp, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", err
	}
	if _, err = uuid.Parse(parts[1]); err != nil {
		return time.Time{}, "", err
	}
	return stamp, parts[1], nil
}
func encodeAuditCursor(stamp time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(iso(stamp) + "|" + id))
}
func decodeDefault(body []byte, fallback any) any {
	if len(body) == 0 {
		return fallback
	}
	var value any
	if json.Unmarshal(body, &value) != nil || value == nil {
		return fallback
	}
	return value
}
func decodeNullable(body []byte) any {
	if len(body) == 0 {
		return nil
	}
	var value any
	_ = json.Unmarshal(body, &value)
	return value
}

type usage struct {
	calls, successes, durationCount int
	durationSum, prompt, completion int64
}

func newUsage() *usage { return &usage{} }
func (u *usage) add(status string, duration, prompt, completion *int) {
	u.calls++
	if status == "completed" {
		u.successes++
	}
	if duration != nil {
		u.durationCount++
		u.durationSum += int64(*duration)
	}
	if prompt != nil {
		u.prompt += int64(*prompt)
	}
	if completion != nil {
		u.completion += int64(*completion)
	}
}
func (u *usage) result() map[string]any {
	rate := float64(0)
	if u.calls > 0 {
		rate = float64(u.successes) / float64(u.calls)
	}
	var average any
	if u.durationCount > 0 {
		average = int(math.RoundToEven(float64(u.durationSum) / float64(u.durationCount)))
	}
	return map[string]any{"calls": u.calls, "successes": u.successes, "failures": u.calls - u.successes, "success_rate": rate, "average_duration_ms": average, "prompt_tokens": u.prompt, "completion_tokens": u.completion}
}
func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
