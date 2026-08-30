package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	scanStatusKey              = "market-loop:scan:status"
	newsExtractionQueueKey     = "market-loop:scan:news-extraction-queue"
	modelQueueOverviewCacheKey = "market-loop:model-queue-overview:snapshot:v3"
)

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2500*time.Millisecond)
	defer cancel()
	database := s.db.Ping(ctx) == nil
	redisOK := s.redis.Ping(ctx).Err() == nil

	var latestNews, latestDiscovery *time.Time
	if database {
		_ = s.db.QueryRow(ctx, `SELECT max(observed_at) FROM news_items`).Scan(&latestNews)
		_ = s.db.QueryRow(ctx, `SELECT max(last_success_at) FROM news_source_states`).Scan(&latestDiscovery)
	}
	var freshness *time.Time
	for _, stamp := range []*time.Time{latestNews, latestDiscovery} {
		if stamp != nil && (freshness == nil || stamp.After(*freshness)) {
			copy := stamp.UTC()
			freshness = &copy
		}
	}
	var age any
	dataFresh := false
	if freshness != nil {
		seconds := time.Since(*freshness).Seconds()
		age = seconds
		dataFresh = seconds <= s.cfg.ScanInterval.Seconds()*3
	}

	failures, successes := int64(0), int64(0)
	if redisOK {
		failures, _ = s.redis.Get(ctx, "market-loop:tasks:failure").Int64()
		successes, _ = s.redis.Get(ctx, "market-loop:tasks:success").Int64()
	}
	failureRate := float64(0)
	if total := failures + successes; total > 0 {
		failureRate = float64(failures) / float64(total)
	}
	instances, models := probeOllama(ctx)
	ollama := len(instances) > 0 && boolValue(instances[0]["healthy"])
	status := "degraded"
	if database && redisOK && ollama && dataFresh {
		status = "ok"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": status, "database": database, "redis": redisOK,
		"task_failure_rate": failureRate, "ollama": ollama, "models": models,
		"ollama_instances": instances, "fmp_configured": s.cfg.FMPAccessToken != "",
		"fmp_mcp_configured": os.Getenv("FMP_MCP_URL") != "",
		"latest_news_at":     timeOrNil(latestNews), "latest_news_discovery_at": timeOrNil(latestDiscovery),
		"news_age_seconds": age, "data_fresh": dataFresh,
		"telegram_configured": os.Getenv("TELEGRAM_BOT_TOKEN") != "" && os.Getenv("TELEGRAM_CHAT_ID") != "",
		"evolution_enabled":   s.cfg.EvolutionEnabled, "evolution_auto_merge": s.cfg.EvolutionAutoMerge,
		"as_of": time.Now().UTC(),
	})
}

func probeOllama(ctx context.Context) ([]map[string]any, []string) {
	type lane struct{ name, model string }
	lanes := []lane{{"extract", envValue("OLLAMA_EXTRACT_MODEL", "qwen2.5:3b")}, {"assist", envValue("OLLAMA_ASSIST_MODEL", "qwen2.5:7b")}, {"research", envValue("OLLAMA_RESEARCH_MODEL", "qwen2.5:7b")}, {"code", envValue("OLLAMA_CODE_MODEL", "qwen2.5-coder:7b")}}
	client := &http.Client{Timeout: 2 * time.Second}
	seenURLs := map[string]struct{}{}
	seenModels := map[string]struct{}{}
	instances := make([]map[string]any, 0)
	for _, item := range lanes {
		urls := splitEnvURLs("OLLAMA_" + strings.ToUpper(item.name) + "_BASE_URLS")
		if len(urls) == 0 {
			urls = splitEnvURLs("OLLAMA_" + strings.ToUpper(item.name) + "_BASE_URL")
		}
		for index, base := range urls {
			if _, ok := seenURLs[base]; ok {
				continue
			}
			seenURLs[base] = struct{}{}
			available := fetchOllamaModels(ctx, client, strings.TrimRight(base, "/"))
			for _, model := range available {
				seenModels[model] = struct{}{}
			}
			instances = append(instances, map[string]any{
				"id": fmt.Sprintf("%s-%d", item.name, index+1), "healthy": available != nil,
				"model_available": contains(available, item.model), "model_loaded": false,
			})
		}
	}
	models := make([]string, 0, len(seenModels))
	for model := range seenModels {
		models = append(models, model)
	}
	sort.Strings(models)
	return instances, models
}

func fetchOllamaModels(ctx context.Context, client *http.Client, base string) []string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/tags", nil)
	if err != nil {
		return nil
	}
	response, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return nil
	}
	var payload struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if json.NewDecoder(response.Body).Decode(&payload) != nil {
		return nil
	}
	result := make([]string, 0, len(payload.Models))
	for _, model := range payload.Models {
		result = append(result, model.Name)
	}
	return result
}

func (s *Server) scanStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.scanStatusPayload(r.Context()))
}

func (s *Server) scanStatusPayload(ctx context.Context) map[string]any {
	payload := map[string]any{
		"state": "idle", "task_id": nil, "phase": nil, "paused_from_phase": nil,
		"current": 0, "total": 0, "started_at": nil, "heartbeat_at": nil,
		"last_completed_at": nil, "next_scan_at": nil, "last_result": nil, "last_error": nil,
	}
	raw, err := s.redis.Get(ctx, scanStatusKey).Bytes()
	if err == nil {
		var stored map[string]any
		if json.Unmarshal(raw, &stored) == nil {
			for key, value := range stored {
				payload[key] = value
			}
		}
	} else if !errors.Is(err, context.Canceled) && err.Error() != "redis: nil" {
		payload["state"] = "failed"
		payload["last_error"] = "scan state unavailable"
	}
	payload["interval_seconds"] = int(s.cfg.ScanInterval.Seconds())
	payload["server_time"] = time.Now().UTC().Format(time.RFC3339Nano)
	return payload
}

func (s *Server) newsExtractionQueue(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 200, 1, 200)
	if !ok {
		return
	}
	now := time.Now().UTC()
	payload := map[string]any{"model": s.cfg.ExtractModel, "scan_task_id": nil, "state": "idle", "total_items": 0, "items": []any{}, "error": nil}
	if raw, err := s.redis.Get(r.Context(), newsExtractionQueueKey).Bytes(); err == nil {
		var stored map[string]any
		if json.Unmarshal(raw, &stored) == nil {
			for key, value := range stored {
				payload[key] = value
			}
		}
	} else if err.Error() != "redis: nil" {
		payload["state"], payload["error"] = "unavailable", "queue state unavailable"
	}
	if queueID := stringValue(payload["scan_task_id"]); queueID != "" {
		if raw, err := s.redis.Get(r.Context(), scanStatusKey).Bytes(); err == nil {
			var scan map[string]any
			if json.Unmarshal(raw, &scan) == nil && stringValue(scan["task_id"]) != "" && stringValue(scan["task_id"]) != queueID {
				payload = map[string]any{"model": s.cfg.ExtractModel, "scan_task_id": nil, "state": "idle", "total_items": 0, "items": []any{}, "error": nil}
			}
		}
	}
	items, _ := payload["items"].([]any)
	counts := map[string]int{"queued": 0, "running": 0, "retrying": 0, "completed": 0, "failed": 0}
	visible := make([]map[string]any, 0)
	queueDurations, executionDurations := make([]int64, 0), make([]int64, 0)
	rank := map[string]int{"running": 0, "retrying": 1, "queued": 2, "failed": 3}
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		status := stringValue(item["status"])
		if _, ok := counts[status]; ok {
			counts[status]++
		}
		queueDuration := durationBetween(item["queued_at"], item["started_at"])
		if queueDuration == nil && status == "queued" {
			queueDuration = durationUntil(item["queued_at"], now)
		}
		executionDuration := int64Value(item["execution_duration_ms"])
		if active := durationUntil(item["attempt_started_at"], now); active != nil {
			executionDuration += *active
		}
		copy := cloneMap(item)
		copy["queue_duration_ms"] = queueDuration
		if item["started_at"] == nil && executionDuration == 0 {
			copy["execution_duration_ms"] = nil
		} else {
			copy["execution_duration_ms"] = executionDuration
		}
		delete(copy, "attempt_started_at")
		if queueDuration != nil {
			queueDurations = append(queueDurations, *queueDuration)
		}
		if (status == "completed" || status == "failed") && copy["execution_duration_ms"] != nil {
			executionDurations = append(executionDurations, executionDuration)
		}
		if _, ok := rank[status]; ok {
			visible = append(visible, copy)
		}
	}
	sort.SliceStable(visible, func(i, j int) bool {
		left, right := rank[stringValue(visible[i]["status"])], rank[stringValue(visible[j]["status"])]
		if left != right {
			return left < right
		}
		return fmt.Sprint(visible[i]["queued_at"]) < fmt.Sprint(visible[j]["queued_at"])
	})
	truncated := len(visible) > limit
	if truncated {
		visible = visible[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": now, "model": defaultAny(payload["model"], s.cfg.ExtractModel),
		"scan_task_id": payload["scan_task_id"], "state": defaultAny(payload["state"], "idle"),
		"total_items": int64Value(payload["total_items"]), "counts": counts,
		"average_queue_duration_ms": averageInt64(queueDurations), "average_execution_duration_ms": averageInt64(executionDurations),
		"queue_duration_sample_count": len(queueDurations), "execution_duration_sample_count": len(executionDurations),
		"truncated": truncated, "items": visible, "error": payload["error"],
	})
}

func (s *Server) researchQueue(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 500, 1, 1000)
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT payload::jsonb FROM research_runs WHERE status IN ('queued','running','verifying')`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "research queue query failed")
		return
	}
	defer rows.Close()
	now := time.Now().UTC()
	counts := map[string]int{"queued": 0, "running": 0, "verifying": 0}
	grouped := map[string]map[string]any{}
	queueDurations, executionDurations := make([]int64, 0), make([]int64, 0)
	totalRuns := 0
	priority := map[string]int{"queued": 1, "running": 2, "verifying": 3}
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			continue
		}
		var run map[string]any
		if json.Unmarshal(body, &run) != nil {
			continue
		}
		status := stringValue(run["status"])
		if _, ok := counts[status]; !ok {
			continue
		}
		asset, _ := run["asset"].(map[string]any)
		assetID := stringValue(asset["asset_id"])
		if assetID == "" {
			continue
		}
		counts[status]++
		totalRuns++
		created := parseAnyTime(run["created_at"])
		started := parseAnyTime(run["started_at"])
		completed := parseAnyTime(run["completed_at"])
		updated := parseAnyTime(run["updated_at"])
		queueDuration := millisBetween(created, started)
		if queueDuration == nil && status == "queued" {
			queueDuration = millisBetween(created, &now)
		}
		executionDuration := millisBetween(started, completed)
		if executionDuration == nil && started != nil {
			executionDuration = millisBetween(started, &now)
		}
		if queueDuration != nil {
			queueDurations = append(queueDurations, *queueDuration)
		}
		if executionDuration != nil {
			executionDurations = append(executionDurations, *executionDuration)
		}
		taskCount := 1
		if triggers, ok := run["trigger_event_ids"].([]any); ok && len(triggers) > 0 {
			taskCount = len(triggers)
		}
		item := grouped[assetID]
		if item == nil {
			item = map[string]any{
				"asset_id": assetID, "symbol": asset["symbol"], "name": asset["name"], "market": asset["market"], "asset_class": asset["asset_class"],
				"status": status, "task_count": taskCount, "queued_at": timeOrNil(created), "representative_queued_at": timeOrNil(created),
				"started_at": timeOrNil(started), "completed_at": timeOrNil(completed), "queue_duration_ms": queueDuration,
				"execution_duration_ms": executionDuration, "updated_at": timeOrNil(updated),
			}
			grouped[assetID] = item
			continue
		}
		item["task_count"] = int64Value(item["task_count"]) + int64(taskCount)
		if created != nil && (parseAnyTime(item["queued_at"]) == nil || created.Before(*parseAnyTime(item["queued_at"]))) {
			item["queued_at"] = created
		}
		oldUpdated := parseAnyTime(item["updated_at"])
		if priority[status] > priority[stringValue(item["status"])] || (priority[status] == priority[stringValue(item["status"])] && updated != nil && (oldUpdated == nil || updated.After(*oldUpdated))) {
			item["status"], item["representative_queued_at"], item["started_at"] = status, timeOrNil(created), timeOrNil(started)
			item["completed_at"], item["queue_duration_ms"], item["execution_duration_ms"] = timeOrNil(completed), queueDuration, executionDuration
		}
		if updated != nil && (oldUpdated == nil || updated.After(*oldUpdated)) {
			item["updated_at"] = updated
		}
	}
	items := make([]map[string]any, 0, len(grouped))
	for _, item := range grouped {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		li, lj := stringValue(items[i]["status"]), stringValue(items[j]["status"])
		if li == "queued" && lj != "queued" {
			return true
		}
		if li != "queued" && lj == "queued" {
			return false
		}
		return fmt.Sprint(items[i]["updated_at"]) > fmt.Sprint(items[j]["updated_at"])
	})
	truncated := len(items) > limit
	if truncated {
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": now, "model": s.cfg.ResearchModel, "total_assets": len(grouped), "total_runs": totalRuns,
		"counts": counts, "average_queue_duration_ms": averageInt64(queueDurations), "average_execution_duration_ms": averageInt64(executionDurations),
		"queue_duration_sample_count": len(queueDurations), "execution_duration_sample_count": len(executionDurations),
		"truncated": truncated, "items": items,
	})
}

func (s *Server) taskStatus(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if id, err := uuid.Parse(taskID); err == nil {
		var status string
		var result []byte
		var jobError *string
		err := s.db.QueryRow(r.Context(), `SELECT status,result,error FROM go_jobs WHERE id=$1`, id).Scan(&status, &result, &jobError)
		if err == nil {
			payload := map[string]any{"task_id": taskID, "state": strings.ToUpper(status)}
			if len(result) > 0 {
				payload["result"] = decodeDefault(result, map[string]any{})
			}
			if jobError != nil {
				payload["error"] = *jobError
			}
			writeJSON(w, http.StatusOK, payload)
			return
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, "task status query failed")
			return
		}
	}
	raw, err := s.redis.Get(r.Context(), "celery-task-meta-"+taskID).Bytes()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "state": "PENDING"})
		return
	}
	var stored map[string]any
	if json.Unmarshal(raw, &stored) != nil {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "state": "PENDING"})
		return
	}
	state := defaultValue(stringValue(stored["status"]), "PENDING")
	payload := map[string]any{"task_id": taskID, "state": state}
	if state == "SUCCESS" {
		payload["result"] = stored["result"]
	}
	if state == "FAILURE" {
		payload["error"] = stringValue(stored["exc_type"])
	}
	if state == "PROGRESS" {
		payload["progress"] = stored["result"]
	}
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) modelInferenceQueues(w http.ResponseWriter, r *http.Request) {
	items := []map[string]any{
		s.inferenceQueue(r.Context(), "assist", s.cfg.AssistModel, "股票映射", "新闻事件二次股票映射", true),
		s.inferenceQueue(r.Context(), "code", s.cfg.CodeModel, "代码演进", evolutionBinding(s.cfg.EvolutionAutoMerge), s.cfg.EvolutionEnabled),
	}
	writeJSON(w, http.StatusOK, map[string]any{"generated_at": time.Now().UTC(), "items": items})
}

func (s *Server) inferenceQueue(ctx context.Context, lane, model, purpose, binding string, enabled bool) map[string]any {
	capacity := envIntValue("OLLAMA_"+strings.ToUpper(lane)+"_MAX_CONCURRENCY", 1)
	urls := modelURLs(lane)
	instanceCount := len(urls)
	base, extra := capacity, 0
	if instanceCount > 0 {
		base, extra = capacity/instanceCount, capacity%instanceCount
	}
	instances := make([]map[string]any, 0, instanceCount)
	queued, running, available := int64(0), 0, 0
	observable := true
	slotOffset := 0
	client := &http.Client{Timeout: 2 * time.Second}
	for index, endpoint := range urls {
		instanceCapacity := base
		if index < extra {
			instanceCapacity++
		}
		instanceID := fmt.Sprintf("%s-%d", lane, index)
		waiting, waitErr := s.redis.ZCard(ctx, fmt.Sprintf("market-loop:llm:%s:%s:waiting", lane, instanceID)).Result()
		instanceRunning := 0
		for slot := slotOffset; slot < slotOffset+instanceCapacity; slot++ {
			if count, probeErr := s.redis.Exists(ctx, fmt.Sprintf("market-loop:llm:%s:%d", lane, slot)).Result(); probeErr == nil && count > 0 {
				instanceRunning++
			}
		}
		slotOffset += instanceCapacity
		models := fetchOllamaModels(ctx, client, endpoint)
		healthy := models != nil
		modelAvailable := healthy && contains(models, model)
		instanceObservable := waitErr == nil
		instanceAvailable := 0
		if healthy && modelAvailable {
			instanceAvailable = max(0, instanceCapacity-instanceRunning)
		}
		instances = append(instances, map[string]any{"id": instanceID, "healthy": healthy, "model_available": modelAvailable,
			"capacity": instanceCapacity, "available": instanceAvailable, "queued": waiting, "running": instanceRunning, "observable": instanceObservable})
		queued += waiting
		running += instanceRunning
		available += instanceAvailable
		observable = observable && instanceObservable
	}
	state := "idle"
	if !observable {
		state = "unavailable"
	} else if queued > 0 {
		state = "queued"
	} else if running > 0 {
		state = "running"
	}
	return map[string]any{"lane": lane, "model": model, "purpose": purpose, "binding": binding, "task_enabled": enabled,
		"capacity": capacity, "queued": queued, "running": running, "available": available, "observable": observable,
		"instances": instances, "instance_count": instanceCount, "per_instance_concurrency": ceilDiv(capacity, instanceCount),
		"threads": modelThreads(lane), "state": state}
}

func (s *Server) modelQueueOverview(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 500, 1, 500)
	if !ok {
		return
	}
	if raw, err := s.redis.Get(r.Context(), modelQueueOverviewCacheKey).Bytes(); err == nil {
		var payload map[string]any
		if json.Unmarshal(raw, &payload) == nil {
			truncateQueueSnapshot(payload, limit)
			writeJSON(w, http.StatusOK, payload)
			return
		}
	}
	queues := make([]map[string]any, 0, 4)
	for _, spec := range []struct {
		lane, model, purpose, binding string
		enabled                       bool
	}{
		{"extract", s.cfg.ExtractModel, "新闻抽取", "新闻事件结构化抽取", true},
		{"research", s.cfg.ResearchModel, "标的研究", "工具深度研究与逐目标事件研报", true},
		{"assist", s.cfg.AssistModel, "股票映射", "新闻事件二次股票映射", true},
		{"code", s.cfg.CodeModel, "代码演进", evolutionBinding(s.cfg.EvolutionAutoMerge), s.cfg.EvolutionEnabled},
	} {
		status := s.inferenceQueue(r.Context(), spec.lane, spec.model, spec.purpose, spec.binding, spec.enabled)
		counts := map[string]int{"queued": int(int64Value(status["queued"])), "running": int(int64Value(status["running"])), "retrying": 0, "verifying": 0, "waiting_for_model": 0, "completed": 0, "failed": 0}
		queues = append(queues, map[string]any{
			"id": spec.lane, "model": spec.model, "purpose": spec.purpose, "binding": spec.binding, "enabled": spec.enabled,
			"state": status["state"], "threads": status["threads"], "capacity": status["capacity"], "available": status["available"],
			"instance_count": 0, "per_instance_concurrency": status["capacity"], "observable": status["observable"], "instances": []any{},
			"counts": counts, "metrics": emptyQueueMetrics(), "total_tasks": counts["queued"] + counts["running"], "truncated": false, "tasks": []any{}, "error": nil,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"generated_at": time.Now().UTC(), "queues": queues})
}

func truncateQueueSnapshot(payload map[string]any, limit int) {
	queues, _ := payload["queues"].([]any)
	for _, raw := range queues {
		queue, _ := raw.(map[string]any)
		tasks, _ := queue["tasks"].([]any)
		queue["truncated"] = len(tasks) > limit
		if len(tasks) > limit {
			queue["tasks"] = tasks[:limit]
		}
	}
}

func modelURLs(lane string) []string {
	prefix := "OLLAMA_" + strings.ToUpper(lane)
	if urls := splitEnvURLs(prefix + "_BASE_URLS"); len(urls) > 0 {
		return urls
	}
	fallback := strings.TrimSpace(os.Getenv(prefix + "_BASE_URL"))
	if fallback == "" {
		fallback = envValue("OLLAMA_BASE_URL", "http://localhost:11434")
	}
	return []string{strings.TrimRight(fallback, "/")}
}

func modelThreads(lane string) int {
	fallback := envIntValue("OLLAMA_NUM_THREADS", 0)
	return envIntValue("OLLAMA_"+strings.ToUpper(lane)+"_NUM_THREADS", fallback)
}

func ceilDiv(value, divisor int) int {
	if divisor <= 0 || value <= 0 {
		return 0
	}
	return (value + divisor - 1) / divisor
}

func emptyQueueMetrics() map[string]any {
	return map[string]any{"average_queue_duration_ms": nil, "average_execution_duration_ms": nil, "longest_wait_ms": nil,
		"estimated_clear_ms": nil, "queue_duration_sample_count": 0, "execution_duration_sample_count": 0,
		"execution_p50_ms": nil, "execution_p90_ms": nil, "throughput_per_hour": nil}
}

func evolutionBinding(auto bool) string {
	if auto {
		return "代码演进任务 · 自动合并开启"
	}
	return "代码演进任务 · 自动合并关闭"
}

func parseAnyTime(value any) *time.Time {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case time.Time:
		result := typed.UTC()
		return &result
	case *time.Time:
		if typed == nil {
			return nil
		}
		result := typed.UTC()
		return &result
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999-07:00", "2006-01-02T15:04:05"} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				parsed = parsed.UTC()
				return &parsed
			}
		}
	}
	return nil
}

func millisBetween(start, end *time.Time) *int64 {
	if start == nil || end == nil {
		return nil
	}
	value := end.Sub(*start).Milliseconds()
	if value < 0 {
		value = 0
	}
	return &value
}
func durationBetween(start, end any) *int64 {
	return millisBetween(parseAnyTime(start), parseAnyTime(end))
}
func durationUntil(start any, end time.Time) *int64 { return millisBetween(parseAnyTime(start), &end) }
func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		result, _ := typed.Int64()
		return result
	case string:
		result, _ := strconv.ParseInt(typed, 10, 64)
		return result
	default:
		return 0
	}
}
func averageInt64(values []int64) any {
	if len(values) == 0 {
		return nil
	}
	var total int64
	for _, value := range values {
		total += value
	}
	return total / int64(len(values))
}
func cloneMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
func timeOrNil(value *time.Time) any {
	if value == nil {
		return nil
	}
	return jsonTime(*value)
}
func jsonTimeOrNil(value *time.Time) any {
	if value == nil {
		return nil
	}
	return jsonTime(*value)
}
func isoTimeOrNil(value *time.Time) any {
	if value == nil {
		return nil
	}
	return iso(*value)
}
func nullableStringPointer(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
func envValue(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
func envIntValue(name string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil {
		return fallback
	}
	return value
}
func splitEnvURLs(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
