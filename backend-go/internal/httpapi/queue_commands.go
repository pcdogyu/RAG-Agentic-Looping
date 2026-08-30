package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

var modelQueueNames = map[string]string{"extract": "extract", "research": "research", "assist": "mapping", "code": "evolution"}

type cancellationInput struct {
	TaskID   string  `json:"task_id"`
	Kind     string  `json:"kind"`
	EntityID *string `json:"entity_id"`
}

type modelRetryInput struct {
	TaskID     string  `json:"task_id"`
	Kind       string  `json:"kind"`
	EntityID   *string `json:"entity_id"`
	InstanceID *string `json:"instance_id"`
}

type cancelSummary struct {
	Cancelled int
	TaskIDs   []string
}

func (s *Server) cancelResearchTask(w http.ResponseWriter, r *http.Request) {
	var input cancellationInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.Kind != "asset_research" && input.Kind != "event_research" {
		writeError(w, http.StatusUnprocessableEntity, "only research tasks can be cancelled")
		return
	}
	entity := stringPointerValue(input.EntityID)
	result, err := s.cancelResearch(r.Context(), input.Kind, entity, input.TaskID, false, "")
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	if result["cancelled"].(int) == 0 {
		writeError(w, http.StatusNotFound, "active research task not found")
		return
	}
	result["revoked"] = 0
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) cancelAssetMappingTask(w http.ResponseWriter, r *http.Request) {
	var input cancellationInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.Kind != "asset_mapping" {
		writeError(w, http.StatusUnprocessableEntity, "only asset mapping tasks can be cancelled")
		return
	}
	if !s.cancelTrackedTask(r.Context(), "assist", input.TaskID) {
		writeError(w, http.StatusNotFound, "active asset mapping task not found")
		return
	}
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusAccepted, map[string]any{"queue_id": "assist", "cancelled": 1, "revoked": 0})
}

func (s *Server) clearResearchTasks(w http.ResponseWriter, r *http.Request) {
	result, err := s.cancelResearch(r.Context(), "", "", "", true, "")
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	result["purged"] = s.purgeResearchQueues(r.Context(), "")
	result["revoked"] = 0
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) clearModelQueue(w http.ResponseWriter, r *http.Request) {
	queueID := chi.URLParam(r, "queueID")
	queueName, ok := modelQueueNames[queueID]
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "unknown model queue")
		return
	}
	if queueID == "research" {
		s.clearResearchTasks(w, r)
		return
	}
	tracked := s.cancelTrackedTasks(r.Context(), queueID, true, "")
	cancelled := tracked.Cancelled
	if queueID == "extract" {
		cancelled += s.clearExtractionQueue(r.Context(), "").Cancelled
	}
	purged := s.purgeCeleryQueue(r.Context(), queueName)
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusAccepted, map[string]any{"queue_id": queueID, "cancelled": cancelled, "purged": purged, "revoked": 0})
}

func (s *Server) clearModelInstanceQueue(w http.ResponseWriter, r *http.Request) {
	queueID, instanceID := chi.URLParam(r, "queueID"), chi.URLParam(r, "instanceID")
	lane, ok := map[string]string{"extract": "extract", "research": "research", "assist": "assist", "code": "code"}[queueID]
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "unknown model queue")
		return
	}
	if _, valid := instanceIndex(lane, instanceID, len(modelURLs(lane))); !valid {
		writeError(w, http.StatusNotFound, "model instance queue not found")
		return
	}
	cancelled := 0
	if queueID == "research" {
		result, err := s.cancelResearch(r.Context(), "", "", "", true, instanceID)
		if err != nil {
			writeAPIFailure(w, err)
			return
		}
		cancelled = result["cancelled"].(int)
	} else {
		cancelled = s.cancelTrackedTasks(r.Context(), lane, true, instanceID).Cancelled
		if queueID == "extract" {
			cancelled += s.clearExtractionQueue(r.Context(), instanceID).Cancelled
		}
	}
	purged := s.purgeCeleryQueue(r.Context(), modelQueueNames[queueID]+"."+instanceID)
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusAccepted, map[string]any{"queue_id": queueID, "instance_id": instanceID, "cancelled": cancelled, "purged": purged, "revoked": 0})
}

func (s *Server) retryModelQueueTask(w http.ResponseWriter, r *http.Request) {
	queueID := chi.URLParam(r, "queueID")
	if _, ok := modelQueueNames[queueID]; !ok {
		writeError(w, http.StatusUnprocessableEntity, "this model queue does not support manual retry")
		return
	}
	var input modelRetryInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	tasks, _, err := s.retryableModelTasks(r.Context(), queueID, stringPointerValue(input.InstanceID))
	if err != nil {
		writeError(w, 500, "model queue state unavailable")
		return
	}
	var selected map[string]any
	for _, task := range tasks {
		if stringValue(task["task_id"]) == input.TaskID && stringValue(task["kind"]) == input.Kind && stringValue(task["entity_id"]) == stringPointerValue(input.EntityID) {
			selected = task
			break
		}
	}
	if selected == nil {
		writeError(w, http.StatusNotFound, "retryable model task not found")
		return
	}
	taskID, retryErr := s.retryModelTask(r.Context(), queueID, selected, 0)
	if retryErr != nil {
		writeAPIFailure(w, retryErr)
		return
	}
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusAccepted, map[string]any{"queue_id": queueID, "requested": 1, "retried": 1, "skipped": 0, "task_ids": []string{taskID}, "priority": "highest"})
}

func (s *Server) retryModelQueueTasks(w http.ResponseWriter, r *http.Request) {
	s.retryModelTasks(w, r, "")
}

func (s *Server) retryModelInstanceTasks(w http.ResponseWriter, r *http.Request) {
	s.retryModelTasks(w, r, chi.URLParam(r, "instanceID"))
}

func (s *Server) retryModelTasks(w http.ResponseWriter, r *http.Request, instanceID string) {
	queueID := chi.URLParam(r, "queueID")
	if _, ok := modelQueueNames[queueID]; !ok {
		writeError(w, http.StatusUnprocessableEntity, "this model queue does not support bulk retry")
		return
	}
	tasks, instanceFound, err := s.retryableModelTasks(r.Context(), queueID, instanceID)
	if err != nil {
		writeError(w, 500, "model queue state unavailable")
		return
	}
	if instanceID != "" && !instanceFound {
		writeError(w, http.StatusNotFound, "model instance queue not found")
		return
	}
	ids := make([]string, 0, len(tasks))
	skipped := 0
	for _, task := range tasks {
		id, retryErr := s.retryModelTask(r.Context(), queueID, task, 5)
		if retryErr != nil {
			skipped++
			continue
		}
		ids = append(ids, id)
	}
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	payload := map[string]any{"queue_id": queueID, "requested": len(tasks), "retried": len(ids), "skipped": skipped, "task_ids": ids, "priority": "normal"}
	if instanceID != "" {
		payload["instance_id"] = instanceID
	}
	writeJSON(w, http.StatusAccepted, payload)
}

func (s *Server) retryableModelTasks(ctx context.Context, queueID, instanceID string) ([]map[string]any, bool, error) {
	raw, err := s.redis.Get(ctx, modelQueueOverviewCacheKey).Bytes()
	if err != nil {
		return []map[string]any{}, instanceID == "", nil
	}
	var snapshot map[string]any
	if json.Unmarshal(raw, &snapshot) != nil {
		return nil, false, errors.New("invalid queue snapshot")
	}
	for _, queueRaw := range anySlice(snapshot["queues"]) {
		queue, _ := queueRaw.(map[string]any)
		if stringValue(queue["id"]) != queueID {
			continue
		}
		taskValues := anySlice(queue["tasks"])
		instanceFound := instanceID == ""
		if instanceID != "" {
			for _, instanceRaw := range anySlice(queue["instances"]) {
				instance, _ := instanceRaw.(map[string]any)
				if stringValue(instance["id"]) == instanceID {
					taskValues, instanceFound = anySlice(instance["tasks"]), true
					break
				}
			}
		}
		result := make([]map[string]any, 0)
		for _, taskRaw := range taskValues {
			task, _ := taskRaw.(map[string]any)
			if task != nil && task["error"] != nil && stringValue(task["error"]) != "" {
				result = append(result, task)
			}
		}
		return result, instanceFound, nil
	}
	return []map[string]any{}, false, nil
}

func (s *Server) retryModelTask(ctx context.Context, queueID string, task map[string]any, priority int) (string, error) {
	kind, entityID, oldTaskID := stringValue(task["kind"]), stringValue(task["entity_id"]), stringValue(task["task_id"])
	preferred := stringValue(task["instance_id"])
	switch queueID {
	case "extract":
		if entityID == "" {
			return "", fail(http.StatusConflict, "news extraction task has no durable news id")
		}
		var title, source string
		if err := s.db.QueryRow(ctx, `SELECT title,source FROM news_items WHERE id=$1`, entityID).Scan(&title, &source); err != nil {
			return "", fail(http.StatusConflict, "source news no longer exists")
		}
		return s.queueNewsRetryWithOptions(ctx, entityID, title, source, preferred, priority)
	case "assist":
		if kind != "asset_mapping" || entityID == "" {
			return "", fail(http.StatusUnprocessableEntity, "only asset mapping tasks can be retried")
		}
		var headline string
		if err := s.db.QueryRow(ctx, `SELECT headline FROM news_events WHERE id=$1`, entityID).Scan(&headline); err != nil {
			return "", fail(http.StatusConflict, "source event no longer exists")
		}
		instanceID, err := s.selectModelInstance(ctx, "assist", preferred)
		if err != nil {
			return "", fail(http.StatusConflict, err.Error())
		}
		taskID := uuid.NewString()
		s.trackModelTask(ctx, "assist", taskID, "asset_mapping", entityID, headline, "股票映射", "manual", instanceID)
		if err = s.publishCelery(ctx, "market_loop.resolve_event_assets", "mapping."+instanceID, taskID, []any{entityID}, map[string]any{"force": true, "model_instance_id": instanceID}, priority); err != nil {
			return "", fail(http.StatusServiceUnavailable, "asset mapping retry could not be queued")
		}
		_ = s.cancelTrackedTask(ctx, "assist", oldTaskID)
		return taskID, nil
	case "code":
		instanceID, err := s.selectModelInstance(ctx, "code", preferred)
		if err != nil {
			return "", fail(http.StatusConflict, err.Error())
		}
		taskID := uuid.NewString()
		taskName, args := "market_loop.evolve_from_outcomes", []any{}
		if entityID != "" {
			var exists bool
			_ = s.db.QueryRow(ctx, `SELECT exists(SELECT 1 FROM evolution_candidates WHERE id=$1)`, entityID).Scan(&exists)
			if !exists {
				return "", fail(http.StatusConflict, "evolution candidate no longer exists")
			}
			taskName, args = "market_loop.execute_evolution", []any{entityID}
		}
		s.trackModelTask(ctx, "code", taskID, "code_evolution", entityID, stringValue(task["title"]), stringValue(task["subtitle"]), "manual", instanceID)
		if err = s.publishCelery(ctx, taskName, "evolution."+instanceID, taskID, args, map[string]any{"model_instance_id": instanceID}, priority); err != nil {
			return "", fail(http.StatusServiceUnavailable, "code evolution retry could not be queued")
		}
		_ = s.cancelTrackedTask(ctx, "code", oldTaskID)
		return taskID, nil
	case "research":
		if kind == "asset_research" {
			queued, err := s.retryAssetResearch(ctx, oldTaskID, preferred)
			return queued.TaskID, err
		}
		if kind == "event_research" {
			queued, err := s.retryEventResearch(ctx, oldTaskID, preferred)
			if err != nil {
				return "", err
			}
			return stringValue(queued["task_id"]), nil
		}
	}
	return "", fail(http.StatusUnprocessableEntity, "only research tasks can be retried in this queue")
}

func (s *Server) queueNewsRetryWithOptions(ctx context.Context, newsID, title, source, preferred string, priority int) (string, error) {
	outboxID := uuid.NewString()
	if _, err := s.db.Exec(ctx, `INSERT INTO news_processing(
		news_id,status,celery_task_id,attempt_count,last_error,queued_at,started_at,completed_at,heartbeat_at,created_at,updated_at
	) VALUES($1,'dispatch_pending',NULL,0,NULL,NULL,NULL,NULL,now(),now(),now())
	ON CONFLICT(news_id) DO UPDATE SET status='dispatch_pending',scan_task_id=NULL,celery_task_id=NULL,last_error=NULL,
		queued_at=NULL,started_at=NULL,completed_at=NULL,heartbeat_at=now(),updated_at=now()`, newsID); err != nil {
		return "", err
	}
	if _, err := s.db.Exec(ctx, `INSERT INTO news_processing_outbox(
		id,news_id,status,force_asset_mapping,dispatch_attempts,available_at,dispatched_at,last_error,created_at,updated_at
	) VALUES($1,$2,'pending',true,0,now(),NULL,NULL,now(),now())
	ON CONFLICT(news_id) DO UPDATE SET status='pending',force_asset_mapping=true,available_at=now(),dispatched_at=NULL,last_error=NULL,updated_at=now()`, outboxID, newsID); err != nil {
		return "", err
	}
	instanceID, err := s.selectModelInstance(ctx, "extract", preferred)
	if err != nil {
		_, _ = s.db.Exec(ctx, `UPDATE news_processing SET last_error=$2,updated_at=now() WHERE news_id=$1`, newsID, err.Error())
		return "", fail(http.StatusServiceUnavailable, err.Error())
	}
	taskID := uuid.NewString()
	s.trackModelTask(ctx, "extract", taskID, "news_extraction", newsID, title, source, "manual", instanceID)
	if err = s.publishCelery(ctx, "market_loop.retry_news_item", "extract."+instanceID, taskID, []any{newsID}, map[string]any{"model_instance_id": instanceID, "force_asset_mapping": true}, priority); err != nil {
		_, _ = s.db.Exec(ctx, `UPDATE news_processing_outbox SET dispatch_attempts=dispatch_attempts+1,last_error=$2,available_at=now()+interval '60 seconds',updated_at=now() WHERE news_id=$1`, newsID, err.Error())
		_, _ = s.db.Exec(ctx, `UPDATE news_processing SET status='dispatch_pending',last_error=$2,updated_at=now() WHERE news_id=$1`, newsID, err.Error())
		return "", fail(http.StatusServiceUnavailable, "news retry could not be dispatched")
	}
	_, err = s.db.Exec(ctx, `UPDATE news_processing SET status='queued',celery_task_id=$2,attempt_count=attempt_count+1,
		last_error=NULL,queued_at=now(),heartbeat_at=now(),updated_at=now() WHERE news_id=$1`, newsID, taskID)
	if err == nil {
		_, err = s.db.Exec(ctx, `UPDATE news_processing_outbox SET status='dispatched',dispatch_attempts=dispatch_attempts+1,
			dispatched_at=now(),last_error=NULL,updated_at=now() WHERE news_id=$1`, newsID)
	}
	if err != nil {
		return "", err
	}
	return taskID, nil
}

func (s *Server) cancelTrackedTask(ctx context.Context, lane, taskID string) bool {
	key := "market-loop:model-queue:" + lane + ":tasks"
	raw, err := s.redis.HGet(ctx, key, taskID).Bytes()
	if err != nil {
		return false
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil || !activeModelStatus(stringValue(payload["status"])) {
		return false
	}
	now := iso(time.Now())
	payload["status"], payload["updated_at"], payload["completed_at"], payload["error"] = "cancelled", now, now, nil
	body, _ := json.Marshal(payload)
	_ = s.redis.HSet(ctx, key, taskID, body).Err()
	s.updateInstanceAssignment(ctx, lane, taskID, "cancelled", stringValue(payload["instance_id"]))
	return true
}

func (s *Server) cancelTrackedTasks(ctx context.Context, lane string, includeFailed bool, instanceID string) cancelSummary {
	key := "market-loop:model-queue:" + lane + ":tasks"
	values, err := s.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return cancelSummary{}
	}
	result := cancelSummary{TaskIDs: []string{}}
	now := time.Now().UTC()
	for taskID, raw := range values {
		var payload map[string]any
		if json.Unmarshal([]byte(raw), &payload) != nil {
			continue
		}
		status := stringValue(payload["status"])
		clearable := activeModelStatus(status) || includeFailed && failedModelStatus(status) && recentWithin(payload["updated_at"], now, 24*time.Hour)
		if !clearable || instanceID != "" && stringValue(payload["instance_id"]) != instanceID {
			continue
		}
		if activeModelStatus(status) {
			result.TaskIDs = append(result.TaskIDs, taskID)
		}
		stamp := iso(now)
		payload["status"], payload["updated_at"], payload["completed_at"], payload["error"] = "cancelled", stamp, stamp, nil
		body, _ := json.Marshal(payload)
		_ = s.redis.HSet(ctx, key, taskID, body).Err()
		s.updateInstanceAssignment(ctx, lane, taskID, "cancelled", stringValue(payload["instance_id"]))
		result.Cancelled++
	}
	return result
}

func (s *Server) updateInstanceAssignment(ctx context.Context, lane, taskID, status, instanceID string) {
	key := "market-loop:model-instance:" + lane + ":assignments"
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "instance_id": nullableString(instanceID), "status": status, "updated_at": iso(time.Now())})
	_ = s.redis.HSet(ctx, key, taskID, payload).Err()
}

func (s *Server) clearExtractionQueue(ctx context.Context, instanceID string) cancelSummary {
	raw, err := s.redis.Get(ctx, newsExtractionQueueKey).Bytes()
	if err != nil {
		return cancelSummary{}
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return cancelSummary{}
	}
	result := cancelSummary{TaskIDs: []string{}}
	items := anySlice(payload["items"])
	for _, itemRaw := range items {
		item, _ := itemRaw.(map[string]any)
		if instanceID != "" && stringValue(item["instance_id"]) != instanceID {
			continue
		}
		status := stringValue(item["status"])
		if !activeModelStatus(status) && status != "failed" {
			continue
		}
		if activeModelStatus(status) && stringValue(item["task_id"]) != "" {
			result.TaskIDs = append(result.TaskIDs, stringValue(item["task_id"]))
		}
		stamp := iso(time.Now())
		item["status"], item["updated_at"], item["completed_at"], item["attempt_started_at"], item["error"] = "cancelled", stamp, stamp, nil, nil
		result.Cancelled++
	}
	payload["state"], payload["error"] = "cancelled", nil
	_ = s.writeRedisJSON(ctx, newsExtractionQueueKey, payload, 12*time.Hour)
	return result
}

func (s *Server) cancelResearch(ctx context.Context, kind, entityID, taskID string, includeFailed bool, instanceID string) (map[string]any, error) {
	if kind == "asset_research" && entityID == "" {
		return nil, fail(http.StatusUnprocessableEntity, "asset research cancellation requires entity_id")
	}
	if kind == "event_research" {
		if _, err := uuid.Parse(taskID); err != nil {
			return nil, fail(http.StatusUnprocessableEntity, "invalid event research task_id")
		}
	}
	statuses := map[string]int{}
	assetRuns, eventRuns := 0, 0
	assetQuery := `SELECT id,payload::jsonb FROM research_runs WHERE (status IN ('queued','running','verifying') OR ($1 AND status='failed' AND updated_at>=now()-interval '24 hours'))`
	args := []any{includeFailed}
	if kind == "asset_research" {
		assetQuery += ` AND asset_id=$2`
		args = append(args, entityID)
	}
	if kind != "event_research" {
		rows, err := s.db.Query(ctx, assetQuery, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var body []byte
			if rows.Scan(&id, &body) != nil {
				continue
			}
			payload, _ := decodeDefault(body, map[string]any{}).(map[string]any)
			if instanceID != "" && stringValue(payload["model_instance_id"]) != instanceID {
				continue
			}
			previous := stringValue(payload["status"])
			statuses[previous]++
			cancelResearchPayload(payload, previous)
			updated, _ := json.Marshal(payload)
			_, _ = s.db.Exec(ctx, `UPDATE research_runs SET status='cancelled',payload=$2,updated_at=now() WHERE id=$1`, id, updated)
			s.updateInstanceAssignment(ctx, "research", stringValue(payload["celery_task_id"]), "cancelled", stringValue(payload["model_instance_id"]))
			assetRuns++
		}
		rows.Close()
	}
	if kind != "asset_research" {
		eventQuery := `SELECT id,payload::jsonb FROM event_research_runs WHERE (status IN ('queued','running','verifying') OR ($1 AND status='failed' AND updated_at>=now()-interval '24 hours'))`
		eventArgs := []any{includeFailed}
		if kind == "event_research" {
			eventQuery += ` AND id=$2`
			eventArgs = append(eventArgs, taskID)
		}
		rows, err := s.db.Query(ctx, eventQuery, eventArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var body []byte
			if rows.Scan(&id, &body) != nil {
				continue
			}
			payload, _ := decodeDefault(body, map[string]any{}).(map[string]any)
			if instanceID != "" && stringValue(payload["model_instance_id"]) != instanceID {
				continue
			}
			previous := stringValue(payload["status"])
			statuses[previous]++
			cancelResearchPayload(payload, previous)
			updated, _ := json.Marshal(payload)
			_, _ = s.db.Exec(ctx, `UPDATE event_research_runs SET status='cancelled',payload=$2,updated_at=now() WHERE id=$1`, id, updated)
			s.updateInstanceAssignment(ctx, "research", stringValue(payload["celery_task_id"]), "cancelled", stringValue(payload["model_instance_id"]))
			eventRuns++
		}
		rows.Close()
	}
	return map[string]any{"cancelled": assetRuns + eventRuns, "asset_runs": assetRuns, "event_runs": eventRuns, "counts_by_status": statuses}, nil
}

func cancelResearchPayload(payload map[string]any, previous string) {
	stamp := jsonTime(time.Now())
	payload["status"], payload["error"], payload["retryable_reason"], payload["updated_at"] = "cancelled", nil, nil, stamp
	if _, exists := payload["completed_at"]; exists {
		payload["completed_at"] = stamp
	}
	payload["analysis_steps"] = append(anySlice(payload["analysis_steps"]), analysisStep("research_cancelled", "cancelled", "admin-api", "用户取消了当前研究任务。", map[string]any{"previous_status": previous}))
}

func (s *Server) purgeResearchQueues(ctx context.Context, instanceID string) int64 {
	if instanceID != "" {
		return s.purgeCeleryQueue(ctx, "research."+instanceID)
	}
	count := s.purgeCeleryQueue(ctx, "research")
	for index := range modelURLs("research") {
		count += s.purgeCeleryQueue(ctx, "research.research-"+strconv.Itoa(index))
	}
	return count
}

func activeModelStatus(status string) bool {
	switch status {
	case "queued", "running", "retrying", "verifying", "generating", "testing", "merging":
		return true
	default:
		return false
	}
}

func failedModelStatus(status string) bool {
	return status == "failed" || status == "rejected" || status == "rolled_back"
}

func recentWithin(value any, now time.Time, duration time.Duration) bool {
	stamp := parseAnyTime(value)
	return stamp != nil && !stamp.Before(now.Add(-duration))
}
