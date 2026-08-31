package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	scanGateKey  = "market-loop:scan:active"
	scanPauseKey = "market-loop:scan:pause"
)

type apiFailure struct {
	Status int
	Detail string
}

func (e *apiFailure) Error() string { return e.Detail }

func fail(status int, detail string) error { return &apiFailure{Status: status, Detail: detail} }

func writeAPIFailure(w http.ResponseWriter, err error) {
	var failure *apiFailure
	if errors.As(err, &failure) {
		writeError(w, failure.Status, failure.Detail)
		return
	}
	slog.Error("command operation failed", "error", err)
	writeError(w, http.StatusInternalServerError, "internal operation failed")
}

func (s *Server) refreshAssetUniverse(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	taskID := uuid.NewString()
	if err := s.publishCelery(r.Context(), "market_loop.refresh_asset_universe", "io", taskID, nil, nil, 5); err != nil {
		writeError(w, http.StatusServiceUnavailable, "asset universe refresh could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": taskID, "status": "queued"})
}

func (s *Server) backfillAssetMappings(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	days, ok := intQuery(w, r.URL.Query(), "days", 7, 1, 30)
	if !ok {
		return
	}
	taskID := uuid.NewString()
	if err := s.publishCelery(r.Context(), "market_loop.backfill_asset_mappings", "io", taskID, nil, map[string]any{"days": days}, 5); err != nil {
		writeError(w, http.StatusServiceUnavailable, "asset mapping backfill could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": taskID, "status": "queued", "days": days})
}

type backgroundInput struct {
	Background *bool `json:"background"`
}

func (s *Server) startScan(w http.ResponseWriter, r *http.Request) {
	input := backgroundInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	background := input.Background == nil || *input.Background
	existing, _ := s.redis.Get(r.Context(), scanGateKey).Result()
	if existing != "" {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": existing, "status": "already_queued", "scan": s.scanStatusPayload(r.Context())})
		return
	}
	taskID := uuid.NewString()
	claimed, err := s.redis.SetNX(r.Context(), scanGateKey, taskID, 12*time.Hour).Result()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "scan state unavailable")
		return
	}
	if !claimed {
		existing, _ = s.redis.Get(r.Context(), scanGateKey).Result()
		writeJSON(w, http.StatusOK, map[string]any{"task_id": defaultValue(existing, taskID), "status": "already_queued", "scan": s.scanStatusPayload(r.Context())})
		return
	}
	_ = s.redis.Del(r.Context(), scanPauseKey).Err()
	status := s.scanStatusPayload(r.Context())
	for key, value := range map[string]any{"state": "queued", "task_id": taskID, "phase": "queued", "paused_from_phase": nil, "current": 0, "total": 0, "started_at": nil, "next_scan_at": nil, "last_error": nil, "heartbeat_at": iso(time.Now())} {
		status[key] = value
	}
	if err = s.writeRedisJSON(r.Context(), scanStatusKey, status, 0); err == nil {
		err = s.publishCelery(r.Context(), "market_loop.scan_news", "io", taskID, nil, nil, 5)
	}
	if err != nil {
		_ = s.redis.Del(r.Context(), scanGateKey).Err()
		writeError(w, http.StatusServiceUnavailable, "scan could not be queued")
		return
	}
	if background {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "status": "queued", "scan": s.scanStatusPayload(r.Context())})
		return
	}
	result, waitErr := s.waitCelery(r.Context(), taskID, 30*time.Minute)
	if waitErr != nil {
		writeError(w, http.StatusGatewayTimeout, waitErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) pauseScan(w http.ResponseWriter, r *http.Request) {
	s.updateScanPause(w, r, true)
}

func (s *Server) resumeScan(w http.ResponseWriter, r *http.Request) {
	s.updateScanPause(w, r, false)
}

func (s *Server) updateScanPause(w http.ResponseWriter, r *http.Request, paused bool) {
	taskID, _ := s.redis.Get(r.Context(), scanGateKey).Result()
	if taskID == "" {
		writeError(w, http.StatusConflict, "no active scan")
		return
	}
	status := s.scanStatusPayload(r.Context())
	if paused {
		pausedFrom := stringValue(status["phase"])
		if stringValue(status["state"]) == "paused" {
			pausedFrom = stringValue(status["paused_from_phase"])
		}
		if pausedFrom == "" {
			pausedFrom = "discovering"
		}
		_ = s.redis.Set(r.Context(), scanPauseKey, taskID, 12*time.Hour).Err()
		status["state"], status["phase"], status["paused_from_phase"], status["next_scan_at"] = "paused", "paused", pausedFrom, nil
	} else {
		_ = s.redis.Del(r.Context(), scanPauseKey).Err()
		phase := defaultValue(stringValue(status["paused_from_phase"]), "discovering")
		status["state"], status["phase"], status["paused_from_phase"], status["next_scan_at"] = "running", phase, nil, nil
	}
	status["task_id"], status["heartbeat_at"] = taskID, iso(time.Now())
	if err := s.writeRedisJSON(r.Context(), scanStatusKey, status, 0); err != nil {
		writeError(w, http.StatusServiceUnavailable, "scan state unavailable")
		return
	}
	responseStatus := "running"
	if paused {
		responseStatus = "paused"
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": responseStatus, "scan": s.scanStatusPayload(r.Context())})
}

type researchInput struct {
	AssetID    string  `json:"asset_id"`
	EventID    *string `json:"event_id"`
	AsOf       *string `json:"as_of"`
	Historical bool    `json:"historical_replay"`
	Background *bool   `json:"background"`
}

type researchOptions struct {
	RetryOf      string
	RetryAttempt int
	Preferred    string
	Force        bool
	QueuePhase   string
	Priority     int
	Historical   bool
	AsOf         *time.Time
}

func (s *Server) startResearch(w http.ResponseWriter, r *http.Request) {
	var input researchInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.AssetID = strings.TrimSpace(input.AssetID)
	if input.AssetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id is required")
		return
	}
	var asOf *time.Time
	if input.AsOf != nil && *input.AsOf != "" {
		parsed := parseAnyTime(*input.AsOf)
		if parsed == nil {
			writeError(w, http.StatusUnprocessableEntity, "invalid as_of")
			return
		}
		asOf = parsed
	}
	result, err := s.enqueueAssetResearch(r.Context(), input.AssetID, stringPointerValue(input.EventID), researchOptions{Historical: input.Historical, AsOf: asOf, Priority: 3})
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	background := input.Background == nil || *input.Background
	if background {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": result.TaskID, "run_id": result.RunID, "status": "queued"})
		return
	}
	value, waitErr := s.waitCelery(r.Context(), result.TaskID, 45*time.Minute)
	if waitErr != nil {
		writeError(w, http.StatusGatewayTimeout, waitErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, value)
}

type queuedResearch struct {
	TaskID, RunID, InstanceID string
	RetryAttempt              int
}

func (s *Server) enqueueAssetResearch(ctx context.Context, assetID, eventID string, options researchOptions) (queuedResearch, error) {
	var assetBody []byte
	if err := s.db.QueryRow(ctx, `SELECT `+assetJSON+` FROM assets WHERE asset_id=$1 AND active=true`, assetID).Scan(&assetBody); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return queuedResearch{}, fail(http.StatusNotFound, "asset not found")
		}
		return queuedResearch{}, err
	}
	asset, _ := decodeDefault(assetBody, map[string]any{}).(map[string]any)
	normalizeAsset(asset)
	var event map[string]any
	if eventID != "" {
		if _, err := uuid.Parse(eventID); err != nil {
			return queuedResearch{}, fail(http.StatusUnprocessableEntity, "invalid event_id")
		}
		var body []byte
		if err := s.db.QueryRow(ctx, `SELECT payload::jsonb FROM news_events WHERE id=$1`, eventID).Scan(&body); err != nil {
			if errors.Is(err, pgx.ErrNoRows) && options.RetryOf == "" && !options.Force {
				eventID = ""
			} else if errors.Is(err, pgx.ErrNoRows) {
				return queuedResearch{}, fail(http.StatusConflict, "source event no longer exists")
			} else {
				return queuedResearch{}, err
			}
		}
		if eventID != "" {
			event, _ = decodeDefault(body, map[string]any{}).(map[string]any)
		}
	}
	var activeID string
	if err := s.db.QueryRow(ctx, `SELECT id FROM research_runs WHERE asset_id=$1 AND status IN ('queued','running','verifying') ORDER BY created_at LIMIT 1`, assetID).Scan(&activeID); err == nil {
		return queuedResearch{}, fail(http.StatusConflict, "该标的已有排队中或执行中的研究任务。")
	}
	if options.RetryOf == "" && !options.Force && s.cfg.ResearchCooldown > 0 {
		var recentID string
		var completedAt time.Time
		err := s.db.QueryRow(ctx, `SELECT id,(payload->>'completed_at')::timestamptz FROM research_runs
			WHERE asset_id=$1 AND status IN ('completed','insufficient_evidence')
			  AND coalesce((payload->>'historical_replay')::boolean,false)=false
			  AND (payload->>'completed_at')::timestamptz > $2
			ORDER BY (payload->>'completed_at')::timestamptz DESC LIMIT 1`, assetID, time.Now().UTC().Add(-s.cfg.ResearchCooldown)).Scan(&recentID, &completedAt)
		if err == nil {
			hours := int(s.cfg.ResearchCooldown.Hours())
			return queuedResearch{}, fail(http.StatusConflict, fmt.Sprintf("该标的在过去 %d 小时内已经完成过研究；任务 %s 可在 %s 后再次创建。", hours, recentID, iso(completedAt.Add(s.cfg.ResearchCooldown))))
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return queuedResearch{}, err
		}
	}
	instanceID, err := s.selectModelInstance(ctx, "research", options.Preferred)
	if err != nil {
		status := http.StatusConflict
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		return queuedResearch{}, fail(status, err.Error())
	}
	now := time.Now().UTC()
	stamp := now
	if options.AsOf != nil {
		stamp = options.AsOf.UTC()
	}
	runID, taskID := uuid.NewString(), uuid.NewString()
	phase := options.QueuePhase
	if phase == "" {
		if options.RetryOf != "" {
			phase = "research_retry_queue"
		} else {
			phase = "research_queue"
		}
	}
	steps := make([]any, 0)
	if event != nil {
		steps = append(steps, anySlice(event["analysis_steps"])...)
	}
	metrics := map[string]any{"instance_id": instanceID, "priority": options.Priority}
	if options.RetryOf != "" {
		metrics["retry_of_run_id"], metrics["retry_attempt"] = options.RetryOf, options.RetryAttempt
	}
	steps = append(steps, analysisStep(phase, "queued", "celery", "已创建深度研究任务。", metrics))
	run := map[string]any{
		"id": runID, "event_id": nil, "trigger_event_ids": []any{}, "asset": asset, "status": "queued", "as_of": jsonTime(stamp),
		"historical_replay": options.Historical, "retry_of_run_id": nil, "retry_attempt": options.RetryAttempt,
		"celery_task_id": taskID, "model_instance_id": instanceID, "coalesced_into_run_id": nil, "retryable_reason": nil,
		"verification_round": 0, "missing_requirements": []any{}, "contradictions": []any{}, "evidence": []any{},
		"recommendation": nil, "error": nil, "analysis_steps": steps, "created_at": jsonTime(now), "started_at": nil,
		"completed_at": nil, "updated_at": jsonTime(now),
	}
	if eventID != "" {
		run["event_id"], run["trigger_event_ids"] = eventID, []any{eventID}
	}
	if options.RetryOf != "" {
		run["retry_of_run_id"] = options.RetryOf
	}
	body, _ := json.Marshal(run)
	_, err = s.db.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at) VALUES($1,$2,$3,'queued',$4,$5,$5)`, runID, nullableUUID(eventID), assetID, body, now)
	if err != nil {
		return queuedResearch{}, err
	}
	_ = s.redis.Set(ctx, "market-loop:research:dispatch:"+runID, taskID, 30*24*time.Hour).Err()
	queue := "research." + instanceID
	err = s.publishCelery(ctx, "market_loop.research_asset", queue, taskID, []any{assetID, nullableString(eventID), runID}, map[string]any{"model_instance_id": instanceID}, options.Priority)
	if err != nil {
		run["status"], run["error"], run["completed_at"], run["updated_at"] = "failed", "research queue failed", jsonTime(time.Now()), jsonTime(time.Now())
		failedBody, _ := json.Marshal(run)
		_, _ = s.db.Exec(ctx, `UPDATE research_runs SET status='failed',payload=$2,updated_at=now() WHERE id=$1`, runID, failedBody)
		return queuedResearch{}, fail(http.StatusServiceUnavailable, "research queue failed")
	}
	return queuedResearch{TaskID: taskID, RunID: runID, InstanceID: instanceID, RetryAttempt: options.RetryAttempt}, nil
}

func (s *Server) retryResearchRun(w http.ResponseWriter, r *http.Request) {
	preferred, ok := researchInstanceQuery(w, r)
	if !ok {
		return
	}
	result, err := s.retryAssetResearch(r.Context(), chi.URLParam(r, "runID"), preferred)
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": result.TaskID, "run_id": result.RunID, "retry_of_run_id": chi.URLParam(r, "runID"), "retry_attempt": result.RetryAttempt, "instance_id": result.InstanceID, "status": "queued"})
}

func (s *Server) retryAssetResearch(ctx context.Context, runID, preferred string) (queuedResearch, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return queuedResearch{}, fail(http.StatusUnprocessableEntity, "invalid run_id")
	}
	var body []byte
	if err := s.db.QueryRow(ctx, `SELECT payload::jsonb FROM research_runs WHERE id=$1`, runID).Scan(&body); err != nil {
		return queuedResearch{}, fail(http.StatusNotFound, "run not found")
	}
	run, _ := decodeDefault(body, map[string]any{}).(map[string]any)
	if stringValue(run["status"]) != "failed" && run["retryable_reason"] == nil {
		return queuedResearch{}, fail(http.StatusConflict, "only failed or model-degraded research runs can be retried")
	}
	if run["retry_of_run_id"] != nil {
		return queuedResearch{}, fail(http.StatusConflict, "retry the original failed research run")
	}
	var activeCount, retryAttempt int
	_ = s.db.QueryRow(ctx, `SELECT count(*)::int,coalesce(max((payload->>'retry_attempt')::int),0)::int FROM research_runs WHERE payload->>'retry_of_run_id'=$1`, runID).Scan(&activeCount, &retryAttempt)
	var activeRetry int
	_ = s.db.QueryRow(ctx, `SELECT count(*)::int FROM research_runs WHERE payload->>'retry_of_run_id'=$1 AND status IN ('queued','running','verifying')`, runID).Scan(&activeRetry)
	if activeRetry > 0 {
		return queuedResearch{}, fail(http.StatusConflict, "a retry is already queued or running")
	}
	asset, _ := run["asset"].(map[string]any)
	assetID := stringValue(asset["asset_id"])
	eventID := stringValue(run["event_id"])
	return s.enqueueAssetResearch(ctx, assetID, eventID, researchOptions{RetryOf: runID, RetryAttempt: retryAttempt + 1, Preferred: preferred, Priority: priorityForPreferred(preferred)})
}

func (s *Server) retryEventResearchRun(w http.ResponseWriter, r *http.Request) {
	preferred, ok := researchInstanceQuery(w, r)
	if !ok {
		return
	}
	result, err := s.retryEventResearch(r.Context(), chi.URLParam(r, "runID"), preferred)
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) retryEventResearch(ctx context.Context, runID, preferred string) (map[string]any, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fail(http.StatusUnprocessableEntity, "invalid run_id")
	}
	var body []byte
	if err := s.db.QueryRow(ctx, `SELECT payload::jsonb FROM event_research_runs WHERE id=$1`, runID).Scan(&body); err != nil {
		return nil, fail(http.StatusNotFound, "event research run not found")
	}
	run, _ := decodeDefault(body, map[string]any{}).(map[string]any)
	if stringValue(run["status"]) != "failed" && run["retryable_reason"] == nil {
		return nil, fail(http.StatusConflict, "only failed or model-degraded event research runs can be retried")
	}
	eventID := stringValue(run["event_id"])
	var eventExists bool
	_ = s.db.QueryRow(ctx, `SELECT exists(SELECT 1 FROM news_events WHERE id=$1)`, eventID).Scan(&eventExists)
	if !eventExists {
		return nil, fail(http.StatusConflict, "source event no longer exists")
	}
	instanceID, err := s.selectModelInstance(ctx, "research", preferred)
	if err != nil {
		status := http.StatusConflict
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		return nil, fail(status, err.Error())
	}
	taskID := uuid.NewString()
	retryCount := int(int64Value(run["retry_count"])) + 1
	run["status"], run["as_of"], run["verification_round"], run["retry_count"] = "queued", jsonTime(time.Now()), 0, retryCount
	run["missing_requirements"], run["contradictions"], run["evidence"], run["report"] = []any{}, []any{}, []any{}, nil
	run["error"], run["retryable_reason"], run["celery_task_id"], run["model_instance_id"] = nil, nil, taskID, instanceID
	run["analysis_steps"] = append(anySlice(run["analysis_steps"]), analysisStep("event_research_retry_queue", "queued", "celery", "已创建事件研报重新执行任务。", map[string]any{"retry_count": retryCount, "instance_id": instanceID, "priority": 1}))
	run["updated_at"] = jsonTime(time.Now())
	updated, _ := json.Marshal(run)
	if _, err = s.db.Exec(ctx, `UPDATE event_research_runs SET status='queued',payload=$2,updated_at=now() WHERE id=$1`, runID, updated); err != nil {
		return nil, err
	}
	_ = s.redis.Set(ctx, "market-loop:research:dispatch:"+runID, taskID, 30*24*time.Hour).Err()
	if err = s.publishCelery(ctx, "market_loop.research_event", "research."+instanceID, taskID, []any{eventID, runID}, map[string]any{"model_instance_id": instanceID}, 1); err != nil {
		return nil, fail(http.StatusServiceUnavailable, "event research retry queue failed")
	}
	return map[string]any{"task_id": taskID, "run_id": runID, "retry_count": retryCount, "instance_id": instanceID, "status": "queued"}, nil
}

func (s *Server) retryFailedResearchRuns(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT 'asset',id FROM research_runs WHERE (status='failed' OR payload->>'retryable_reason' IS NOT NULL) AND payload->>'retry_of_run_id' IS NULL UNION ALL SELECT 'event',id FROM event_research_runs WHERE status='failed' OR payload->>'retryable_reason' IS NOT NULL`)
	if err != nil {
		writeError(w, 500, "failed research query failed")
		return
	}
	defer rows.Close()
	results := make([]map[string]any, 0)
	for rows.Next() {
		var kind, sourceID string
		if rows.Scan(&kind, &sourceID) != nil {
			continue
		}
		item := map[string]any{"kind": kind, "source_run_id": sourceID, "run_id": nil, "task_id": nil, "status": "skipped", "detail": nil}
		if kind == "asset" {
			queued, queueErr := s.retryAssetResearch(r.Context(), sourceID, "")
			if queueErr == nil {
				item["run_id"], item["task_id"], item["status"] = queued.RunID, queued.TaskID, "queued"
			} else {
				item["detail"] = queueErr.Error()
			}
		} else {
			queued, queueErr := s.retryEventResearch(r.Context(), sourceID, "")
			if queueErr == nil {
				item["run_id"], item["task_id"], item["status"] = queued["run_id"], queued["task_id"], "queued"
			} else {
				item["detail"] = queueErr.Error()
			}
		}
		results = append(results, item)
	}
	retried := 0
	for _, item := range results {
		if item["status"] == "queued" {
			retried++
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"requested": len(results), "retried": retried, "skipped": len(results) - retried, "failed": 0, "results": results})
}

func (s *Server) researchConclusionAgain(w http.ResponseWriter, r *http.Request) {
	recommendationID := chi.URLParam(r, "recommendationID")
	if _, err := uuid.Parse(recommendationID); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid recommendation_id")
		return
	}
	var recommendationBody, runBody []byte
	if err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM recommendations WHERE id=$1`, recommendationID).Scan(&recommendationBody); err != nil {
		writeError(w, http.StatusNotFound, "recommendation not found")
		return
	}
	recommendation, _ := decodeDefault(recommendationBody, map[string]any{}).(map[string]any)
	runID := stringValue(recommendation["run_id"])
	if err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM research_runs WHERE id=$1`, runID).Scan(&runBody); err != nil {
		writeError(w, http.StatusConflict, "source research run no longer exists")
		return
	}
	run, _ := decodeDefault(runBody, map[string]any{}).(map[string]any)
	asset, _ := recommendation["asset"].(map[string]any)
	queued, err := s.enqueueAssetResearch(r.Context(), stringValue(asset["asset_id"]), stringValue(run["event_id"]), researchOptions{Force: true, QueuePhase: "forced_research_queue", Priority: 3})
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": queued.TaskID, "run_id": queued.RunID, "source_recommendation_id": recommendationID, "status": "queued"})
}

func (s *Server) researchEventConclusionAgain(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	if _, err := uuid.Parse(runID); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid run_id")
		return
	}
	var body []byte
	if err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM event_research_runs WHERE id=$1`, runID).Scan(&body); err != nil {
		writeError(w, http.StatusNotFound, "event conclusion not found")
		return
	}
	run, _ := decodeDefault(body, map[string]any{}).(map[string]any)
	status := stringValue(run["status"])
	if status == "queued" || status == "running" || status == "verifying" || fullResearchActive(run) {
		writeError(w, http.StatusConflict, "event research is already active")
		return
	}
	if status != "completed" && status != "insufficient_evidence" && status != "failed" {
		writeError(w, http.StatusConflict, "event conclusion is not refreshable")
		return
	}
	if run["report"] == nil {
		writeError(w, http.StatusConflict, "event conclusion has no report")
		return
	}
	eventID := stringValue(run["event_id"])
	var newsCount int
	if err := s.db.QueryRow(r.Context(), `SELECT count(*)::int FROM news_items WHERE id IN (SELECT jsonb_array_elements_text(payload->'news_item_ids') FROM news_events WHERE id=$1)`, eventID).Scan(&newsCount); err != nil || newsCount == 0 {
		writeError(w, http.StatusConflict, "该事件没有可用的关联原始新闻，无法执行完整重新研究。")
		return
	}
	instanceID, err := s.selectModelInstance(r.Context(), "extract", "")
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	taskID := uuid.NewString()
	run["analysis_steps"] = append(anySlice(run["analysis_steps"]), analysisStep("full_event_research", "queued", "celery", "已创建事件抽取、股票映射、深度研究与联网搜索的完整重跑任务。", map[string]any{"stage": "event_extraction", "task_id": taskID}))
	run["updated_at"] = jsonTime(time.Now())
	updated, _ := json.Marshal(run)
	if _, err = s.db.Exec(r.Context(), `UPDATE event_research_runs SET payload=$2,updated_at=now() WHERE id=$1`, runID, updated); err != nil {
		writeError(w, 500, "event research update failed")
		return
	}
	s.trackModelTask(r.Context(), "extract", taskID, "event_reextraction", eventID, "事件完整重新研究", "完整重新研究", "manual", instanceID)
	if err = s.publishCelery(r.Context(), "market_loop.reextract_event", "extract."+instanceID, taskID, []any{eventID, runID}, map[string]any{"model_instance_id": instanceID}, 5); err != nil {
		writeError(w, http.StatusServiceUnavailable, "event research could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": taskID, "run_id": runID, "source_run_id": runID, "status": "queued", "stage": "event_extraction"})
}

type evolutionInput struct {
	Failures   []map[string]any `json:"failures"`
	Background *bool            `json:"background"`
}

func (s *Server) proposeEvolution(w http.ResponseWriter, r *http.Request) {
	var input evolutionInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.Failures == nil {
		writeError(w, http.StatusUnprocessableEntity, "failures is required")
		return
	}
	background := input.Background == nil || *input.Background
	if background && !s.cfg.EvolutionEnabled {
		writeError(w, http.StatusConflict, "EVOLUTION_ENABLED is false")
		return
	}
	instanceID, err := s.selectModelInstance(r.Context(), "code", "")
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	taskID := uuid.NewString()
	s.trackModelTask(r.Context(), "code", taskID, "code_evolution", "", fmt.Sprintf("失败案例代码演进（%d 条）", len(input.Failures)), "等待生成改进方案", "manual", instanceID)
	if err = s.publishCelery(r.Context(), "market_loop.evolve_failures", "evolution."+instanceID, taskID, []any{input.Failures}, map[string]any{"model_instance_id": instanceID}, 5); err != nil {
		writeError(w, http.StatusServiceUnavailable, "evolution could not be queued")
		return
	}
	if background {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "status": "queued"})
		return
	}
	value, waitErr := s.waitCelery(r.Context(), taskID, 30*time.Minute)
	if waitErr != nil {
		writeError(w, http.StatusGatewayTimeout, waitErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) executeEvolution(w http.ResponseWriter, r *http.Request) {
	candidateID := chi.URLParam(r, "candidateID")
	if _, err := uuid.Parse(candidateID); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid candidate_id")
		return
	}
	var body []byte
	if err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM evolution_candidates WHERE id=$1`, candidateID).Scan(&body); err != nil {
		writeError(w, http.StatusNotFound, "evolution candidate not found")
		return
	}
	candidate, _ := decodeDefault(body, map[string]any{}).(map[string]any)
	background := true
	if raw := r.URL.Query().Get("background"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			validationError(w, "background", "Input should be a valid boolean")
			return
		}
		background = parsed
	}
	instanceID, err := s.selectModelInstance(r.Context(), "code", "")
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	taskID := uuid.NewString()
	s.trackModelTask(r.Context(), "code", taskID, "code_evolution", candidateID, stringValue(candidate["hypothesis"]), stringValue(candidate["target_metric"]), "manual", instanceID)
	if err = s.publishCelery(r.Context(), "market_loop.execute_evolution", "evolution."+instanceID, taskID, []any{candidateID}, map[string]any{"model_instance_id": instanceID}, 5); err != nil {
		writeError(w, http.StatusServiceUnavailable, "evolution execution could not be queued")
		return
	}
	if background {
		writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "status": "queued"})
		return
	}
	value, waitErr := s.waitCelery(r.Context(), taskID, 30*time.Minute)
	if waitErr != nil {
		writeError(w, http.StatusGatewayTimeout, waitErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) retryNewsProcessing(w http.ResponseWriter, r *http.Request) {
	newsID := chi.URLParam(r, "newsID")
	if _, err := uuid.Parse(newsID); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid news_id")
		return
	}
	var title, source string
	if err := s.db.QueryRow(r.Context(), `SELECT title,source FROM news_items WHERE id=$1`, newsID).Scan(&title, &source); err != nil {
		writeError(w, http.StatusNotFound, "news item not found")
		return
	}
	taskID, err := s.queueNewsRetry(r.Context(), newsID, title, source)
	if err != nil {
		writeAPIFailure(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "queued", "task_id": taskID, "news_id": newsID, "title": title})
}

func (s *Server) queueNewsRetry(ctx context.Context, newsID, title, source string) (string, error) {
	var active int
	_ = s.db.QueryRow(ctx, `SELECT count(*)::int FROM news_processing WHERE news_id=$1 AND status IN ('dispatch_pending','queued','running','retrying')`, newsID).Scan(&active)
	if active > 0 {
		return "", fail(http.StatusConflict, "news retry already active")
	}
	return s.queueNewsRetryWithOptions(ctx, newsID, title, source, "", 5)
}

func (s *Server) rescanSourceFilterLog(w http.ResponseWriter, r *http.Request) {
	logID := chi.URLParam(r, "logID")
	if _, err := uuid.Parse(logID); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid log_id")
		return
	}
	var contentHash, source, title, urlValue, reason string
	var published, firstFiltered, lastFiltered time.Time
	var hitCount int
	if err := s.db.QueryRow(r.Context(), `SELECT content_hash,source,title,url,matched_keyword,published_at,first_filtered_at,last_filtered_at,hit_count FROM news_filter_logs WHERE id=$1`, logID).Scan(&contentHash, &source, &title, &urlValue, &reason, &published, &firstFiltered, &lastFiltered, &hitCount); err != nil {
		writeError(w, http.StatusNotFound, "filtered news record not found")
		return
	}
	if reason != "未命中白名单" {
		writeError(w, http.StatusConflict, "only whitelist-miss records can be rescanned")
		return
	}
	originalHash := contentHash
	var newsID string
	err := s.db.QueryRow(r.Context(), `SELECT id FROM news_items WHERE content_hash=$1`, contentHash).Scan(&newsID)
	if err == nil {
		var alreadyInEvent bool
		_ = s.db.QueryRow(r.Context(), `SELECT EXISTS(
			SELECT 1 FROM news_events e, jsonb_array_elements_text(coalesce(e.payload->'news_item_ids','[]'::jsonb)) item
			WHERE item.value=$1
		)`, newsID).Scan(&alreadyInEvent)
		if alreadyInEvent {
			contentHash = sha256Hex(originalHash + ":rescan:" + logID)
			err = s.db.QueryRow(r.Context(), `SELECT id FROM news_items WHERE content_hash=$1`, contentHash).Scan(&newsID)
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		newsID = uuid.NewString()
		quality := "aggregator"
		normalized := strings.ToLower(source)
		if normalized == "sec" || strings.HasPrefix(normalized, "sec ") || strings.Contains(normalized, "sec.gov") {
			quality = "official"
		} else if strings.Contains(normalized, "fmp") {
			quality = "professional"
		}
		metadata, _ := json.Marshal(map[string]any{"manual_source_filter_rescan": true, "source_filter_log_id": logID, "original_filter_reason": reason, "original_content_hash": originalHash})
		_, err = s.db.Exec(r.Context(), `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,$2,$3,$4,'',$5,'en',$6,now(),now(),$7,'[]',$8)`, newsID, source, quality, title, urlValue, published, contentHash, metadata)
		if err != nil {
			writeError(w, http.StatusConflict, "filtered news could not be restored")
			return
		}
	} else if err != nil {
		writeError(w, 500, "filtered news query failed")
		return
	}
	if _, err = s.db.Exec(r.Context(), `DELETE FROM news_filter_logs WHERE id=$1`, logID); err != nil {
		writeError(w, 500, "filtered news record could not be removed")
		return
	}
	taskID, queueErr := s.queueNewsRetry(r.Context(), newsID, title, source)
	if queueErr != nil {
		_, _ = s.db.Exec(r.Context(), `INSERT INTO news_filter_logs(id,content_hash,source,title,url,matched_keyword,published_at,first_filtered_at,last_filtered_at,hit_count)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO NOTHING`, logID, originalHash, source, title, urlValue, reason, published, firstFiltered, lastFiltered, hitCount)
		writeAPIFailure(w, queueErr)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "queued", "task_id": taskID, "news_id": newsID, "title": title})
}

func analysisStep(phase, status, executor, summary string, metrics map[string]any) map[string]any {
	return map[string]any{"phase": phase, "status": status, "executor": executor, "model": nil, "summary": summary, "metrics": defaultAny(metrics, map[string]any{}), "occurred_at": jsonTime(time.Now())}
}

func (s *Server) writeRedisJSON(ctx context.Context, key string, payload any, ttl time.Duration) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.redis.Set(ctx, key, body, ttl).Err()
}

func nullableUUID(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func priorityForPreferred(value string) int {
	if value != "" {
		return 0
	}
	return 3
}

func researchInstanceQuery(w http.ResponseWriter, r *http.Request) (string, bool) {
	values, present := r.URL.Query()["instance_id"]
	if !present {
		return "", true
	}
	value := ""
	if len(values) > 0 {
		value = strings.TrimSpace(values[0])
	}
	if value == "" || len(value) > 64 {
		validationError(w, "instance_id", "String should have at least 1 and at most 64 characters")
		return "", false
	}
	return value, true
}

func fullResearchActive(run map[string]any) bool {
	steps := anySlice(run["analysis_steps"])
	for index := len(steps) - 1; index >= 0; index-- {
		step, _ := steps[index].(map[string]any)
		if stringValue(step["phase"]) == "full_event_research" {
			status := stringValue(step["status"])
			return status == "queued" || status == "running" || status == "retrying"
		}
	}
	return false
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
