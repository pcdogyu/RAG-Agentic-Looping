package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	cleanupModelAuditsTask      = "market_loop.cleanup_model_audits"
	reconcileResearchLeasesTask = "market_loop.reconcile_research_leases"
	reconcileMappingLeasesTask  = "market_loop.reconcile_asset_mapping_leases"
	recoverOrphanedNewsTask     = "market_loop.recover_orphaned_news"
	recoveryNewsLimit           = 100
	recoveryEventLimit          = 100
	recoveryRetention           = 7 * 24 * time.Hour
	recoveryNewsGrace           = 2 * time.Minute
	recoveryNewsStale           = 10 * time.Minute
)

type recoveryRuntime struct {
	cfg    config.Config
	db     *pgxpool.Pool
	redis  *redis.Client
	store  *Store
	shared *ExtractRuntime
}

func NewRecoveryHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &recoveryRuntime{
		cfg: cfg, db: db, redis: redisClient, store: NewStore(db),
		shared: &ExtractRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: cfg.OllamaTimeout}},
	}
	return map[string]Handler{
		cleanupModelAuditsTask:      runtime.cleanupModelAudits,
		reconcileResearchLeasesTask: runtime.reconcileResearchLeases,
		reconcileMappingLeasesTask:  runtime.reconcileMappingLeases,
		recoverOrphanedNewsTask:     runtime.recoverOrphanedNews,
	}
}

func (runtime *recoveryRuntime) cleanupModelAudits(ctx context.Context, _ Job) (any, error) {
	result, err := runtime.db.Exec(ctx, `DELETE FROM model_call_audits WHERE started_at < now()-$1::interval`, interval(runtime.cfg.ModelAuditRetention))
	if err != nil {
		return nil, err
	}
	return map[string]any{"deleted": result.RowsAffected(), "retention_days": int(runtime.cfg.ModelAuditRetention.Hours() / 24)}, nil
}

func (runtime *recoveryRuntime) reconcileResearchLeases(ctx context.Context, _ Job) (any, error) {
	expired, err := runtime.store.ReconcileExpired(ctx)
	if err != nil {
		return nil, err
	}
	repaired, err := runtime.store.ReconcileResearchBusinessState(ctx, runtime.cfg.ResearchHardLimit)
	if err != nil {
		return nil, err
	}
	recovered, failed, err := runtime.recoverQueuedResearch(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"expired_jobs": expired, "repaired": repaired, "queued_recovered": recovered, "recovery_failed": failed}, nil
}

func (runtime *recoveryRuntime) reconcileMappingLeases(ctx context.Context, _ Job) (any, error) {
	expired, err := runtime.store.ReconcileExpired(ctx)
	if err != nil {
		return nil, err
	}
	var active, terminal int
	err = runtime.db.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status IN ('queued','running','retrying')),
		count(*) FILTER (WHERE status IN ('failed','cancelled'))
		FROM go_jobs WHERE task_type=$1 AND updated_at >= now()-$2::interval`, mappingTask, interval(runtime.cfg.ResearchLease*2)).Scan(&active, &terminal)
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": "completed", "expired_jobs": expired, "active": active, "terminal": terminal}, nil
}

type queuedResearch struct {
	Kind    string
	ID      uuid.UUID
	EventID *uuid.UUID
	AssetID string
	Payload map[string]any
}

func (runtime *recoveryRuntime) recoverQueuedResearch(ctx context.Context) (recovered, failed int, returnedErr error) {
	rows, err := runtime.db.Query(ctx, `
		SELECT 'asset',r.id,r.event_id,r.asset_id,r.payload::jsonb
		FROM research_runs r
		WHERE r.status='queued' AND r.updated_at < now()-$1::interval
		  AND NOT EXISTS (SELECT 1 FROM go_jobs j WHERE j.id::text=r.payload->>'celery_task_id' AND j.status IN ('queued','running','retrying'))
		UNION ALL
		SELECT 'event',r.id,r.event_id,''::text,r.payload::jsonb
		FROM event_research_runs r
		WHERE r.status='queued' AND r.updated_at < now()-$1::interval
		  AND NOT EXISTS (SELECT 1 FROM go_jobs j WHERE j.id::text=r.payload->>'celery_task_id' AND j.status IN ('queued','running','retrying'))
		ORDER BY 2 LIMIT 100`, interval(runtime.cfg.ResearchLease))
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	candidates := make([]queuedResearch, 0)
	for rows.Next() {
		var candidate queuedResearch
		var body []byte
		if err := rows.Scan(&candidate.Kind, &candidate.ID, &candidate.EventID, &candidate.AssetID, &body); err != nil {
			return recovered, failed, err
		}
		if err := json.Unmarshal(body, &candidate.Payload); err != nil {
			return recovered, failed, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return recovered, failed, err
	}
	for _, candidate := range candidates {
		if err := runtime.requeueResearch(ctx, candidate); err != nil {
			failed++
			continue
		}
		recovered++
	}
	return recovered, failed, nil
}

func (runtime *recoveryRuntime) requeueResearch(ctx context.Context, candidate queuedResearch) error {
	oldTaskID := stringValue(candidate.Payload["celery_task_id"])
	instanceID := stringValue(candidate.Payload["model_instance_id"])
	if instanceID == "" {
		instanceID = runtime.shared.selectDownstreamInstance(ctx, "research", len(runtime.cfg.ResearchURLs))
	}
	taskID := uuid.New()
	taskType, priority := researchAssetTask, int16(3)
	args := []any{candidate.AssetID, nil, candidate.ID.String()}
	if candidate.EventID != nil {
		args[1] = candidate.EventID.String()
	}
	if candidate.Kind == "event" {
		if candidate.EventID == nil {
			return errors.New("queued event research has no event id")
		}
		taskType, priority = researchEventTask, 1
		args = []any{candidate.EventID.String(), candidate.ID.String()}
	}
	actualID, err := runtime.store.Enqueue(ctx, EnqueueParams{
		ID: taskID, Queue: "research", TaskType: taskType,
		Payload:  taskEnvelope{Args: args, Kwargs: map[string]any{"model_instance_id": instanceID}},
		Priority: priority, MaxAttempts: 3, DedupeKey: "research-run:" + candidate.ID.String(),
	})
	if err != nil {
		return err
	}
	created := actualID == taskID
	taskID = actualID
	candidate.Payload["celery_task_id"], candidate.Payload["model_instance_id"], candidate.Payload["updated_at"] = taskID.String(), instanceID, iso(time.Now())
	appendAnalysisStep(candidate.Payload, analysisStep("research_dispatch_recovery", "queued", "go-recovery", "检测到研究任务缺少活动派发记录，已由 Go 恢复队列。 / Go recovered a queued research run whose active dispatch was missing.", map[string]any{"previous_task_id": oldTaskID, "instance_id": instanceID, "priority": priority}))
	body, _ := json.Marshal(candidate.Payload)
	table := "research_runs"
	if candidate.Kind == "event" {
		table = "event_research_runs"
	}
	command, err := runtime.db.Exec(ctx, fmt.Sprintf(`UPDATE %s SET payload=$2,updated_at=now() WHERE id=$1 AND status='queued' AND coalesce(payload->>'celery_task_id','')=$3`, table), candidate.ID, body, oldTaskID) //nolint:gosec
	if err != nil || command.RowsAffected() != 1 {
		if created {
			_ = runtime.store.Cancel(ctx, taskID)
		}
		if err != nil {
			return err
		}
		return errors.New("queued research changed during recovery")
	}
	if created {
		runtime.shared.recordModelTask(ctx, "research", taskID.String(), ternary(candidate.Kind == "event", "event_research", "asset_research"), candidate.ID.String(), "研究任务自动恢复", candidate.AssetID, "automatic_recovery", instanceID)
	}
	return nil
}

type orphanNewsState struct {
	ID           uuid.UUID
	Processed    bool
	Status       *string
	Heartbeat    *time.Time
	Updated      *time.Time
	Created      *time.Time
	OutboxStatus *string
}

func shouldRecoverNews(state orphanNewsState, staleCutoff time.Time) (recover, stale bool) {
	if state.Processed || (state.Status != nil && *state.Status == "cancelled") {
		return false, false
	}
	status := ""
	if state.Status != nil {
		status = *state.Status
	}
	if status == "queued" || status == "running" || status == "retrying" {
		heartbeat := state.Created
		if state.Updated != nil {
			heartbeat = state.Updated
		}
		if state.Heartbeat != nil {
			heartbeat = state.Heartbeat
		}
		if heartbeat != nil && heartbeat.After(staleCutoff) {
			return false, false
		}
		stale = true
	}
	if status == "dispatch_pending" && state.OutboxStatus != nil && (*state.OutboxStatus == "pending" || *state.OutboxStatus == "failed") {
		return false, stale
	}
	return true, stale
}

func (runtime *recoveryRuntime) recoverOrphanedNews(ctx context.Context, _ Job) (any, error) {
	now := time.Now().UTC()
	rows, err := runtime.db.Query(ctx, `WITH processed AS MATERIALIZED (
		SELECT DISTINCT jsonb_array_elements_text(coalesce(payload::jsonb->'news_item_ids','[]'::jsonb)) AS news_id
		FROM news_events
	), candidates AS MATERIALIZED (
		SELECT id,observed_at FROM news_items
		WHERE observed_at >= $1 AND observed_at <= $2
		ORDER BY observed_at,id LIMIT $3
	)
		SELECT n.id,processed.news_id IS NOT NULL,
		p.status,p.heartbeat_at,p.updated_at,p.created_at,o.status
		FROM candidates n
		LEFT JOIN processed ON processed.news_id=n.id::text
		LEFT JOIN news_processing p ON p.news_id=n.id
		LEFT JOIN news_processing_outbox o ON o.news_id=n.id
		ORDER BY n.observed_at,n.id`, now.Add(-recoveryRetention), now.Add(-recoveryNewsGrace), recoveryNewsLimit*5)
	if err != nil {
		return nil, err
	}
	states := make([]orphanNewsState, 0)
	for rows.Next() {
		var state orphanNewsState
		if err := rows.Scan(&state.ID, &state.Processed, &state.Status, &state.Heartbeat, &state.Updated, &state.Created, &state.OutboxStatus); err != nil {
			rows.Close()
			return nil, err
		}
		states = append(states, state)
	}
	rows.Close()
	recovered, stale := 0, 0
	for _, state := range states {
		if state.Processed {
			_, _ = runtime.db.Exec(ctx, `UPDATE news_processing SET status='completed',completed_at=coalesce(completed_at,now()),heartbeat_at=now(),updated_at=now() WHERE news_id=$1 AND status<>'completed'`, state.ID)
			continue
		}
		recover, wasStale := shouldRecoverNews(state, now.Add(-recoveryNewsStale))
		if wasStale {
			stale++
		}
		if !recover || recovered >= recoveryNewsLimit {
			continue
		}
		_, err := runtime.db.Exec(ctx, `WITH processing AS (
			INSERT INTO news_processing(news_id,status,scan_task_id,celery_task_id,attempt_count,last_error,queued_at,started_at,completed_at,heartbeat_at,created_at,updated_at)
			VALUES($1,'dispatch_pending',NULL,NULL,0,NULL,NULL,NULL,NULL,now(),now(),now())
			ON CONFLICT(news_id) DO UPDATE SET status='dispatch_pending',scan_task_id=NULL,celery_task_id=NULL,last_error=NULL,queued_at=NULL,started_at=NULL,completed_at=NULL,heartbeat_at=now(),updated_at=now()
		)
		INSERT INTO news_processing_outbox(id,news_id,status,force_asset_mapping,dispatch_attempts,available_at,dispatched_at,last_error,created_at,updated_at)
		VALUES($2,$1,'pending',true,0,now(),NULL,NULL,now(),now())
		ON CONFLICT(news_id) DO UPDATE SET status='pending',force_asset_mapping=true,available_at=now(),dispatched_at=NULL,last_error=NULL,updated_at=now()`, state.ID, uuid.New())
		if err != nil {
			return nil, err
		}
		recovered++
	}
	followups, err := runtime.recoverStrandedEvents(ctx, now)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"recovered": recovered, "stale": stale}
	for key, value := range followups {
		result[key] = value
	}
	return result, nil
}

func (runtime *recoveryRuntime) recoverStrandedEvents(ctx context.Context, now time.Time) (map[string]int, error) {
	result := map[string]int{"stranded_events": 0, "event_research_queued": 0, "asset_mapping_queued": 0, "active_mapping": 0, "followup_failed": 0}
	if !runtime.cfg.AutoResearch {
		return result, nil
	}
	rows, err := runtime.db.Query(ctx, `SELECT e.payload::jsonb FROM news_events e
		WHERE e.observed_at >= $1
		AND NOT EXISTS(SELECT 1 FROM event_research_runs r WHERE r.event_id=e.id)
		AND NOT EXISTS(SELECT 1 FROM research_runs r WHERE r.event_id=e.id)
		ORDER BY e.observed_at,e.id LIMIT $2`, now.Add(-recoveryRetention), recoveryEventLimit)
	if err != nil {
		return nil, err
	}
	events := make([]map[string]any, 0)
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			rows.Close()
			return nil, err
		}
		var event map[string]any
		if json.Unmarshal(body, &event) == nil {
			events = append(events, event)
		}
	}
	rows.Close()
	result["stranded_events"] = len(events)
	for _, event := range events {
		if recoveryMappingActive(event) {
			result["active_mapping"]++
			continue
		}
		var queued bool
		if recoveryMappingTerminal(event) {
			queued, err = runtime.shared.enqueueEventResearch(ctx, event, false)
			result["event_research_queued"] += boolInt(queued)
		} else {
			force := latestAnalysisStep(event, "asset_mapping_queue") != nil
			queued, err = runtime.shared.enqueueMapping(ctx, event, force, false, false)
			result["asset_mapping_queued"] += boolInt(queued)
		}
		if err != nil {
			result["followup_failed"]++
		}
	}
	return result, nil
}

func recoveryMappingActive(event map[string]any) bool {
	queueStep := latestAnalysisStep(event, "asset_mapping_queue")
	if queueStep == nil || stringValue(queueStep["status"]) != "queued" {
		return false
	}
	mappingStep := latestAnalysisStep(event, "asset_mapping")
	return mappingStep == nil || !parseTime(mappingStep["occurred_at"]).After(parseTime(queueStep["occurred_at"])) || stringValue(mappingStep["status"]) == "running" || stringValue(mappingStep["status"]) == "retrying"
}

func recoveryMappingTerminal(event map[string]any) bool {
	if len(anySlice(event["candidates"])) > 0 {
		return true
	}
	queueStep := latestAnalysisStep(event, "asset_mapping_queue")
	mappingStep := latestAnalysisStep(event, "asset_mapping")
	if queueStep != nil {
		if stringValue(queueStep["status"]) == "completed" {
			return true
		}
		return mappingStep != nil && parseTime(mappingStep["occurred_at"]).After(parseTime(queueStep["occurred_at"])) && (stringValue(mappingStep["status"]) == "completed" || stringValue(mappingStep["status"]) == "unmapped")
	}
	return mappingStep != nil && (stringValue(mappingStep["status"]) == "completed" || stringValue(mappingStep["status"]) == "unmapped")
}

type recoverySchedule struct {
	task     string
	interval time.Duration
}

var recoverySchedules = []recoverySchedule{
	{task: recoverOrphanedNewsTask, interval: time.Minute},
	{task: reconcileMappingLeasesTask, interval: time.Minute},
	{task: reconcileResearchLeasesTask, interval: 5 * time.Minute},
	{task: cleanupModelAuditsTask, interval: 24 * time.Hour},
}

type RecoveryScheduler struct {
	cfg   config.Config
	store *Store
	redis *redis.Client
}

func NewRecoveryScheduler(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) *RecoveryScheduler {
	return &RecoveryScheduler{cfg: cfg, store: NewStore(db), redis: redisClient}
}

func (scheduler *RecoveryScheduler) Enabled() bool {
	return true
}

func (scheduler *RecoveryScheduler) Tick(ctx context.Context) error {
	if !scheduler.Enabled() {
		return nil
	}
	for _, spec := range recoverySchedules {
		key := "market-loop:go-schedule:" + spec.task
		claimed, err := scheduler.redis.SetNX(ctx, key, iso(time.Now()), spec.interval).Result()
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		_, err = scheduler.store.Enqueue(ctx, EnqueueParams{Queue: "recovery", TaskType: spec.task, Payload: taskEnvelope{Args: []any{}, Kwargs: map[string]any{}}, Priority: 5, MaxAttempts: 3, DedupeKey: "scheduled:" + spec.task})
		if err != nil {
			_ = scheduler.redis.Del(ctx, key).Err()
			return err
		}
	}
	return nil
}
