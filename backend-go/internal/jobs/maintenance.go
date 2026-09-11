package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	CompactResearchBacklogTask    = "market_loop.compact_research_backlog"
	ReprocessTargetImpactsTask    = "market_loop.reprocess_target_impacts_v2"
	ReplayRecentEventResearchTask = "market_loop.replay_recent_event_research_v6"
	SeedAssetsTask                = "market_loop.seed_assets"
	currentEventScoringVersion    = "llm-direction-v4"
	recentEventReplaySetting      = "maintenance:recent-event-research-v6"
)

type maintenanceRuntime struct {
	cfg   config.Config
	db    *pgxpool.Pool
	redis *redis.Client
}

type queuedResearchRun struct {
	ID        uuid.UUID
	AssetID   string
	CreatedAt time.Time
	Payload   map[string]any
}

type targetReplayCandidate struct {
	EventID     uuid.UUID
	Event       map[string]any
	RunID       *uuid.UUID
	RunStatus   string
	Run         map[string]any
	PublishedAt time.Time
}

func NewMaintenanceHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &maintenanceRuntime{cfg: cfg, db: db, redis: redisClient}
	return map[string]Handler{
		CompactResearchBacklogTask:    runtime.compactResearchBacklog,
		ReprocessTargetImpactsTask:    runtime.reprocessTargetImpacts,
		ReplayRecentEventResearchTask: runtime.replayRecentEventResearch,
		SeedAssetsTask:                runtime.seedAssets,
	}
}

type MaintenanceScheduler struct {
	db    *pgxpool.Pool
	store *Store
}

func NewMaintenanceScheduler(_ config.Config, db *pgxpool.Pool, _ *redis.Client) *MaintenanceScheduler {
	return &MaintenanceScheduler{db: db, store: NewStore(db)}
}

func (scheduler *MaintenanceScheduler) Enabled() bool { return scheduler.db != nil }

// Tick installs each versioned replay once. The durable setting keeps a
// scheduler or Redis restart from creating a second maintenance campaign.
func (scheduler *MaintenanceScheduler) Tick(ctx context.Context) error {
	if !scheduler.Enabled() {
		return nil
	}
	var body []byte
	err := scheduler.db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key=$1`, recentEventReplaySetting).Scan(&body)
	setting := map[string]any{}
	if err == nil {
		if err := json.Unmarshal(body, &setting); err != nil {
			return err
		}
		if stringValue(setting["status"]) == "completed" {
			return nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	} else {
		now := time.Now().UTC()
		setting = map[string]any{"version": eventResearchPromptVersion, "status": "queued", "cutoff_at": iso(now.Add(-72 * time.Hour)), "created_at": iso(now), "updated_at": iso(now)}
		encoded, _ := json.Marshal(setting)
		if _, err := scheduler.db.Exec(ctx, `INSERT INTO integration_settings(key,payload,updated_at) VALUES($1,$2,now()) ON CONFLICT(key) DO NOTHING`, recentEventReplaySetting, encoded); err != nil {
			return err
		}
	}
	_, err = scheduler.store.Enqueue(ctx, EnqueueParams{
		Queue: "maintenance", TaskType: ReplayRecentEventResearchTask,
		Payload:  taskEnvelope{Args: []any{}, Kwargs: map[string]any{"batch_size": 10, "max_active": 50}},
		Priority: 5, MaxAttempts: 3, DedupeKey: "maintenance:" + ReplayRecentEventResearchTask,
	})
	return err
}

func (runtime *maintenanceRuntime) replayRecentEventResearch(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, permanentJobError{err}
	}
	batchSize, maxActive := int(numberValue(envelope.Kwargs["batch_size"])), int(numberValue(envelope.Kwargs["max_active"]))
	if batchSize < 1 {
		batchSize = 10
	}
	if maxActive < 1 {
		maxActive = 50
	}
	setting, cutoff, err := runtime.recentEventReplayState(ctx)
	if err != nil {
		return nil, err
	}
	candidates, noSource, err := runtime.recentEventReplayCandidates(ctx, cutoff)
	if err != nil {
		return nil, err
	}
	active, err := runtime.activeRecentEventPipelines(ctx)
	if err != nil {
		return nil, err
	}
	campaignActive, err := runtime.activeRecentEventReplayCount(ctx, cutoff)
	if err != nil {
		return nil, err
	}
	// The production queue can legitimately contain a large pre-existing
	// backlog. Limit this versioned campaign itself instead of waiting for the
	// entire unrelated backlog to fall below the campaign ceiling.
	capacity := max(0, maxActive-campaignActive)
	selected := min(batchSize, min(capacity, len(candidates)))
	results, queued, failures := make([]any, 0, selected), 0, 0
	for _, candidate := range candidates[:selected] {
		result, queueErr := runtime.queueRecentEventReplay(ctx, candidate)
		if queueErr != nil {
			failures++
			results = append(results, map[string]any{"event_id": candidate.EventID, "status": "failed", "detail": queueErr.Error()})
			continue
		}
		results = append(results, result)
		if stringValue(objectValue(result)["status"]) == "queued" {
			queued++
		}
	}
	pending := len(candidates)
	complete := pending == 0 && campaignActive == 0
	setting["status"], setting["updated_at"] = ternaryString(complete, "completed", "running"), iso(time.Now())
	setting["pending"], setting["active"], setting["queued"], setting["failed"] = pending, campaignActive, queued, failures
	setting["skipped_no_source"], setting["scoring_version"] = noSource, currentEventScoringVersion
	if complete {
		setting["completed_at"] = iso(time.Now())
	}
	if err := runtime.saveRecentEventReplayState(ctx, setting); err != nil {
		return nil, err
	}
	summary := map[string]any{"version": eventResearchPromptVersion, "cutoff_at": iso(cutoff), "pending": pending, "active": campaignActive, "global_pipeline_active": active, "capacity": capacity, "selected": selected, "queued": queued, "failed": failures, "skipped_no_source": noSource, "complete": complete, "results": results}
	if complete {
		return summary, nil
	}
	return nil, &continuationError{Payload: taskEnvelope{Args: []any{}, Kwargs: map[string]any{"batch_size": batchSize, "max_active": maxActive}}, Progress: summary, Delay: 60 * time.Second}
}

func (runtime *maintenanceRuntime) recentEventReplayState(ctx context.Context) (map[string]any, time.Time, error) {
	var body []byte
	err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key=$1`, recentEventReplaySetting).Scan(&body)
	setting := map[string]any{}
	if errors.Is(err, pgx.ErrNoRows) {
		now := time.Now().UTC()
		setting = map[string]any{"version": eventResearchPromptVersion, "status": "running", "cutoff_at": iso(now.Add(-72 * time.Hour)), "created_at": iso(now)}
		if err := runtime.saveRecentEventReplayState(ctx, setting); err != nil {
			return nil, time.Time{}, err
		}
	} else if err != nil {
		return nil, time.Time{}, err
	} else if err := json.Unmarshal(body, &setting); err != nil {
		return nil, time.Time{}, err
	}
	cutoff := parseTime(setting["cutoff_at"])
	if cutoff.IsZero() {
		cutoff = time.Now().UTC().Add(-72 * time.Hour)
		setting["cutoff_at"] = iso(cutoff)
	}
	return setting, cutoff, nil
}

func (runtime *maintenanceRuntime) saveRecentEventReplayState(ctx context.Context, setting map[string]any) error {
	body, _ := json.Marshal(setting)
	_, err := runtime.db.Exec(ctx, `INSERT INTO integration_settings(key,payload,updated_at) VALUES($1,$2,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, recentEventReplaySetting, body)
	return err
}

func (runtime *maintenanceRuntime) recentEventReplayCandidates(ctx context.Context, cutoff time.Time) ([]targetReplayCandidate, int, error) {
	rows, err := runtime.db.Query(ctx, `SELECT e.id,e.published_at,e.payload::jsonb,r.id,r.status,r.payload::jsonb,
		EXISTS(SELECT 1 FROM jsonb_array_elements_text(coalesce(e.payload::jsonb->'news_item_ids','[]'::jsonb)) linked(news_id) JOIN news_items n ON n.id::text=linked.news_id) AS has_source
		FROM event_research_runs r JOIN news_events e ON e.id=r.event_id
		WHERE r.updated_at >= $1 AND r.status IN ('completed','insufficient_evidence','failed')
		ORDER BY r.updated_at,e.id`, cutoff)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	result, noSource := []targetReplayCandidate{}, 0
	for rows.Next() {
		var candidate targetReplayCandidate
		var eventBody, runBody []byte
		var hasSource bool
		if err := rows.Scan(&candidate.EventID, &candidate.PublishedAt, &eventBody, &candidate.RunID, &candidate.RunStatus, &runBody, &hasSource); err != nil {
			return nil, 0, err
		}
		if err := json.Unmarshal(eventBody, &candidate.Event); err != nil {
			return nil, 0, err
		}
		if err := json.Unmarshal(runBody, &candidate.Run); err != nil {
			return nil, 0, err
		}
		if eventRunHasCurrentScoring(candidate.Run) || recentEventReplayActive(candidate.Run) {
			continue
		}
		if !hasSource || candidate.Run["report"] == nil {
			noSource++
			continue
		}
		result = append(result, candidate)
	}
	return result, noSource, rows.Err()
}

func recentEventReplayActive(run map[string]any) bool {
	step := latestAnalysisStep(run, "full_event_research")
	status := stringValue(step["status"])
	return status == "queued" || status == "running" || status == "retrying"
}

func (runtime *maintenanceRuntime) activeRecentEventPipelines(ctx context.Context) (int, error) {
	var count int
	err := runtime.db.QueryRow(ctx, `SELECT (
		(SELECT count(*) FROM research_runs WHERE status IN ('queued','running','verifying')) +
		(SELECT count(*) FROM event_research_runs WHERE status IN ('queued','running','verifying'))
	)::int`).Scan(&count)
	return count, err
}

func (runtime *maintenanceRuntime) activeRecentEventReplayCount(ctx context.Context, cutoff time.Time) (int, error) {
	rows, err := runtime.db.Query(ctx, `SELECT r.status,r.payload::jsonb FROM event_research_runs r WHERE r.updated_at >= $1`, cutoff)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var status string
		var body []byte
		if err := rows.Scan(&status, &body); err != nil {
			return 0, err
		}
		run := map[string]any{}
		if json.Unmarshal(body, &run) != nil || eventRunHasCurrentScoring(run) {
			continue
		}
		step := latestAnalysisStep(run, "full_event_research")
		if stringValue(objectValue(step["metrics"])["maintenance_version"]) == eventResearchPromptVersion && (recentEventReplayActive(run) || activeResearchStatus(status)) {
			count++
		}
	}
	return count, rows.Err()
}

func (runtime *maintenanceRuntime) queueRecentEventReplay(ctx context.Context, candidate targetReplayCandidate) (any, error) {
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var body []byte
	var status string
	if err := tx.QueryRow(ctx, `SELECT status,payload::jsonb FROM event_research_runs WHERE id=$1 FOR UPDATE`, candidate.RunID).Scan(&status, &body); err != nil {
		return nil, err
	}
	run := map[string]any{}
	if err := json.Unmarshal(body, &run); err != nil {
		return nil, err
	}
	if eventRunHasCurrentScoring(run) {
		return map[string]any{"event_id": candidate.EventID, "status": "already_current"}, tx.Commit(ctx)
	}
	if activeResearchStatus(status) || recentEventReplayActive(run) {
		return map[string]any{"event_id": candidate.EventID, "status": "active"}, tx.Commit(ctx)
	}
	taskID := uuid.New()
	instanceID := (&ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis}).selectDownstreamInstance(ctx, "extract", len(runtime.cfg.ExtractURLs))
	appendAnalysisStep(run, analysisStep("full_event_research", "queued", "go-maintenance", "已创建三日关联研究 V6 的事件抽取、证券映射和研究重跑任务。", map[string]any{"stage": "event_extraction", "task_id": taskID.String(), "maintenance_version": eventResearchPromptVersion}))
	run["updated_at"] = iso(time.Now())
	updated, _ := json.Marshal(run)
	if _, err := tx.Exec(ctx, `UPDATE event_research_runs SET payload=$2,updated_at=now() WHERE id=$1`, candidate.RunID, updated); err != nil {
		return nil, err
	}
	jobBody, _ := json.Marshal(taskEnvelope{Args: []any{candidate.EventID.String(), candidate.RunID.String()}, Kwargs: map[string]any{"model_instance_id": instanceID, "source": "maintenance"}})
	if _, err := tx.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,dedupe_key,created_at,updated_at)
		VALUES($1,'extract',$2,$3,'queued',5,3,now(),$4,now(),now())`, taskID, reextractTask, jobBody, "event-research-v6-reextract:"+candidate.EventID.String()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	(&ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis}).recordModelTask(ctx, "extract", taskID.String(), "event_reextraction", candidate.EventID.String(), stringValue(candidate.Event["headline"]), "三日关联研究 V6 重跑", "maintenance", instanceID)
	return map[string]any{"event_id": candidate.EventID, "run_id": candidate.RunID, "task_id": taskID, "status": "queued"}, nil
}

func (runtime *maintenanceRuntime) compactResearchBacklog(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, permanentJobError{err}
	}
	dryRun := true
	if _, exists := envelope.Kwargs["dry_run"]; exists {
		dryRun = boolValue(envelope.Kwargs["dry_run"])
	}
	rows, err := runtime.db.Query(ctx, `SELECT id,asset_id,created_at,payload::jsonb
		FROM research_runs
		WHERE status='queued'
		  AND coalesce((payload->>'historical_replay')::boolean,false)=false
		  AND nullif(payload->>'retry_of_run_id','') IS NULL
		ORDER BY asset_id,created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grouped := map[string][]queuedResearchRun{}
	for rows.Next() {
		var run queuedResearchRun
		var body []byte
		if err := rows.Scan(&run.ID, &run.AssetID, &run.CreatedAt, &body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &run.Payload); err != nil {
			return nil, err
		}
		grouped[run.AssetID] = append(grouped[run.AssetID], run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	scanned, canonicalCount, coalescedCount := 0, 0, 0
	coalesceWindow := runtime.cfg.ResearchCoalesce
	if coalesceWindow <= 0 {
		coalesceWindow = 24 * time.Hour
	}
	for _, runs := range grouped {
		scanned += len(runs)
		var canonical *queuedResearchRun
		var windowEnd time.Time
		for index := range runs {
			run := &runs[index]
			if canonical == nil || run.CreatedAt.After(windowEnd) {
				canonical, windowEnd = run, run.CreatedAt.Add(coalesceWindow)
				canonicalCount++
				continue
			}
			coalescedCount++
			if dryRun {
				continue
			}
			merged, err := runtime.coalesceResearchRun(ctx, canonical, run)
			if err != nil {
				return nil, err
			}
			if !merged {
				coalescedCount--
			}
		}
	}
	return map[string]any{"scanned": scanned, "canonical": canonicalCount, "coalesced": coalescedCount}, nil
}

func (runtime *maintenanceRuntime) coalesceResearchRun(ctx context.Context, canonical, duplicate *queuedResearchRun) (bool, error) {
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var canonicalStatus, duplicateStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM research_runs WHERE id=$1 FOR UPDATE`, canonical.ID).Scan(&canonicalStatus); err != nil {
		return false, err
	}
	if err := tx.QueryRow(ctx, `SELECT status FROM research_runs WHERE id=$1 FOR UPDATE`, duplicate.ID).Scan(&duplicateStatus); err != nil {
		return false, err
	}
	if canonicalStatus != "queued" || duplicateStatus != "queued" {
		return false, tx.Commit(ctx)
	}
	triggerIDs := uniqueStrings(append(stringSlice(canonical.Payload["trigger_event_ids"]), stringSlice(duplicate.Payload["trigger_event_ids"])...))
	canonical.Payload["trigger_event_ids"], canonical.Payload["updated_at"] = triggerIDs, iso(time.Now())
	duplicate.Payload["status"], duplicate.Payload["coalesced_into_run_id"] = "coalesced", canonical.ID.String()
	duplicate.Payload["completed_at"], duplicate.Payload["updated_at"] = iso(time.Now()), iso(time.Now())
	appendAnalysisStep(duplicate.Payload, analysisStep("research_coalescing", "completed", "go-maintenance", fmt.Sprintf("该任务已合并到同标的主研究任务 %s。", canonical.ID), map[string]any{"canonical_run_id": canonical.ID.String()}))
	canonicalBody, _ := json.Marshal(canonical.Payload)
	duplicateBody, _ := json.Marshal(duplicate.Payload)
	if _, err := tx.Exec(ctx, `UPDATE research_runs SET payload=$2,updated_at=now() WHERE id=$1 AND status='queued'`, canonical.ID, canonicalBody); err != nil {
		return false, err
	}
	command, err := tx.Exec(ctx, `UPDATE research_runs SET status='coalesced',payload=$2,updated_at=now() WHERE id=$1 AND status='queued'`, duplicate.ID, duplicateBody)
	if err != nil {
		return false, err
	}
	if command.RowsAffected() != 1 {
		return false, tx.Commit(ctx)
	}
	return true, tx.Commit(ctx)
}

func (runtime *maintenanceRuntime) reprocessTargetImpacts(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, permanentJobError{err}
	}
	batchSize, maxActive := int(numberValue(envelope.Kwargs["batch_size"])), int(numberValue(envelope.Kwargs["max_active"]))
	if batchSize < 1 {
		batchSize = 25
	}
	if maxActive < 1 {
		maxActive = 50
	}
	candidates, err := runtime.targetReplayCandidates(ctx)
	if err != nil {
		return nil, err
	}
	pending, failures, active := 0, 0, 0
	available := make([]targetReplayCandidate, 0)
	for _, candidate := range candidates {
		if eventRunHasCurrentScoring(candidate.Run) {
			continue
		}
		pending++
		if candidate.RunStatus == "failed" {
			failures++
		}
		if activeResearchStatus(candidate.RunStatus) {
			active++
			continue
		}
		available = append(available, candidate)
	}
	capacity := max(0, maxActive-active)
	selected := min(batchSize, capacity)
	if selected > len(available) {
		selected = len(available)
	}
	results := make([]any, 0, selected)
	queued, queueFailures := 0, 0
	for _, candidate := range available[:selected] {
		result, queueErr := runtime.queueTargetImpactReplay(ctx, candidate)
		if queueErr != nil {
			queueFailures++
			results = append(results, map[string]any{"event_id": candidate.EventID, "status": "failed", "detail": queueErr.Error()})
			continue
		}
		results = append(results, result)
		if stringValue(objectValue(result)["status"]) == "queued" {
			queued++
		}
	}
	complete := pending == 0 && failures == 0
	summary := map[string]any{
		"dry_run": false, "scoring_version": currentEventScoringVersion, "pending": pending,
		"failed": failures, "active": active, "capacity": capacity, "selected": selected,
		"queued": queued, "queue_failures": queueFailures, "complete": complete, "results": results,
	}
	if complete {
		return summary, nil
	}
	return nil, &continuationError{
		Payload:  taskEnvelope{Args: []any{}, Kwargs: map[string]any{"batch_size": batchSize, "max_active": maxActive}},
		Progress: summary, Delay: 60 * time.Second,
	}
}

func (runtime *maintenanceRuntime) targetReplayCandidates(ctx context.Context) ([]targetReplayCandidate, error) {
	rows, err := runtime.db.Query(ctx, `SELECT e.id,e.published_at,e.payload::jsonb,r.id,r.status,coalesce(r.payload,'{}'::json)::jsonb
		FROM news_events e LEFT JOIN event_research_runs r ON r.event_id=e.id
		ORDER BY e.published_at,e.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]targetReplayCandidate, 0)
	for rows.Next() {
		var candidate targetReplayCandidate
		var runStatus *string
		var eventBody, runBody []byte
		if err := rows.Scan(&candidate.EventID, &candidate.PublishedAt, &eventBody, &candidate.RunID, &runStatus, &runBody); err != nil {
			return nil, err
		}
		if runStatus != nil {
			candidate.RunStatus = *runStatus
		}
		if err := json.Unmarshal(eventBody, &candidate.Event); err != nil {
			return nil, err
		}
		if candidate.RunID != nil {
			if err := json.Unmarshal(runBody, &candidate.Run); err != nil {
				return nil, err
			}
		}
		result = append(result, candidate)
	}
	return result, rows.Err()
}

func eventRunHasCurrentScoring(run map[string]any) bool {
	report := objectValue(run["report"])
	return stringValue(report["scoring_version"]) == currentEventScoringVersion &&
		stringValue(report["prompt_version"]) == eventResearchPromptVersion &&
		stringValue(report["target_evaluation_version"]) == targetEvaluationVersion &&
		stringValue(report["news_confidence_version"]) == newsConfidenceVersion &&
		stringValue(report["report_confidence_version"]) == reportConfidenceVersion
}

func activeResearchStatus(status string) bool {
	return status == "queued" || status == "running" || status == "verifying"
}

func (runtime *maintenanceRuntime) queueTargetImpactReplay(ctx context.Context, candidate targetReplayCandidate) (any, error) {
	event := candidate.Event
	if len(anySlice(event["actions"])) == 0 {
		shared := (&researchRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis}).shared()
		for _, rawID := range stringSlice(event["news_item_ids"]) {
			newsID, err := uuid.Parse(rawID)
			if err != nil {
				continue
			}
			news, err := shared.loadNews(ctx, newsID)
			if err == nil {
				event["actions"] = fallbackActions(news)
				if err := shared.saveEvent(ctx, event); err != nil {
					return nil, err
				}
				break
			}
		}
	}

	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var runID uuid.UUID
	var run map[string]any
	var body []byte
	err = tx.QueryRow(ctx, `SELECT id,payload::jsonb FROM event_research_runs WHERE event_id=$1 FOR UPDATE`, candidate.EventID).Scan(&runID, &body)
	runExists := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		runID, run = uuid.New(), map[string]any{
			"id": nil, "event_id": candidate.EventID.String(), "report_history": []any{}, "analysis_steps": []any{},
			"created_at": iso(time.Now()), "evidence": []any{}, "missing_requirements": []any{}, "contradictions": []any{},
		}
		run["id"] = runID.String()
	} else if err != nil {
		return nil, err
	} else if err := json.Unmarshal(body, &run); err != nil {
		return nil, err
	}
	if eventRunHasCurrentScoring(run) {
		return map[string]any{"event_id": candidate.EventID, "status": "already_current"}, tx.Commit(ctx)
	}
	if activeResearchStatus(stringValue(run["status"])) {
		return map[string]any{"event_id": candidate.EventID, "status": "active"}, tx.Commit(ctx)
	}
	if report := run["report"]; report != nil {
		run["report_history"] = append(anySlice(run["report_history"]), report)
	}
	taskID := uuid.New()
	shared := &ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis}
	instanceID := shared.selectDownstreamInstance(ctx, "research", len(runtime.cfg.ResearchURLs))
	profile, routeReason, matchedKeywords := eventResearchProfile(event, false)
	now := time.Now().UTC()
	run["report"], run["status"], run["as_of"] = nil, "queued", event["as_of"]
	run["historical_replay"], run["verification_round"], run["retry_count"] = true, 0, 0
	run["celery_task_id"], run["model_instance_id"] = taskID.String(), instanceID
	run["research_profile"], run["route_reason"], run["matched_whitelist_keywords"] = profile, routeReason, matchedKeywords
	run["escalated_to_deep"], run["waiting_for_deep_slot"] = false, false
	run["retryable_reason"], run["error"] = nil, nil
	run["missing_requirements"], run["contradictions"], run["evidence"] = []any{}, []any{}, []any{}
	run["updated_at"] = iso(now)
	appendAnalysisStep(run, analysisStep("target_impact_v2_replay", "queued", "go-maintenance", "已按原 as_of 创建点时逐目标新版后继运行；实时搜索和当前基本面已禁用。", map[string]any{"historical_replay": true, "as_of": event["as_of"], "archived_report_count": len(anySlice(run["report_history"])), "scoring_version": currentEventScoringVersion}))
	runBody, _ := json.Marshal(run)
	if !runExists {
		if _, err := tx.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'queued',$3,$4,$4)`, runID, candidate.EventID, runBody, now); err != nil {
			return nil, err
		}
	} else if _, err := tx.Exec(ctx, `UPDATE event_research_runs SET status='queued',payload=$2,updated_at=now() WHERE id=$1`, runID, runBody); err != nil {
		return nil, err
	}
	jobBody, _ := json.Marshal(taskEnvelope{Args: []any{candidate.EventID.String(), runID.String()}, Kwargs: map[string]any{"model_instance_id": instanceID, "research_profile": profile, "route_reason": routeReason, "matched_whitelist_keywords": matchedKeywords}})
	if _, err := tx.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,dedupe_key,created_at,updated_at)
		VALUES($1,'research',$2,$3,'queued',1,3,now(),$4,now(),now())`, taskID, researchEventTask, jobBody, "research-run:"+runID.String()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	(&researchRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis}).recordResearchTask(ctx, taskID.String(), "event_research", runID.String(), stringValue(event["headline"]), stringValue(event["event_type"]), instanceID)
	return map[string]any{"event_id": candidate.EventID, "run_id": runID, "task_id": taskID, "status": "queued"}, nil
}

func (runtime *maintenanceRuntime) seedAssets(ctx context.Context, _ Job) (any, error) {
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	for _, asset := range curatedSeedAssets() {
		aliases, _ := json.Marshal(asset.Aliases)
		products, _ := json.Marshal(asset.Products)
		competitors, _ := json.Marshal(asset.Competitors)
		_, err := tx.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,sector_id,industry_id,raw_sector,raw_industry,instrument_type,association_tier,association_reason,provider_association_tier,provider_association_reason,last_synced_at,issuer_id,primary_listing_asset_id,lot_size,active)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'standard','provider_verified','standard','provider_verified',now(),$16,$17,$18,true)
			ON CONFLICT(id) DO NOTHING`, asset.ID, asset.Class, asset.Market, asset.Symbol, asset.Name, asset.Exchange, asset.Currency, aliases, products, competitors, asset.Sector, asset.Industry, asset.RawSector, asset.RawIndustry, asset.Instrument, nullableMasterString(asset.IssuerID), nullableMasterString(asset.PrimaryListingID), max(1, asset.LotSize))
		if err != nil {
			return nil, err
		}
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*)::int FROM assets`).Scan(&count); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"assets": count}, nil
}

func curatedSeedAssets() []masterAsset {
	assets := []masterAsset{
		{ID: "equity:XNAS:AAPL", Class: "equity", Market: "US", Symbol: "AAPL", Name: "Apple Inc.", Exchange: "XNAS", Currency: "USD", Aliases: []string{"Apple", "苹果公司"}, Products: []string{"iPhone", "Mac", "Services"}, Competitors: []string{"MSFT", "GOOGL", "SAMSUNG"}, Sector: "sector:information_technology", Industry: "industry:hardware", RawSector: "Information Technology", RawIndustry: "Technology Hardware", Instrument: "common_stock", LotSize: 1},
		{ID: "equity:XSHG:600519", Class: "equity", Market: "CN", Symbol: "600519", Name: "贵州茅台", Exchange: "XSHG", Currency: "CNY", Aliases: []string{"茅台", "Kweichow Moutai"}, Sector: "sector:consumer_staples", Industry: "industry:food_beverage", RawSector: "必选消费", RawIndustry: "食品饮料", Instrument: "common_stock", LotSize: 100},
		{ID: "equity:XHKG:00700", Class: "equity", Market: "HK", Symbol: "00700", Name: "腾讯控股", Exchange: "XHKG", Currency: "HKD", Aliases: []string{"腾讯", "Tencent"}, Products: []string{"微信", "游戏", "云服务"}, Competitors: []string{"9988", "NTES"}, Sector: "sector:communication_services", Industry: "industry:internet", RawSector: "Communication Services", RawIndustry: "Internet Services", Instrument: "common_stock", LotSize: 100},
		{ID: "crypto:coingecko:bitcoin", Class: "crypto", Market: "CRYPTO", Symbol: "BTC", Name: "Bitcoin", Exchange: "coingecko", Currency: "USD", Aliases: []string{"bitcoin", "比特币"}, Sector: "sector:digital_assets", Industry: "industry:cryptocurrency", Instrument: "crypto", LotSize: 1},
		{ID: "crypto:coingecko:ethereum", Class: "crypto", Market: "CRYPTO", Symbol: "ETH", Name: "Ethereum", Exchange: "coingecko", Currency: "USD", Aliases: []string{"ethereum", "以太坊"}, Sector: "sector:digital_assets", Industry: "industry:cryptocurrency", Instrument: "crypto", LotSize: 1},
		{ID: "equity:XHKG:09988", Class: "equity", Market: "HK", Symbol: "09988", Name: "Alibaba Group Holding Limited", Exchange: "XHKG", Currency: "HKD", Aliases: []string{"阿里巴巴", "阿里巴巴集团", "Alibaba", "Alibaba Group"}, Products: []string{"阿里云", "Alibaba Cloud"}, IssuerID: "curated:alibaba-group", Sector: "sector:communication_services", Industry: "industry:internet", RawSector: "Communication Services", RawIndustry: "Internet Services", Instrument: "common_stock", LotSize: 100},
		{ID: "equity:NYSE:BABA", Class: "equity", Market: "US", Symbol: "BABA", Name: "Alibaba Group Holding Limited", Exchange: "NYSE", Currency: "USD", Aliases: []string{"阿里巴巴", "阿里巴巴集团", "Alibaba", "Alibaba Group"}, Products: []string{"阿里云", "Alibaba Cloud"}, IssuerID: "curated:alibaba-group", Sector: "sector:communication_services", Industry: "industry:internet", RawSector: "Communication Services", RawIndustry: "Internet Services", Instrument: "adr", LotSize: 1},
		{ID: "commodity:fmp:CLUSD", Class: "commodity", Market: "COMMODITY", Symbol: "CLUSD", Name: "WTI Crude Oil Continuous Benchmark", Exchange: "fmp", Currency: "USD", Aliases: []string{"WTI", "WTI crude", "West Texas Intermediate", "WTI 原油"}, LotSize: 1},
		{ID: "commodity:fmp:BZUSD", Class: "commodity", Market: "COMMODITY", Symbol: "BZUSD", Name: "Brent Crude Oil Continuous Benchmark", Exchange: "fmp", Currency: "USD", Aliases: []string{"Brent", "Brent crude", "布伦特原油"}, LotSize: 1},
		{ID: "commodity:fmp:ZGUSD", Class: "commodity", Market: "COMMODITY", Symbol: "ZGUSD", Name: "Gold Continuous Benchmark", Exchange: "fmp", Currency: "USD", Aliases: []string{"Gold", "黄金", "现货黄金"}, LotSize: 1},
		{ID: "fx:fmp:EURUSD", Class: "fx", Market: "FX", Symbol: "EURUSD", Name: "EUR/USD Spot FX", Exchange: "fmp", Currency: "USD", Aliases: []string{"EUR/USD", "欧元兑美元"}, LotSize: 1},
		{ID: "fx:fmp:USDJPY", Class: "fx", Market: "FX", Symbol: "USDJPY", Name: "USD/JPY Spot FX", Exchange: "fmp", Currency: "USD", Aliases: []string{"USD/JPY", "美元兑日元"}, LotSize: 1},
		{ID: "fx:fmp:USDCNH", Class: "fx", Market: "FX", Symbol: "USDCNH", Name: "USD/CNH Spot FX", Exchange: "fmp", Currency: "USD", Aliases: []string{"USD/CNH", "美元兑离岸人民币"}, LotSize: 1},
		{ID: "equity:OTC:EADSY", Class: "equity", Market: "US", Symbol: "EADSY", Name: "Airbus SE Sponsored ADR", Exchange: "OTC", Currency: "USD", Aliases: []string{"Airbus", "Airbus SE", "空客", "空中客车"}, IssuerID: "curated:airbus-se", PrimaryListingID: "equity:XPAR:AIR.PA", Sector: "sector:industrials", Industry: "industry:aerospace_defense", RawSector: "Industrials", RawIndustry: "Aerospace & Defense", Instrument: "adr", LotSize: 1},
	}
	for index := range assets {
		assets[index].AssociationTier, assets[index].AssociationReason, assets[index].Active = "standard", "provider_verified", true
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].ID < assets[j].ID })
	return assets
}
