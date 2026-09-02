package httpapi

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	modelQueueTaskRetention       = 24 * time.Hour
	modelQueueRecentExecutionTime = 4 * time.Hour
)

type nativeModelQueueSpec struct {
	id      string
	model   string
	purpose string
	binding string
	enabled bool
}

type nativeModelJob struct {
	ID          string
	TaskType    string
	Status      string
	Attempt     int
	Error       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	AttemptAt   *time.Time
	CompletedAt *time.Time
	ExecutionMS int64
	Kind        string
	EntityID    string
	Title       string
	Subtitle    string
	Source      string
	InstanceID  string
}

type nativeModelQueueResult struct {
	index int
	queue map[string]any
}

type nativeExecutionSamples struct {
	all        []int64
	byInstance map[string][]int64
}

func (s *Server) buildNativeModelQueueOverview(ctx context.Context, limit int) map[string]any {
	now := time.Now().UTC()
	specs := []nativeModelQueueSpec{
		{id: "extract", model: s.cfg.ExtractModel, purpose: "新闻抽取", binding: "新闻事件结构化抽取", enabled: true},
		{id: "research", model: s.cfg.ResearchModel, purpose: "标的研究", binding: "工具深度研究与逐目标事件研报", enabled: true},
		{id: "assist", model: s.cfg.AssistModel, purpose: "股票映射", binding: "新闻事件二次股票映射", enabled: true},
		{id: "code", model: s.cfg.CodeModel, purpose: "代码演进", binding: evolutionBinding(s.cfg.EvolutionAutoMerge), enabled: s.cfg.EvolutionEnabled},
	}
	results := make(chan nativeModelQueueResult, len(specs))
	for index, spec := range specs {
		go func(index int, spec nativeModelQueueSpec) {
			inference := s.inferenceQueue(ctx, spec.id, spec.model, spec.purpose, spec.binding, spec.enabled)
			jobs, err := s.loadNativeModelJobs(ctx, spec.id, now.Add(-modelQueueTaskRetention))
			if err != nil {
				slog.Error("native model queue query failed", "queue", spec.id, "error", err)
			}
			executionSamples, auditErr := s.loadRecentModelExecutionSamples(ctx, spec.id, now.Add(-modelQueueRecentExecutionTime))
			if auditErr != nil {
				slog.Error("native model execution metrics query failed", "queue", spec.id, "error", auditErr)
			}
			results <- nativeModelQueueResult{index: index, queue: nativeModelQueueItem(spec, inference, jobs, executionSamples, limit, now, err)}
		}(index, spec)
	}
	queues := make([]any, len(specs))
	for range specs {
		result := <-results
		queues[result.index] = result.queue
	}
	return map[string]any{"generated_at": now, "producer": "go-api", "queues": queues}
}

// loadRecentModelExecutionSamples uses the model audit log as the authority for
// actual inference time. Some queue jobs are lifecycle wrappers and can finish
// without a meaningful started_at/execution_duration_ms value.
func (s *Server) loadRecentModelExecutionSamples(ctx context.Context, queue string, cutoff time.Time) (nativeExecutionSamples, error) {
	rows, err := s.db.Query(ctx, `
		SELECT coalesce(nullif(metrics->>'endpoint',''),$1||'-0'),duration_ms
		FROM model_call_audits
		WHERE provider='ollama'
		  AND metrics->>'lane'=$1
		  AND status IN ('completed','failed')
		  AND completed_at >= $2
		  AND duration_ms IS NOT NULL
		ORDER BY completed_at`, queue, cutoff)
	if err != nil {
		return nativeExecutionSamples{}, err
	}
	defer rows.Close()
	samples := nativeExecutionSamples{byInstance: map[string][]int64{}}
	for rows.Next() {
		var instanceID string
		var duration int64
		if err := rows.Scan(&instanceID, &duration); err != nil {
			return nativeExecutionSamples{}, err
		}
		duration = max(int64(0), duration)
		samples.all = append(samples.all, duration)
		samples.byInstance[instanceID] = append(samples.byInstance[instanceID], duration)
	}
	return samples, rows.Err()
}

// loadNativeModelJobs uses go_jobs as the lifecycle authority. The joins are
// deliberately done in one query so that a large research backlog does not
// degrade into one headline lookup per card.
func (s *Server) loadNativeModelJobs(ctx context.Context, queue string, cutoff time.Time) ([]nativeModelJob, error) {
	rows, err := s.db.Query(ctx, `
		SELECT j.id::text,j.task_type,j.status,j.attempt,
		       coalesce(j.error,''),j.created_at,j.updated_at,j.started_at,j.attempt_started_at,
		       j.completed_at,j.execution_duration_ms,
		       CASE
		         WHEN j.task_type='market_loop.research_event' THEN 'event_research'
		         WHEN j.task_type='market_loop.research_asset' THEN 'asset_research'
		         WHEN j.queue='extract' THEN 'news_extraction'
		         WHEN j.queue='assist' THEN 'asset_mapping'
		         WHEN j.queue='code' THEN 'code_evolution'
		         ELSE j.task_type
		       END AS kind,
		       CASE
		         WHEN j.task_type='market_loop.research_event' THEN coalesce(er.event_id::text,j.payload->'args'->>0)
		         WHEN j.task_type='market_loop.research_asset' THEN coalesce(rr.asset_id,j.payload->'args'->>0)
		         ELSE j.payload->'args'->>0
		       END AS entity_id,
		       CASE
		         WHEN j.task_type='market_loop.research_event' THEN coalesce(nullif(event_news.headline,''),nullif(event_news.payload->>'headline',''),'事件研究')
		         WHEN j.task_type='market_loop.research_asset' THEN coalesce(
		           nullif(concat_ws(' · ',nullif(rr.payload#>>'{asset,symbol}',''),nullif(rr.payload#>>'{asset,name}','')),''),
		           nullif(rr.asset_id,''),nullif(j.payload->'args'->>0,''),'标的研究')
		         WHEN j.queue='extract' THEN coalesce(nullif(ni.title,''),'新闻抽取')
		         WHEN j.queue='assist' THEN coalesce(nullif(mapping_event.headline,''),nullif(mapping_event.payload->>'headline',''),'事件股票映射')
		         WHEN j.queue='code' THEN coalesce(nullif(ec.payload->>'title',''),nullif(ec.payload->>'goal',''),'代码演进')
		         ELSE j.task_type
		       END AS title,
		       CASE
		         WHEN j.task_type='market_loop.research_event' THEN coalesce(nullif(event_news.event_type,''),'逐目标事件研报')
		         WHEN j.task_type='market_loop.research_asset' THEN coalesce(
		           nullif(concat_ws(' · ',nullif(rr.payload#>>'{asset,market}',''),nullif(rr.payload#>>'{asset,asset_class}','')),''),
		           '工具深度研究')
		         WHEN j.queue='extract' THEN coalesce(nullif(ni.source,''),'新闻事件')
		         WHEN j.queue='assist' THEN coalesce(nullif(mapping_event.event_type,''),'新闻事件二次股票映射')
		         WHEN j.queue='code' THEN coalesce(nullif(ec.status,''),'失败案例驱动')
		         ELSE ''
		       END AS subtitle,
		       CASE
		         WHEN j.queue='extract' THEN nullif(ni.source,'')
		         ELSE nullif(j.payload#>>'{kwargs,source}','')
		       END AS source,
		       coalesce(
		         nullif(er.payload->>'model_instance_id',''),
		         nullif(rr.payload->>'model_instance_id',''),
		         nullif(j.payload#>>'{kwargs,model_instance_id}','')
		       ) AS instance_id
		FROM go_jobs j
		LEFT JOIN event_research_runs er
		  ON j.task_type='market_loop.research_event' AND er.id=j.payload->'args'->>1
		LEFT JOIN news_events event_news
		  ON j.task_type='market_loop.research_event' AND event_news.id=j.payload->'args'->>0
		LEFT JOIN research_runs rr
		  ON j.task_type='market_loop.research_asset' AND rr.id=j.payload->'args'->>2
		LEFT JOIN news_items ni
		  ON j.queue='extract' AND ni.id=j.payload->'args'->>0
		LEFT JOIN news_events mapping_event
		  ON j.queue='assist' AND mapping_event.id=j.payload->'args'->>0
		LEFT JOIN evolution_candidates ec
		  ON j.queue='code' AND ec.id=j.payload->'args'->>0
		WHERE j.queue=$1
		  AND (j.status IN ('queued','running','retrying') OR j.updated_at >= $2)
		ORDER BY CASE j.status WHEN 'running' THEN 0 WHEN 'retrying' THEN 1 WHEN 'queued' THEN 2 WHEN 'failed' THEN 3 ELSE 4 END,
		         j.updated_at DESC,j.id DESC`, queue, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]nativeModelJob, 0)
	for rows.Next() {
		var job nativeModelJob
		var entityID, title, subtitle, source, instanceID *string
		if err := rows.Scan(
			&job.ID, &job.TaskType, &job.Status, &job.Attempt, &job.Error,
			&job.CreatedAt, &job.UpdatedAt, &job.StartedAt, &job.AttemptAt,
			&job.CompletedAt, &job.ExecutionMS, &job.Kind,
			&entityID, &title, &subtitle, &source, &instanceID,
		); err != nil {
			return nil, err
		}
		job.EntityID = stringPointerValue(entityID)
		job.Title = stringPointerValue(title)
		job.Subtitle = stringPointerValue(subtitle)
		job.Source = stringPointerValue(source)
		job.InstanceID = stringPointerValue(instanceID)
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func nativeModelQueueItem(spec nativeModelQueueSpec, inference map[string]any, jobs []nativeModelJob, executionSamples nativeExecutionSamples, limit int, now time.Time, queryErr error) map[string]any {
	rawInstances := nativeInferenceInstances(inference["instances"])
	instanceIDs := make([]string, 0, len(rawInstances))
	for _, raw := range rawInstances {
		if instance, ok := raw.(map[string]any); ok && stringValue(instance["id"]) != "" {
			instanceIDs = append(instanceIDs, stringValue(instance["id"]))
		}
	}
	for index := range jobs {
		if !contains(instanceIDs, jobs[index].InstanceID) {
			jobs[index].InstanceID = nativeTaskInstance(jobs[index].ID, instanceIDs)
		}
	}
	counts := nativeQueueCounts(jobs)
	counts["waiting_for_model"] = int64Value(inference["queued"])
	visible := nativeVisibleTasks(spec.id, jobs, now)
	instances := make([]any, 0, len(rawInstances))
	for _, raw := range rawInstances {
		base, _ := raw.(map[string]any)
		instanceID := stringValue(base["id"])
		instanceJobs := nativeJobsForInstance(jobs, instanceID)
		instanceCounts := nativeQueueCounts(instanceJobs)
		instanceCounts["waiting_for_model"] = int64Value(base["queued"])
		instanceVisible := nativeVisibleTasks(spec.id, instanceJobs, now)
		instanceLimit := min(limit, len(instanceVisible))
		available := max(0, int(int64Value(base["capacity"])-instanceCounts["running"]))
		observable := boolValue(base["observable"])
		healthy := boolValue(base["healthy"])
		modelAvailable := boolValue(base["model_available"])
		metrics := nativeQueueMetrics(instanceJobs, instanceCounts, now)
		nativeApplyExecutionSamples(metrics, executionSamples.byInstance[instanceID])
		instances = append(instances, map[string]any{
			"id": instanceID, "healthy": healthy, "model_available": modelAvailable,
			"state":    nativeQueueState(instanceCounts, spec.enabled, observable && healthy && modelAvailable),
			"capacity": int64Value(base["capacity"]), "available": available, "observable": observable,
			"counts": instanceCounts, "metrics": metrics,
			"total_tasks": nativeCountedTasks(instanceCounts), "truncated": len(instanceVisible) > limit,
			"tasks": nativeTaskValues(instanceVisible[:instanceLimit], now),
		})
	}
	// Tasks with no inferencer are still represented in aggregate counts. Put
	// their count on the first instance so panel totals stay consistent.
	if len(instances) > 0 {
		first := instances[0].(map[string]any)
		firstCounts := first["counts"].(map[string]int64)
		for _, field := range []string{"queued", "running", "retrying", "verifying", "completed", "failed"} {
			var assigned int64
			for _, raw := range instances {
				assigned += raw.(map[string]any)["counts"].(map[string]int64)[field]
			}
			if counts[field] > assigned {
				firstCounts[field] += counts[field] - assigned
			}
		}
		first["total_tasks"] = nativeCountedTasks(firstCounts)
	}
	available := max(0, int(int64Value(inference["capacity"])-counts["running"]))
	visibleLimit := min(limit, len(visible))
	var errorValue any
	if queryErr != nil {
		errorValue = "模型队列任务状态暂时不可用。"
	}
	metrics := nativeQueueMetrics(jobs, counts, now)
	nativeApplyExecutionSamples(metrics, executionSamples.all)
	return map[string]any{
		"id": spec.id, "model": spec.model, "purpose": spec.purpose, "binding": spec.binding,
		"enabled": spec.enabled, "state": nativeQueueState(counts, spec.enabled, boolValue(inference["observable"])),
		"threads": modelThreads(spec.id), "capacity": int64Value(inference["capacity"]), "available": available,
		"instance_count": len(instanceIDs), "per_instance_concurrency": int64Value(inference["per_instance_concurrency"]),
		"observable": boolValue(inference["observable"]), "instances": instances,
		"counts": counts, "metrics": metrics, "total_tasks": nativeCountedTasks(counts),
		"truncated": len(visible) > limit, "tasks": nativeTaskValues(visible[:visibleLimit], now), "error": errorValue,
	}
}

func nativeApplyExecutionSamples(metrics map[string]any, samples []int64) {
	if len(samples) == 0 {
		return
	}
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	metrics["average_execution_duration_ms"] = averageInt64(ordered)
	metrics["execution_duration_sample_count"] = len(ordered)
	metrics["execution_p50_ms"] = nativePercentile(ordered, 0.5)
	metrics["execution_p90_ms"] = nativePercentile(ordered, 0.9)
}

func nativeInferenceInstances(value any) []any {
	if values, ok := value.([]map[string]any); ok {
		result := make([]any, len(values))
		for index := range values {
			result[index] = values[index]
		}
		return result
	}
	return anySlice(value)
}

func nativeQueueCounts(jobs []nativeModelJob) map[string]int64 {
	counts := map[string]int64{"queued": 0, "running": 0, "retrying": 0, "verifying": 0, "waiting_for_model": 0, "completed": 0, "failed": 0}
	for _, job := range jobs {
		switch job.Status {
		case "queued", "proposed":
			counts["queued"]++
		case "running", "generating", "testing", "merging":
			counts["running"]++
		case "retrying":
			counts["retrying"]++
		case "verifying":
			counts["verifying"]++
		case "completed", "merged", "unmapped", "insufficient_evidence":
			counts["completed"]++
		case "failed", "rejected", "rolled_back":
			counts["failed"]++
		}
	}
	return counts
}

func nativeQueueState(counts map[string]int64, enabled, observable bool) string {
	if !enabled {
		return "disabled"
	}
	if !observable {
		return "unavailable"
	}
	if counts["running"] > 0 || counts["verifying"] > 0 {
		return "running"
	}
	if counts["queued"] > 0 || counts["retrying"] > 0 || counts["waiting_for_model"] > 0 {
		return "queued"
	}
	if counts["failed"] > 0 {
		return "failed"
	}
	return "idle"
}

func nativeVisibleTasks(queue string, jobs []nativeModelJob, now time.Time) []nativeModelJob {
	visible := make([]nativeModelJob, 0)
	for _, job := range jobs {
		if nativeActiveStatus(job.Status) || nativeFailedStatus(job.Status) {
			visible = append(visible, job)
		}
	}
	sort.SliceStable(visible, func(i, j int) bool {
		left, right := nativeStatusRank(visible[i].Status), nativeStatusRank(visible[j].Status)
		if left != right {
			return left < right
		}
		return visible[i].UpdatedAt.After(visible[j].UpdatedAt)
	})
	if queue != "research" {
		return visible
	}
	result := make([]nativeModelJob, 0, len(visible))
	seen := map[string]bool{}
	for _, job := range visible {
		key := job.Kind + ":" + job.EntityID
		if job.EntityID == "" {
			key = job.Kind + ":title:" + strings.ToLower(strings.TrimSpace(job.Title))
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, job)
	}
	return result
}

func nativeTaskValues(jobs []nativeModelJob, now time.Time) []any {
	values := make([]any, 0, len(jobs))
	for _, job := range jobs {
		var completedAt any
		if job.CompletedAt != nil {
			completedAt = job.CompletedAt.UTC()
		}
		var queueDuration any
		if job.Status == "queued" || job.Status == "retrying" {
			queueDuration = max(int64(0), now.Sub(job.CreatedAt).Milliseconds())
		}
		var errorValue any
		if trimmed := strings.TrimSpace(job.Error); trimmed != "" {
			if len(trimmed) > 500 {
				trimmed = trimmed[:500]
			}
			errorValue = trimmed
		}
		var startedAt any
		if job.StartedAt != nil {
			startedAt = job.StartedAt.UTC()
		}
		var executionDuration any
		if job.StartedAt != nil {
			executionDuration = nativeExecutionDuration(job, now)
		}
		values = append(values, map[string]any{
			"task_id": job.ID, "instance_id": nilIfEmpty(job.InstanceID), "kind": job.Kind,
			"entity_id": nilIfEmpty(job.EntityID), "title": job.Title, "subtitle": job.Subtitle,
			"source": nilIfEmpty(job.Source), "status": job.Status, "attempt": max(1, job.Attempt), "task_count": 1,
			"queued_at": job.CreatedAt.UTC(), "started_at": startedAt, "completed_at": completedAt,
			"updated_at": job.UpdatedAt.UTC(), "queue_duration_ms": queueDuration,
			"execution_duration_ms": executionDuration, "error": errorValue, "metrics": map[string]any{},
		})
	}
	return values
}

func nativeQueueMetrics(jobs []nativeModelJob, counts map[string]int64, now time.Time) map[string]any {
	metrics := emptyQueueMetrics()
	waits := make([]int64, 0)
	executionDurations := make([]int64, 0)
	completedTimes := make([]time.Time, 0)
	var singleCompletedExecution int64
	recentCutoff := now.Add(-modelQueueRecentExecutionTime)
	var longest int64
	for _, job := range jobs {
		if job.Status == "queued" || job.Status == "retrying" {
			wait := max(int64(0), now.Sub(job.CreatedAt).Milliseconds())
			waits = append(waits, wait)
			longest = max(longest, wait)
		}
		terminalAt := job.CompletedAt
		if terminalAt == nil {
			terminalAt = &job.UpdatedAt
		}
		if job.StartedAt != nil && nativeTerminalStatus(job.Status) && !terminalAt.Before(recentCutoff) {
			executionDurations = append(executionDurations, nativeExecutionDuration(job, now))
		}
		if nativeCompletedStatus(job.Status) && job.CompletedAt != nil && !job.CompletedAt.Before(recentCutoff) {
			completedTimes = append(completedTimes, job.CompletedAt.UTC())
			singleCompletedExecution = nativeExecutionDuration(job, now)
		}
	}
	if len(waits) > 0 {
		metrics["average_queue_duration_ms"] = averageInt64(waits)
		metrics["longest_wait_ms"] = longest
		metrics["queue_duration_sample_count"] = len(waits)
	}
	if len(executionDurations) > 0 {
		sort.Slice(executionDurations, func(i, j int) bool { return executionDurations[i] < executionDurations[j] })
		metrics["average_execution_duration_ms"] = averageInt64(executionDurations)
		metrics["execution_duration_sample_count"] = len(executionDurations)
		metrics["execution_p50_ms"] = nativePercentile(executionDurations, 0.5)
		metrics["execution_p90_ms"] = nativePercentile(executionDurations, 0.9)
	}
	sort.Slice(completedTimes, func(i, j int) bool { return completedTimes[i].Before(completedTimes[j]) })
	var throughput float64
	if len(completedTimes) >= 2 {
		elapsedHours := completedTimes[len(completedTimes)-1].Sub(completedTimes[0]).Hours()
		if elapsedHours > 0 {
			throughput = float64(len(completedTimes)-1) / elapsedHours
		}
	} else if len(completedTimes) == 1 && singleCompletedExecution > 0 {
		throughput = float64(time.Hour.Milliseconds()) / float64(singleCompletedExecution)
	}
	if throughput > 0 {
		metrics["throughput_per_hour"] = throughput
		work := counts["queued"] + counts["retrying"] + counts["running"] + counts["verifying"]
		if work > 0 {
			metrics["estimated_clear_ms"] = int64(math.Ceil(float64(work) / throughput * float64(time.Hour.Milliseconds())))
		}
	}
	return metrics
}

func nativeExecutionDuration(job nativeModelJob, now time.Time) int64 {
	duration := max(int64(0), job.ExecutionMS)
	if job.Status == "running" && job.AttemptAt != nil {
		duration += max(int64(0), now.Sub(*job.AttemptAt).Milliseconds())
	}
	return duration
}

func nativePercentile(ordered []int64, ratio float64) int64 {
	index := int(math.Ceil(float64(len(ordered))*ratio)) - 1
	return ordered[max(0, min(index, len(ordered)-1))]
}

func nativeCompletedStatus(status string) bool {
	return status == "completed" || status == "merged" || status == "unmapped" || status == "insufficient_evidence"
}

func nativeTerminalStatus(status string) bool {
	return nativeCompletedStatus(status) || nativeFailedStatus(status)
}

func nativeJobsForInstance(jobs []nativeModelJob, instanceID string) []nativeModelJob {
	result := make([]nativeModelJob, 0)
	for _, job := range jobs {
		if job.InstanceID == instanceID {
			result = append(result, job)
		}
	}
	return result
}

func nativeTaskInstance(taskID string, instanceIDs []string) string {
	if len(instanceIDs) == 0 {
		return ""
	}
	var sum int
	for _, value := range []byte(taskID) {
		sum += int(value)
	}
	return instanceIDs[sum%len(instanceIDs)]
}

func nativeCountedTasks(counts map[string]int64) int64 {
	return counts["queued"] + counts["running"] + counts["retrying"] + counts["verifying"] + counts["completed"] + counts["failed"]
}

func nativeActiveStatus(status string) bool {
	return status == "queued" || status == "running" || status == "retrying" || status == "verifying" || status == "generating" || status == "testing" || status == "merging" || status == "proposed"
}

func nativeFailedStatus(status string) bool {
	return status == "failed" || status == "rejected" || status == "rolled_back"
}

func nativeStatusRank(status string) int {
	switch status {
	case "running", "generating", "testing", "merging":
		return 0
	case "retrying", "verifying":
		return 1
	case "queued", "proposed":
		return 2
	case "failed", "rejected", "rolled_back":
		return 3
	default:
		return 4
	}
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
