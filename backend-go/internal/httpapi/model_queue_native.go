package httpapi

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"sort"
	"time"
)

type nativeQueueSpec struct {
	lane    string
	model   string
	purpose string
	binding string
	enabled bool
}

type nativeQueueResult struct {
	index int
	queue map[string]any
}

func (s *Server) buildNativeModelQueueOverview(ctx context.Context, limit int) map[string]any {
	now := time.Now().UTC()
	specs := []nativeQueueSpec{
		{lane: "extract", model: s.cfg.ExtractModel, purpose: "新闻抽取", binding: "新闻事件结构化抽取", enabled: true},
		{lane: "research", model: s.cfg.ResearchModel, purpose: "标的研究", binding: "工具深度研究与逐目标事件研报", enabled: true},
		{lane: "assist", model: s.cfg.AssistModel, purpose: "股票映射", binding: "新闻事件二次股票映射", enabled: true},
		{lane: "code", model: s.cfg.CodeModel, purpose: "代码演进", binding: evolutionBinding(s.cfg.EvolutionAutoMerge), enabled: s.cfg.EvolutionEnabled},
	}
	result := make(chan nativeQueueResult, len(specs))
	for index, spec := range specs {
		go func(index int, spec nativeQueueSpec) {
			inference := s.inferenceQueue(ctx, spec.lane, spec.model, spec.purpose, spec.binding, spec.enabled)
			tasks, taskErr := s.nativeModelTasks(ctx, spec.lane, now)
			result <- nativeQueueResult{index: index, queue: nativeQueueItem(spec, inference, tasks, taskErr, now, limit)}
		}(index, spec)
	}
	queues := make([]any, len(specs))
	for range specs {
		item := <-result
		queues[item.index] = item.queue
	}
	return map[string]any{"generated_at": now, "producer": "go-api", "queues": queues}
}

func (s *Server) nativeModelTasks(ctx context.Context, lane string, now time.Time) ([]map[string]any, error) {
	values, err := s.redis.HGetAll(ctx, "market-loop:model-queue:"+lane+":tasks").Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]map[string]any, 0, len(values))
	for taskID, raw := range values {
		var task map[string]any
		if json.Unmarshal([]byte(raw), &task) != nil {
			continue
		}
		if stringValue(task["task_id"]) == "" {
			task["task_id"] = taskID
		}
		normalizeNativeModelTask(task, now)
		if lane == "extract" && !nativeTaskLeaseCurrent(task, now, time.Duration(envIntValue("MODEL_TASK_LEASE_SECONDS", 180))*time.Second) {
			continue
		}
		tasks = append(tasks, task)
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		left, right := nativeTaskRank(stringValue(tasks[i]["status"])), nativeTaskRank(stringValue(tasks[j]["status"]))
		if left != right {
			return left < right
		}
		if left == 3 {
			return stringValue(tasks[i]["updated_at"]) > stringValue(tasks[j]["updated_at"])
		}
		return stringValue(tasks[i]["queued_at"]) < stringValue(tasks[j]["queued_at"])
	})
	return tasks, nil
}

func nativeTaskLeaseCurrent(task map[string]any, now time.Time, lease time.Duration) bool {
	if !nativeRunningStatus(stringValue(task["status"])) {
		return true
	}
	updated := parseAnyTime(task["updated_at"])
	return updated != nil && !updated.Before(now.Add(-lease))
}

func normalizeNativeModelTask(task map[string]any, now time.Time) {
	queued := parseAnyTime(task["queued_at"])
	if queued == nil {
		queued = &now
	}
	started := parseAnyTime(task["started_at"])
	completed := parseAnyTime(task["completed_at"])
	updated := parseAnyTime(task["updated_at"])
	if updated == nil {
		updated = queued
	}
	status := stringValue(task["status"])
	queueEnd := started
	if queueEnd == nil && (status == "queued" || status == "retrying" || status == "proposed") {
		queueEnd = &now
	}
	executionEnd := completed
	if executionEnd == nil && started != nil && nativeRunningStatus(status) {
		executionEnd = &now
	}
	task["queued_at"], task["started_at"], task["completed_at"], task["updated_at"] = queued, started, completed, updated
	task["queue_duration_ms"] = millisBetween(queued, queueEnd)
	task["execution_duration_ms"] = millisBetween(started, executionEnd)
	if int64Value(task["attempt"]) < 0 {
		task["attempt"] = 0
	} else if task["attempt"] == nil {
		task["attempt"] = 1
	}
	if int64Value(task["task_count"]) < 1 {
		task["task_count"] = 1
	}
	if task["entity_id"] == nil {
		task["entity_id"] = nil
	}
	if task["instance_id"] == nil {
		task["instance_id"] = nil
	}
	if task["source"] == nil {
		task["source"] = nil
	}
	if task["error"] == nil {
		task["error"] = nil
	}
	if _, ok := task["metrics"].(map[string]any); !ok {
		task["metrics"] = map[string]any{}
	}
}

func nativeQueueItem(spec nativeQueueSpec, inference map[string]any, tasks []map[string]any, taskErr error, now time.Time, limit int) map[string]any {
	instances, _ := inference["instances"].([]map[string]any)
	if instances == nil {
		for _, raw := range anySlice(inference["instances"]) {
			if item, ok := raw.(map[string]any); ok {
				instances = append(instances, item)
			}
		}
	}
	instanceIDs := make([]string, 0, len(instances))
	for _, instance := range instances {
		if id := stringValue(instance["id"]); id != "" {
			instanceIDs = append(instanceIDs, id)
		}
	}
	for _, task := range tasks {
		if len(instanceIDs) == 0 || stringValue(task["instance_id"]) != "" {
			continue
		}
		task["instance_id"] = instanceIDs[nativeTaskInstanceIndex(stringValue(task["task_id"]), len(instanceIDs))]
	}

	counts := nativeTaskCounts(tasks)
	counts["running"] = max(counts["running"], int(int64Value(inference["running"])))
	counts["waiting_for_model"] = int(int64Value(inference["queued"]))
	metrics := nativeTaskMetrics(tasks, counts, int(int64Value(inference["capacity"])), now)
	visible := nativeVisibleTasks(tasks)
	instanceItems := make([]any, 0, len(instances))
	for _, instance := range instances {
		id := stringValue(instance["id"])
		instanceTasks := make([]map[string]any, 0)
		for _, task := range tasks {
			if stringValue(task["instance_id"]) == id {
				instanceTasks = append(instanceTasks, task)
			}
		}
		instanceCounts := nativeTaskCounts(instanceTasks)
		instanceCounts["running"] = max(instanceCounts["running"], int(int64Value(instance["running"])))
		instanceCounts["waiting_for_model"] = int(int64Value(instance["queued"]))
		instanceVisible := nativeVisibleTasks(instanceTasks)
		instanceCapacity := int(int64Value(instance["capacity"]))
		instanceObservable := boolValue(instance["observable"])
		instanceHealthy := boolValue(instance["healthy"])
		instanceAvailable := boolValue(instance["model_available"])
		instanceItems = append(instanceItems, map[string]any{
			"id": id, "healthy": instanceHealthy, "model_available": instanceAvailable,
			"state":    nativeQueueState(instanceCounts, spec.enabled, instanceObservable && instanceHealthy && instanceAvailable),
			"capacity": instanceCapacity, "available": int(int64Value(instance["available"])), "observable": instanceObservable,
			"counts": instanceCounts, "metrics": nativeTaskMetrics(instanceTasks, instanceCounts, instanceCapacity, now),
			"total_tasks": nativeCountTotal(instanceCounts), "truncated": len(instanceVisible) > limit, "tasks": mapTasksToAny(instanceVisible, limit),
		})
	}
	errorValue := any(nil)
	if taskErr != nil {
		errorValue = "Redis 任务状态暂时不可用。 / Redis task state is temporarily unavailable."
	}
	return map[string]any{
		"id": spec.lane, "model": spec.model, "purpose": spec.purpose, "binding": spec.binding, "enabled": spec.enabled,
		"state": nativeQueueState(counts, spec.enabled, boolValue(inference["observable"])), "threads": modelThreads(spec.lane),
		"capacity": int(int64Value(inference["capacity"])), "available": int(int64Value(inference["available"])),
		"instance_count": len(instanceItems), "per_instance_concurrency": int(int64Value(inference["per_instance_concurrency"])),
		"observable": boolValue(inference["observable"]), "instances": instanceItems, "counts": counts, "metrics": metrics,
		"total_tasks": nativeCountTotal(counts), "truncated": len(visible) > limit, "tasks": mapTasksToAny(visible, limit), "error": errorValue,
	}
}

func nativeTaskCounts(tasks []map[string]any) map[string]int {
	counts := map[string]int{"queued": 0, "running": 0, "retrying": 0, "verifying": 0, "waiting_for_model": 0, "completed": 0, "failed": 0}
	for _, task := range tasks {
		switch status := stringValue(task["status"]); {
		case status == "queued" || status == "proposed":
			counts["queued"]++
		case nativeRunningStatus(status):
			counts["running"]++
		case status == "retrying":
			counts["retrying"]++
		case status == "verifying":
			counts["verifying"]++
		case nativeFailedStatus(status):
			counts["failed"]++
		case nativeCompletedStatus(status):
			counts["completed"]++
		}
	}
	return counts
}

func nativeTaskMetrics(tasks []map[string]any, counts map[string]int, capacity int, now time.Time) map[string]any {
	queueDurations, executionDurations, waits := []int64{}, []int64{}, []int64{}
	for _, task := range tasks {
		if value := nullableInt64(task["queue_duration_ms"]); value != nil {
			queueDurations = append(queueDurations, *value)
			if status := stringValue(task["status"]); status == "queued" || status == "retrying" {
				waits = append(waits, *value)
			}
		}
		if value := nullableInt64(task["execution_duration_ms"]); value != nil && (nativeFailedStatus(stringValue(task["status"])) || nativeCompletedStatus(stringValue(task["status"]))) {
			executionDurations = append(executionDurations, *value)
		}
	}
	sort.Slice(executionDurations, func(i, j int) bool { return executionDurations[i] < executionDurations[j] })
	metrics := emptyQueueMetrics()
	metrics["average_queue_duration_ms"] = averageInt64(queueDurations)
	metrics["average_execution_duration_ms"] = averageInt64(executionDurations)
	metrics["longest_wait_ms"] = maxInt64OrNil(waits)
	metrics["queue_duration_sample_count"] = len(queueDurations)
	metrics["execution_duration_sample_count"] = len(executionDurations)
	metrics["execution_p50_ms"] = percentileInt64(executionDurations, 50)
	metrics["execution_p90_ms"] = percentileInt64(executionDurations, 90)
	work := counts["queued"] + counts["retrying"] + counts["running"] + counts["verifying"]
	if average := nullableInt64(metrics["average_execution_duration_ms"]); average != nil && capacity > 0 && work > 0 {
		metrics["estimated_clear_ms"] = int64((work+capacity-1)/capacity) * *average
	}
	_ = now
	return metrics
}

func nativeVisibleTasks(tasks []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(tasks))
	for _, task := range tasks {
		status := stringValue(task["status"])
		if status == "queued" || status == "retrying" || status == "verifying" || nativeRunningStatus(status) || nativeFailedStatus(status) {
			result = append(result, task)
		}
	}
	return result
}

func nativeTaskRank(status string) int {
	if nativeRunningStatus(status) || status == "verifying" {
		return 0
	}
	if status == "retrying" {
		return 1
	}
	if status == "queued" || status == "proposed" {
		return 2
	}
	if nativeFailedStatus(status) {
		return 3
	}
	return 4
}

func nativeRunningStatus(status string) bool {
	return status == "running" || status == "coding" || status == "testing" || status == "merging"
}

func nativeFailedStatus(status string) bool {
	return status == "failed" || status == "rejected" || status == "rolled_back"
}

func nativeCompletedStatus(status string) bool {
	return status == "completed" || status == "merged" || status == "cancelled"
}

func nativeQueueState(counts map[string]int, enabled, observable bool) string {
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

func nativeCountTotal(counts map[string]int) int {
	return counts["queued"] + counts["running"] + counts["retrying"] + counts["verifying"] + counts["completed"] + counts["failed"]
}

func nativeTaskInstanceIndex(taskID string, count int) int {
	if count <= 1 {
		return 0
	}
	digest := fnv.New32a()
	_, _ = digest.Write([]byte(taskID))
	return int(digest.Sum32() % uint32(count))
}

func mapTasksToAny(tasks []map[string]any, limit int) []any {
	if len(tasks) > limit {
		tasks = tasks[:limit]
	}
	result := make([]any, len(tasks))
	for index := range tasks {
		result[index] = tasks[index]
	}
	return result
}

func nullableInt64(value any) *int64 {
	if value == nil {
		return nil
	}
	result := int64Value(value)
	return &result
}

func maxInt64OrNil(values []int64) any {
	if len(values) == 0 {
		return nil
	}
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}

func percentileInt64(values []int64, percentile int) any {
	if len(values) == 0 {
		return nil
	}
	index := (len(values)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}
