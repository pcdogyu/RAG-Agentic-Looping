package httpapi

import (
	"errors"
	"testing"
	"time"
)

func TestNativeModelQueueItemUsesDurableJobCountsAndInstances(t *testing.T) {
	now := time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC)
	jobs := []nativeModelJob{
		{ID: "running", Status: "running", CreatedAt: now.Add(-2 * time.Minute), UpdatedAt: now, Kind: "event_research", EntityID: "event-1", Title: "真实新闻标题"},
		{ID: "failed", Status: "failed", CreatedAt: now.Add(-4 * time.Minute), UpdatedAt: now.Add(-2 * time.Minute), Kind: "event_research", EntityID: "event-2", Title: "另一条新闻", Error: "timeout"},
		{ID: "completed", Status: "completed", CreatedAt: now.Add(-6 * time.Minute), UpdatedAt: now.Add(-4 * time.Minute), Kind: "event_research", EntityID: "event-3", Title: "已完成新闻"},
	}
	inference := map[string]any{
		"capacity": 3, "available": 2, "running": 1, "queued": 2, "observable": true, "per_instance_concurrency": 3,
		"instances": []map[string]any{{"id": "research-0", "healthy": true, "model_available": true, "capacity": 3, "available": 2, "running": 1, "queued": 2, "observable": true}},
	}
	queue := nativeModelQueueItem(nativeModelQueueSpec{id: "research", model: "qwen2.5:7b", purpose: "标的研究", binding: "研究", enabled: true}, inference, jobs, nativeExecutionSamples{}, 50, now, nil)
	counts := queue["counts"].(map[string]int64)
	if counts["running"] != 1 || counts["failed"] != 1 || counts["completed"] != 1 || counts["waiting_for_model"] != 2 {
		t.Fatalf("unexpected counts: %#v", counts)
	}
	if queue["state"] != "running" || queue["instance_count"] != 1 || queue["total_tasks"] != int64(3) {
		t.Fatalf("unexpected queue summary: %#v", queue)
	}
	visible := queue["tasks"].([]any)
	if len(visible) != 2 {
		t.Fatalf("visible tasks = %d, want 2", len(visible))
	}
	first := visible[0].(map[string]any)
	if stringValue(first["title"]) != "真实新闻标题" || stringValue(first["instance_id"]) != "research-0" {
		t.Fatalf("headline or instance assignment lost: %#v", first)
	}
}

func TestNativeResearchVisibleTasksDeduplicatesActiveAndFailedSubject(t *testing.T) {
	now := time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC)
	jobs := []nativeModelJob{
		{ID: "active", Status: "queued", Kind: "event_research", EntityID: "event-1", Title: "同一新闻标题", CreatedAt: now, UpdatedAt: now},
		{ID: "failed", Status: "failed", Kind: "event_research", EntityID: "event-1", Title: "同一新闻标题", CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)},
	}
	visible := nativeVisibleTasks("research", jobs, now)
	if len(visible) != 1 || visible[0].ID != "active" {
		t.Fatalf("active task should win research deduplication: %#v", visible)
	}
}

func TestNativeModelQueueItemReturnsDatabaseError(t *testing.T) {
	queue := nativeModelQueueItem(
		nativeModelQueueSpec{id: "assist", enabled: true},
		map[string]any{"capacity": 1, "available": 0, "observable": true, "instances": []map[string]any{}},
		nil,
		nativeExecutionSamples{},
		50,
		time.Now().UTC(),
		errors.New("database unavailable"),
	)
	if queue["error"] != "模型队列任务状态暂时不可用。" {
		t.Fatalf("unexpected error: %v", queue["error"])
	}
}

func TestNativeModelQueueItemPrefersAuditExecutionSamples(t *testing.T) {
	now := time.Date(2026, time.September, 2, 8, 0, 0, 0, time.UTC)
	completed := now.Add(-time.Hour)
	started := completed.Add(-time.Millisecond)
	jobs := []nativeModelJob{{
		ID: "wrapper", Status: "completed", CreatedAt: started, UpdatedAt: completed,
		StartedAt: &started, CompletedAt: &completed, ExecutionMS: 1, InstanceID: "extract-0",
	}}
	inference := map[string]any{
		"capacity": 1, "observable": true, "per_instance_concurrency": 1,
		"instances": []map[string]any{{"id": "extract-0", "healthy": true, "model_available": true, "capacity": 1, "observable": true}},
	}
	samples := nativeExecutionSamples{
		all:        []int64{20_000, 40_000, 60_000},
		byInstance: map[string][]int64{"extract-0": {20_000, 40_000, 60_000}},
	}
	queue := nativeModelQueueItem(nativeModelQueueSpec{id: "extract", enabled: true}, inference, jobs, samples, 50, now, nil)
	metrics := queue["metrics"].(map[string]any)
	instanceMetrics := queue["instances"].([]any)[0].(map[string]any)["metrics"].(map[string]any)
	for label, values := range map[string]map[string]any{"queue": metrics, "instance": instanceMetrics} {
		if values["average_execution_duration_ms"] != int64(40_000) || values["execution_duration_sample_count"] != 3 {
			t.Fatalf("%s metrics did not use audit samples: %#v", label, values)
		}
		if values["execution_p50_ms"] != int64(40_000) || values["execution_p90_ms"] != int64(60_000) {
			t.Fatalf("%s percentiles did not use audit samples: %#v", label, values)
		}
	}
}

func TestNativeQueueState(t *testing.T) {
	counts := nativeQueueCounts(nil)
	if state := nativeQueueState(counts, false, true); state != "disabled" {
		t.Fatalf("disabled state = %s", state)
	}
	counts["queued"] = 1
	if state := nativeQueueState(counts, true, true); state != "queued" {
		t.Fatalf("queued state = %s", state)
	}
}

func TestNativeQueueMetricsUsesRecentExecutionSamples(t *testing.T) {
	now := time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC)
	started := now.Add(-6 * time.Hour)
	recentOne := now.Add(-3 * time.Hour)
	recentTwo := now.Add(-time.Hour)
	recentFailure := now.Add(-30 * time.Minute)
	old := now.Add(-5 * time.Hour)
	jobs := []nativeModelJob{
		{ID: "one", Status: "completed", StartedAt: &started, CompletedAt: &recentOne, UpdatedAt: recentOne, ExecutionMS: 1000},
		{ID: "two", Status: "completed", StartedAt: &started, CompletedAt: &recentTwo, UpdatedAt: recentTwo, ExecutionMS: 3000},
		{ID: "failure", Status: "failed", StartedAt: &started, CompletedAt: &recentFailure, UpdatedAt: recentFailure, ExecutionMS: 5000},
		{ID: "old", Status: "completed", StartedAt: &started, CompletedAt: &old, UpdatedAt: old, ExecutionMS: 9000},
	}
	counts := nativeQueueCounts(jobs)
	counts["queued"] = 2
	metrics := nativeQueueMetrics(jobs, counts, now)
	if metrics["average_execution_duration_ms"] != int64(3000) || metrics["execution_duration_sample_count"] != 3 {
		t.Fatalf("unexpected average execution metrics: %#v", metrics)
	}
	if metrics["execution_p50_ms"] != int64(3000) || metrics["execution_p90_ms"] != int64(5000) {
		t.Fatalf("unexpected execution percentiles: %#v", metrics)
	}
	if metrics["throughput_per_hour"] != float64(0.5) {
		t.Fatalf("unexpected throughput: %#v", metrics)
	}
}

func TestNativeTaskValuesIncludesLiveExecutionDuration(t *testing.T) {
	now := time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC)
	started := now.Add(-time.Minute)
	attempt := now.Add(-2 * time.Second)
	values := nativeTaskValues([]nativeModelJob{{
		ID: "running", Status: "running", Attempt: 1, CreatedAt: started, UpdatedAt: now,
		StartedAt: &started, AttemptAt: &attempt, ExecutionMS: 500,
	}}, now)
	task := values[0].(map[string]any)
	if task["started_at"] == nil || task["execution_duration_ms"] != int64(2500) {
		t.Fatalf("missing live execution timing: %#v", task)
	}
}
