package httpapi

import (
	"testing"
	"time"
)

func TestNativeQueueItemUsesTrackedTasksAndInferenceCapacity(t *testing.T) {
	now := time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC)
	tasks := []map[string]any{
		{"task_id": "running", "status": "running", "queued_at": now.Add(-2 * time.Minute), "started_at": now.Add(-time.Minute), "updated_at": now, "task_count": 1, "metrics": map[string]any{}},
		{"task_id": "failed", "status": "failed", "queued_at": now.Add(-4 * time.Minute), "started_at": now.Add(-3 * time.Minute), "completed_at": now.Add(-2 * time.Minute), "updated_at": now.Add(-2 * time.Minute), "task_count": 1, "metrics": map[string]any{}},
		{"task_id": "completed", "status": "completed", "queued_at": now.Add(-6 * time.Minute), "started_at": now.Add(-5 * time.Minute), "completed_at": now.Add(-4 * time.Minute), "updated_at": now.Add(-4 * time.Minute), "task_count": 1, "metrics": map[string]any{}},
	}
	for _, task := range tasks {
		normalizeNativeModelTask(task, now)
	}
	inference := map[string]any{
		"capacity": 3, "available": 2, "running": 1, "queued": 2, "observable": true, "per_instance_concurrency": 3,
		"instances": []map[string]any{{"id": "research-0", "healthy": true, "model_available": true, "capacity": 3, "available": 2, "running": 1, "queued": 2, "observable": true}},
	}
	queue := nativeQueueItem(nativeQueueSpec{lane: "research", model: "qwen2.5:7b", purpose: "标的研究", binding: "研究", enabled: true}, inference, tasks, nil, now, 50)
	counts := queue["counts"].(map[string]int)
	if counts["running"] != 1 || counts["failed"] != 1 || counts["completed"] != 1 || counts["waiting_for_model"] != 2 {
		t.Fatalf("unexpected counts: %#v", counts)
	}
	if queue["state"] != "running" || queue["instance_count"] != 1 || queue["total_tasks"] != 3 {
		t.Fatalf("unexpected queue summary: %#v", queue)
	}
	visible := queue["tasks"].([]any)
	if len(visible) != 2 {
		t.Fatalf("visible tasks = %d, want 2", len(visible))
	}
	if stringValue(visible[0].(map[string]any)["instance_id"]) != "research-0" {
		t.Fatalf("task was not assigned to the native instance: %#v", visible[0])
	}
}

func TestNativeQueueItemReturnsBilingualRedisError(t *testing.T) {
	queue := nativeQueueItem(
		nativeQueueSpec{lane: "assist", enabled: true},
		map[string]any{"capacity": 1, "available": 0, "observable": false, "instances": []map[string]any{}},
		nil,
		contextDeadlineError{},
		time.Now().UTC(),
		50,
	)
	if queue["state"] != "unavailable" {
		t.Fatalf("state = %v, want unavailable", queue["state"])
	}
	if queue["error"] != "Redis 任务状态暂时不可用。 / Redis task state is temporarily unavailable." {
		t.Fatalf("unexpected error: %v", queue["error"])
	}
}

type contextDeadlineError struct{}

func (contextDeadlineError) Error() string { return "deadline" }
