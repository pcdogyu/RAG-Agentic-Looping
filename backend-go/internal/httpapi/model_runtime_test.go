package httpapi

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildModelRuntimeSummaryUsesLogicalTasksAndCurrentQueues(t *testing.T) {
	now := time.Date(2026, time.September, 4, 8, 0, 0, 0, time.UTC)
	windowStart := now.Add(-modelRuntimeWindow)
	specs := []nativeModelQueueSpec{
		{id: "extract", model: "model-a", purpose: "新闻抽取", enabled: true},
		{id: "assist", model: "model-a", purpose: "股票映射", enabled: true},
		{id: "research", model: "model-b", purpose: "标的研究", enabled: true},
		{id: "code", model: "model-c", purpose: "代码演进", enabled: false},
	}
	attempts := []modelRuntimeAuditAttempt{
		// A retry that began before the 24-hour boundary belongs to the task
		// whose successful terminal attempt completed inside the window.
		{Provider: "ollama", Model: "model-a", LogicalID: "retry-success", Status: "failed", CompletedAt: windowStart.Add(-time.Hour), DurationMS: intPointer(100), InputTokens: intPointer(10), OutputTokens: intPointer(1)},
		{Provider: "ollama", Model: "model-a", LogicalID: "retry-success", Status: "completed", CompletedAt: windowStart.Add(time.Hour), DurationMS: intPointer(300), InputTokens: intPointer(30), OutputTokens: intPointer(3)},
		{Provider: "ollama", Model: "model-a", LogicalID: "all-failed", Status: "failed", CompletedAt: now.Add(-2 * time.Hour), DurationMS: intPointer(200), InputTokens: intPointer(5), OutputTokens: intPointer(2)},
		{Provider: "ollama", Model: "model-a", LogicalID: "all-failed", Status: "failed", CompletedAt: now.Add(-time.Hour), DurationMS: intPointer(400), InputTokens: intPointer(5), OutputTokens: intPointer(2)},
		// The lower boundary is inclusive, while an older terminal task is excluded.
		{Provider: "ollama", Model: "model-b", LogicalID: "boundary", Status: "completed", CompletedAt: windowStart, DurationMS: intPointer(600), InputTokens: intPointer(20), OutputTokens: intPointer(2)},
		{Provider: "ollama", Model: "model-b", LogicalID: "missing-token", Status: "failed", CompletedAt: now.Add(-2 * time.Minute), DurationMS: intPointer(200), InputTokens: intPointer(100), OutputTokens: intPointer(10)},
		{Provider: "ollama", Model: "model-b", LogicalID: "missing-token", Status: "completed", CompletedAt: now.Add(-time.Minute), DurationMS: intPointer(800)},
		{Provider: "ollama", Model: "model-b", LogicalID: "old", Status: "completed", CompletedAt: windowStart.Add(-time.Nanosecond), DurationMS: intPointer(900), InputTokens: intPointer(99), OutputTokens: intPointer(99)},
		// A recently used, no-longer-configured model remains visible as history.
		{Provider: "openai-compatible", Model: "model-z", LogicalID: "historical", Status: "completed", CompletedAt: now.Add(-30 * time.Minute), DurationMS: intPointer(1_000), InputTokens: intPointer(7), OutputTokens: intPointer(3)},
	}
	queues := map[string]modelRuntimeQueueCount{
		"extract":  {Queued: 2, Running: 1},
		"assist":   {Queued: 1},
		"research": {Queued: 4},
	}

	payload := buildModelRuntimeSummary(now, specs, attempts, queues)
	if payload.WindowHours != 24 || !payload.WindowStart.Equal(windowStart) || !payload.WindowEnd.Equal(now) {
		t.Fatalf("unexpected window: %+v", payload)
	}
	if len(payload.Models) != 4 {
		t.Fatalf("models = %d, want 4", len(payload.Models))
	}

	modelA := payload.Models[0]
	if modelA.Model != "model-a" || len(modelA.Lanes) != 2 || modelA.ActivityState != "running" {
		t.Fatalf("configured multi-lane model is incorrect: %+v", modelA)
	}
	if modelA.ProcessedTasks != 2 || modelA.SuccessfulTasks != 1 || modelA.FailedTasks != 1 {
		t.Fatalf("logical retry outcome is incorrect: %+v", modelA.modelRuntimeMetrics)
	}
	if modelA.QueuedTasks != 3 || modelA.RunningTasks != 1 {
		t.Fatalf("current queue counts are incorrect: %+v", modelA.modelRuntimeMetrics)
	}
	if modelA.InputTokens != 50 || modelA.OutputTokens != 8 || valueOrZero(modelA.AverageInputTokens) != 25 || valueOrZero(modelA.AverageOutputTokens) != 4 {
		t.Fatalf("retry token accounting is incorrect: %+v", modelA.modelRuntimeMetrics)
	}
	if valueOrZero(modelA.AverageProcessingMS) != 500 || modelA.SuccessRate == nil || *modelA.SuccessRate != 0.5 || modelA.FailureRate == nil || *modelA.FailureRate != 0.5 {
		t.Fatalf("duration or rates are incorrect: %+v", modelA.modelRuntimeMetrics)
	}

	modelB := payload.Models[1]
	if modelB.ProcessedTasks != 2 || modelB.QueuedTasks != 4 || modelB.ActivityState != "queued" {
		t.Fatalf("boundary or queued model is incorrect: %+v", modelB)
	}
	if modelB.InputTokens != 120 || modelB.InputTokenTaskCount != 1 || valueOrZero(modelB.AverageInputTokens) != 20 {
		t.Fatalf("missing input tokens must not count as zero in averages: %+v", modelB.modelRuntimeMetrics)
	}
	if modelB.OutputTokens != 12 || modelB.OutputTokenTaskCount != 1 || valueOrZero(modelB.AverageOutputTokens) != 2 {
		t.Fatalf("missing output tokens must not count as zero in averages: %+v", modelB.modelRuntimeMetrics)
	}

	modelC := payload.Models[2]
	if modelC.Model != "model-c" || modelC.ActivityState != "disabled" || modelC.ProcessedTasks != 0 {
		t.Fatalf("disabled zero-task model is incorrect: %+v", modelC)
	}
	historical := payload.Models[3]
	if historical.Provider != "openai-compatible" || historical.Model != "model-z" || historical.Configured || historical.ActivityState != "historical" {
		t.Fatalf("historical model is incorrect: %+v", historical)
	}
	if payload.Totals.ProcessedTasks != 5 || payload.Totals.QueuedTasks != 7 || payload.Totals.RunningTasks != 1 {
		t.Fatalf("global totals are incorrect: %+v", payload.Totals)
	}
}

func TestBuildModelRuntimeSummaryReturnsNullRatesAndAveragesWithoutTasks(t *testing.T) {
	now := time.Date(2026, time.September, 4, 8, 0, 0, 0, time.UTC)
	payload := buildModelRuntimeSummary(now, []nativeModelQueueSpec{{id: "extract", model: "model-a", enabled: true}}, nil, nil)
	if len(payload.Models) != 1 || payload.Models[0].ActivityState != "idle" {
		t.Fatalf("unexpected empty summary: %+v", payload)
	}
	metrics := payload.Models[0].modelRuntimeMetrics
	if metrics.SuccessRate != nil || metrics.FailureRate != nil || metrics.AverageProcessingMS != nil || metrics.AverageInputTokens != nil || metrics.AverageOutputTokens != nil {
		t.Fatalf("empty rates and averages must be null: %+v", metrics)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Models) != 1 || response.Models[0]["processed_tasks"] != float64(0) || response.Models[0]["activity_state"] != "idle" {
		t.Fatalf("runtime metrics must be flattened in the JSON model: %s", body)
	}
}

func intPointer(value int) *int { return &value }

func valueOrZero(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
