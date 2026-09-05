package httpapi

import (
	"context"
	"math"
	"net/http"
	"sort"
	"time"
)

const modelRuntimeWindow = 24 * time.Hour

type modelRuntimeAuditAttempt struct {
	Provider     string
	Model        string
	LogicalID    string
	Status       string
	CompletedAt  time.Time
	DurationMS   *int
	InputTokens  *int
	OutputTokens *int
}

type modelRuntimeQueueCount struct {
	Queued  int64
	Running int64
}

type modelRuntimeLane struct {
	ID      string `json:"id"`
	Purpose string `json:"purpose"`
	Enabled bool   `json:"enabled"`
}

type modelRuntimeMetrics struct {
	ProcessedTasks       int64    `json:"processed_tasks"`
	SuccessfulTasks      int64    `json:"successful_tasks"`
	FailedTasks          int64    `json:"failed_tasks"`
	QueuedTasks          int64    `json:"queued_tasks"`
	RunningTasks         int64    `json:"running_tasks"`
	SuccessRate          *float64 `json:"success_rate"`
	FailureRate          *float64 `json:"failure_rate"`
	AverageProcessingMS  *int64   `json:"average_processing_ms"`
	InputTokens          int64    `json:"input_tokens"`
	OutputTokens         int64    `json:"output_tokens"`
	AverageInputTokens   *int64   `json:"average_input_tokens"`
	AverageOutputTokens  *int64   `json:"average_output_tokens"`
	InputTokenTaskCount  int64    `json:"input_token_task_count"`
	OutputTokenTaskCount int64    `json:"output_token_task_count"`
}

type modelRuntimeModel struct {
	Provider      string             `json:"provider"`
	Model         string             `json:"model"`
	Configured    bool               `json:"configured"`
	Enabled       bool               `json:"enabled"`
	ActivityState string             `json:"activity_state"`
	Lanes         []modelRuntimeLane `json:"lanes"`
	modelRuntimeMetrics
}

type modelRuntimeSummaryPayload struct {
	GeneratedAt time.Time           `json:"generated_at"`
	WindowStart time.Time           `json:"window_started_at"`
	WindowEnd   time.Time           `json:"window_ended_at"`
	WindowHours int                 `json:"window_hours"`
	Totals      modelRuntimeMetrics `json:"totals"`
	Models      []modelRuntimeModel `json:"models"`
}

type modelRuntimeTaskAccumulator struct {
	Provider        string
	Model           string
	LogicalID       string
	FinalAt         time.Time
	Successful      bool
	DurationSum     int64
	DurationKnown   bool
	DurationMissing bool
	InputSum        int64
	InputKnown      bool
	InputMissing    bool
	OutputSum       int64
	OutputKnown     bool
	OutputMissing   bool
}

type modelRuntimeMetricAccumulator struct {
	processed        int64
	successful       int64
	failed           int64
	queued           int64
	running          int64
	durationSum      int64
	durationTasks    int64
	inputTotal       int64
	inputAverageSum  int64
	inputTasks       int64
	outputTotal      int64
	outputAverageSum int64
	outputTasks      int64
}

type modelRuntimeModelAccumulator struct {
	provider   string
	model      string
	configured bool
	enabled    bool
	lanes      []modelRuntimeLane
	metrics    modelRuntimeMetricAccumulator
}

func (s *Server) modelRuntimeSummary(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	windowStart := now.Add(-modelRuntimeWindow)
	attempts, err := s.loadModelRuntimeAuditAttempts(r.Context(), windowStart, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "model runtime audit query failed")
		return
	}
	queueCounts, err := s.loadModelRuntimeQueueCounts(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "model runtime queue query failed")
		return
	}
	writeJSON(w, http.StatusOK, buildModelRuntimeSummary(now, s.nativeModelQueueSpecs(), attempts, queueCounts))
}

// loadModelRuntimeAuditAttempts first selects logical calls with activity in
// the requested window, then loads every attempt for those calls. This keeps a
// retry that began before the boundary attached to the task that finished in
// the last 24 hours.
func (s *Server) loadModelRuntimeAuditAttempts(ctx context.Context, windowStart, windowEnd time.Time) ([]modelRuntimeAuditAttempt, error) {
	rows, err := s.db.Query(ctx, `
		WITH eligible AS (
			SELECT DISTINCT provider,model,logical_call_id
			FROM model_call_audits
			WHERE completed_at >= $1 AND completed_at <= $2
			  AND status IN ('completed','failed')
			  AND provider <> '' AND model <> ''
		)
		SELECT audit.provider,audit.model,audit.logical_call_id,audit.status,audit.completed_at,
		       audit.duration_ms,audit.prompt_tokens,audit.completion_tokens
		FROM model_call_audits audit
		JOIN eligible USING(provider,model,logical_call_id)
		WHERE audit.status IN ('completed','failed')
		ORDER BY audit.provider,audit.model,audit.logical_call_id,audit.completed_at,audit.attempt`, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := make([]modelRuntimeAuditAttempt, 0)
	for rows.Next() {
		var attempt modelRuntimeAuditAttempt
		if err := rows.Scan(
			&attempt.Provider, &attempt.Model, &attempt.LogicalID, &attempt.Status, &attempt.CompletedAt,
			&attempt.DurationMS, &attempt.InputTokens, &attempt.OutputTokens,
		); err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}

func (s *Server) loadModelRuntimeQueueCounts(ctx context.Context) (map[string]modelRuntimeQueueCount, error) {
	rows, err := s.db.Query(ctx, `
		SELECT queue,
		       count(*) FILTER (WHERE status IN ('queued','retrying'))::bigint,
		       count(*) FILTER (WHERE status='running')::bigint
		FROM go_jobs
		WHERE status IN ('queued','retrying','running')
		GROUP BY queue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]modelRuntimeQueueCount{}
	for rows.Next() {
		var queue string
		var count modelRuntimeQueueCount
		if err := rows.Scan(&queue, &count.Queued, &count.Running); err != nil {
			return nil, err
		}
		counts[queue] = count
	}
	return counts, rows.Err()
}

func buildModelRuntimeSummary(now time.Time, specs []nativeModelQueueSpec, attempts []modelRuntimeAuditAttempt, queueCounts map[string]modelRuntimeQueueCount) modelRuntimeSummaryPayload {
	windowEnd := now.UTC()
	windowStart := windowEnd.Add(-modelRuntimeWindow)
	models := map[string]*modelRuntimeModelAccumulator{}
	configuredOrder := make([]string, 0, len(specs))
	for _, spec := range specs {
		key := modelRuntimeKey("ollama", spec.model)
		item := models[key]
		if item == nil {
			item = &modelRuntimeModelAccumulator{provider: "ollama", model: spec.model, configured: true}
			models[key] = item
			configuredOrder = append(configuredOrder, key)
		}
		item.enabled = item.enabled || spec.enabled
		item.lanes = append(item.lanes, modelRuntimeLane{ID: spec.id, Purpose: spec.purpose, Enabled: spec.enabled})
		count := queueCounts[spec.id]
		item.metrics.queued += count.Queued
		item.metrics.running += count.Running
	}

	tasks := map[string]*modelRuntimeTaskAccumulator{}
	for _, attempt := range attempts {
		key := modelRuntimeKey(attempt.Provider, attempt.Model) + "\x00" + attempt.LogicalID
		task := tasks[key]
		if task == nil {
			task = &modelRuntimeTaskAccumulator{Provider: attempt.Provider, Model: attempt.Model, LogicalID: attempt.LogicalID}
			tasks[key] = task
		}
		if attempt.CompletedAt.After(task.FinalAt) {
			task.FinalAt = attempt.CompletedAt.UTC()
		}
		if attempt.Status == "completed" {
			task.Successful = true
		}
		if attempt.DurationMS != nil {
			task.DurationKnown = true
			task.DurationSum += int64(*attempt.DurationMS)
		} else {
			task.DurationMissing = true
		}
		if attempt.InputTokens != nil {
			task.InputKnown = true
			task.InputSum += int64(*attempt.InputTokens)
		} else {
			task.InputMissing = true
		}
		if attempt.OutputTokens != nil {
			task.OutputKnown = true
			task.OutputSum += int64(*attempt.OutputTokens)
		} else {
			task.OutputMissing = true
		}
	}
	for _, task := range tasks {
		if task.FinalAt.Before(windowStart) || task.FinalAt.After(windowEnd) {
			continue
		}
		key := modelRuntimeKey(task.Provider, task.Model)
		item := models[key]
		if item == nil {
			item = &modelRuntimeModelAccumulator{provider: task.Provider, model: task.Model}
			models[key] = item
		}
		item.metrics.addTask(task)
	}

	historicalOrder := make([]string, 0)
	configured := map[string]bool{}
	for _, key := range configuredOrder {
		configured[key] = true
	}
	for key := range models {
		if !configured[key] {
			historicalOrder = append(historicalOrder, key)
		}
	}
	sort.Slice(historicalOrder, func(i, j int) bool {
		left, right := models[historicalOrder[i]], models[historicalOrder[j]]
		if left.provider != right.provider {
			return left.provider < right.provider
		}
		return left.model < right.model
	})
	order := append(configuredOrder, historicalOrder...)
	items := make([]modelRuntimeModel, 0, len(order))
	var totals modelRuntimeMetricAccumulator
	for _, key := range order {
		item := models[key]
		totals.add(item.metrics)
		lanes := item.lanes
		if lanes == nil {
			lanes = []modelRuntimeLane{}
		}
		items = append(items, modelRuntimeModel{
			Provider: item.provider, Model: item.model, Configured: item.configured, Enabled: item.enabled,
			ActivityState: modelRuntimeActivityState(item), Lanes: lanes,
			modelRuntimeMetrics: item.metrics.result(),
		})
	}
	return modelRuntimeSummaryPayload{
		GeneratedAt: windowEnd, WindowStart: windowStart, WindowEnd: windowEnd, WindowHours: int(modelRuntimeWindow / time.Hour),
		Totals: totals.result(), Models: items,
	}
}

func (a *modelRuntimeMetricAccumulator) addTask(task *modelRuntimeTaskAccumulator) {
	a.processed++
	if task.Successful {
		a.successful++
	} else {
		a.failed++
	}
	if task.DurationKnown && !task.DurationMissing {
		a.durationSum += task.DurationSum
		a.durationTasks++
	}
	a.inputTotal += task.InputSum
	if task.InputKnown && !task.InputMissing {
		a.inputAverageSum += task.InputSum
		a.inputTasks++
	}
	a.outputTotal += task.OutputSum
	if task.OutputKnown && !task.OutputMissing {
		a.outputAverageSum += task.OutputSum
		a.outputTasks++
	}
}

func (a *modelRuntimeMetricAccumulator) add(other modelRuntimeMetricAccumulator) {
	a.processed += other.processed
	a.successful += other.successful
	a.failed += other.failed
	a.queued += other.queued
	a.running += other.running
	a.durationSum += other.durationSum
	a.durationTasks += other.durationTasks
	a.inputTotal += other.inputTotal
	a.inputAverageSum += other.inputAverageSum
	a.inputTasks += other.inputTasks
	a.outputTotal += other.outputTotal
	a.outputAverageSum += other.outputAverageSum
	a.outputTasks += other.outputTasks
}

func (a modelRuntimeMetricAccumulator) result() modelRuntimeMetrics {
	var successRate, failureRate *float64
	if a.processed > 0 {
		success := float64(a.successful) / float64(a.processed)
		failure := float64(a.failed) / float64(a.processed)
		successRate, failureRate = &success, &failure
	}
	return modelRuntimeMetrics{
		ProcessedTasks: a.processed, SuccessfulTasks: a.successful, FailedTasks: a.failed,
		QueuedTasks: a.queued, RunningTasks: a.running, SuccessRate: successRate, FailureRate: failureRate,
		AverageProcessingMS: roundedAverage(a.durationSum, a.durationTasks),
		InputTokens:         a.inputTotal, OutputTokens: a.outputTotal,
		AverageInputTokens: roundedAverage(a.inputAverageSum, a.inputTasks), AverageOutputTokens: roundedAverage(a.outputAverageSum, a.outputTasks),
		InputTokenTaskCount: a.inputTasks, OutputTokenTaskCount: a.outputTasks,
	}
}

func roundedAverage(sum, count int64) *int64 {
	if count == 0 {
		return nil
	}
	value := int64(math.RoundToEven(float64(sum) / float64(count)))
	return &value
}

func modelRuntimeKey(provider, model string) string {
	return provider + "\x00" + model
}

func modelRuntimeActivityState(item *modelRuntimeModelAccumulator) string {
	if !item.configured {
		return "historical"
	}
	if item.metrics.running > 0 {
		return "running"
	}
	if item.metrics.queued > 0 {
		return "queued"
	}
	if !item.enabled {
		return "disabled"
	}
	return "idle"
}
