package jobs

import (
	"fmt"
	"slices"
	"strings"
)

const WorkerMigrationBatch = 6

type MigrationLane struct {
	ID                string   `json:"id"`
	Order             int      `json:"order"`
	PythonModelLane   string   `json:"python_model_lane"`
	CeleryQueuePrefix string   `json:"celery_queue_prefix"`
	GoQueue           string   `json:"go_queue"`
	TaskTypes         []string `json:"task_types"`
}

type MigrationLaneStatus struct {
	MigrationLane
	Status string `json:"status"`
}

type WorkerMigrationStatus struct {
	Batch              int                   `json:"batch"`
	Order              []string              `json:"order"`
	CompletedLanes     []string              `json:"completed_lanes"`
	NextLane           string                `json:"next_lane,omitempty"`
	CutoverReady       bool                  `json:"cutover_ready"`
	Lanes              []MigrationLaneStatus `json:"lanes"`
	ConfigurationError string                `json:"configuration_error,omitempty"`
}

var batchFourLaneDefinitions = []MigrationLane{
	{ID: "extract", Order: 1, PythonModelLane: "extract", CeleryQueuePrefix: "extract", GoQueue: "extract", TaskTypes: []string{
		"market_loop.extract_news_item", "market_loop.reextract_event",
		"market_loop.retry_news_item", "market_loop.finalize_news_extraction",
	}},
	{ID: "mapping", Order: 2, PythonModelLane: "assist", CeleryQueuePrefix: "mapping", GoQueue: "assist", TaskTypes: []string{
		"market_loop.resolve_event_assets",
	}},
	{ID: "research", Order: 3, PythonModelLane: "research", CeleryQueuePrefix: "research", GoQueue: "research", TaskTypes: []string{
		"market_loop.research_event", "market_loop.research_asset",
	}},
	{ID: "evolution", Order: 4, PythonModelLane: "code", CeleryQueuePrefix: "evolution", GoQueue: "code", TaskTypes: []string{
		"market_loop.evolve_from_outcomes", "market_loop.evolve_failures",
		"market_loop.execute_evolution",
	}},
	{ID: "discovery", Order: 5, PythonModelLane: "io", CeleryQueuePrefix: "io", GoQueue: "io", TaskTypes: []string{
		"market_loop.scan_news", "market_loop.dispatch_news_processing_outbox",
	}},
	{ID: "recovery", Order: 6, PythonModelLane: "io", CeleryQueuePrefix: "io", GoQueue: "recovery", TaskTypes: []string{
		"market_loop.cleanup_model_audits", "market_loop.reconcile_research_leases",
		"market_loop.reconcile_asset_mapping_leases", "market_loop.recover_orphaned_news",
	}},
}

func BatchFourLanes() []MigrationLane {
	lanes := make([]MigrationLane, len(batchFourLaneDefinitions))
	for index, lane := range batchFourLaneDefinitions {
		lanes[index] = lane
		lanes[index].TaskTypes = slices.Clone(lane.TaskTypes)
	}
	return lanes
}

func BatchFourMigrationStatus(completed []string) WorkerMigrationStatus {
	lanes := BatchFourLanes()
	status := WorkerMigrationStatus{Batch: WorkerMigrationBatch, Lanes: make([]MigrationLaneStatus, 0, len(lanes))}
	for _, lane := range lanes {
		status.Order = append(status.Order, lane.ID)
	}
	normalized, err := validateCompletedLanes(completed, lanes)
	if err != nil {
		status.ConfigurationError = err.Error()
		normalized = nil
	}
	status.CompletedLanes = normalized
	completedSet := make(map[string]bool, len(normalized))
	for _, lane := range normalized {
		completedSet[lane] = true
	}
	for _, lane := range lanes {
		laneStatus := "blocked"
		if completedSet[lane.ID] {
			laneStatus = "completed"
		} else if status.NextLane == "" && status.ConfigurationError == "" {
			laneStatus = "next"
			status.NextLane = lane.ID
		}
		status.Lanes = append(status.Lanes, MigrationLaneStatus{MigrationLane: lane, Status: laneStatus})
	}
	status.CutoverReady = status.ConfigurationError == "" && len(normalized) == len(lanes)
	return status
}

func ValidateBatchFourActivation(laneID string, completed []string) (MigrationLane, error) {
	status := BatchFourMigrationStatus(completed)
	if status.ConfigurationError != "" {
		return MigrationLane{}, fmt.Errorf("invalid batch 4 state: %s", status.ConfigurationError)
	}
	laneID = strings.TrimSpace(laneID)
	if laneID == "" {
		return MigrationLane{}, fmt.Errorf("GO_WORKER_LANE is required; next batch 4 lane is %s", status.NextLane)
	}
	for _, lane := range status.Lanes {
		if lane.ID != laneID {
			continue
		}
		if lane.Status == "blocked" {
			return MigrationLane{}, fmt.Errorf("batch 4 lane %s is blocked; next lane is %s", laneID, status.NextLane)
		}
		return lane.MigrationLane, nil
	}
	return MigrationLane{}, fmt.Errorf("unknown batch 4 lane %q", laneID)
}

func ValidateLaneHandlers(lane MigrationLane, handlers map[string]Handler) error {
	missing := make([]string, 0)
	for _, taskType := range lane.TaskTypes {
		if handlers[taskType] == nil {
			missing = append(missing, taskType)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("batch 4 lane %s has no Go handler for: %s", lane.ID, strings.Join(missing, ", "))
	}
	return nil
}

func validateCompletedLanes(completed []string, lanes []MigrationLane) ([]string, error) {
	result := make([]string, 0, len(completed))
	seen := map[string]bool{}
	for _, lane := range completed {
		lane = strings.TrimSpace(lane)
		if lane == "" {
			continue
		}
		if seen[lane] {
			return nil, fmt.Errorf("duplicate completed lane %q", lane)
		}
		if len(result) >= len(lanes) || lanes[len(result)].ID != lane {
			expected := "none"
			if len(result) < len(lanes) {
				expected = lanes[len(result)].ID
			}
			return nil, fmt.Errorf("completed lanes must be a prefix of batch 4 order; expected %s, got %s", expected, lane)
		}
		seen[lane] = true
		result = append(result, lane)
	}
	return result, nil
}
