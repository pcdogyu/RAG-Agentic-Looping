package jobs

import (
	"fmt"
	"slices"
	"strings"
)

const WorkerRuntimeVersion = 1

type MigrationLane struct {
	ID        string   `json:"id"`
	Order     int      `json:"order"`
	GoQueue   string   `json:"go_queue"`
	TaskTypes []string `json:"task_types"`
}

type WorkerRuntimeState struct {
	Version      int             `json:"version"`
	Runtime      string          `json:"runtime"`
	QueueBackend string          `json:"queue_backend"`
	CutoverReady bool            `json:"cutover_ready"`
	Order        []string        `json:"order"`
	Lanes        []MigrationLane `json:"lanes"`
}

var workerLaneDefinitions = []MigrationLane{
	{ID: "extract", Order: 1, GoQueue: "extract", TaskTypes: []string{
		"market_loop.extract_news_item", "market_loop.reextract_event",
		"market_loop.retry_news_item", "market_loop.finalize_news_extraction",
	}},
	{ID: "mapping", Order: 2, GoQueue: "assist", TaskTypes: []string{
		"market_loop.resolve_event_assets",
	}},
	{ID: "research", Order: 3, GoQueue: "research", TaskTypes: []string{
		"market_loop.research_event", "market_loop.research_asset",
	}},
	{ID: "evolution", Order: 4, GoQueue: "code", TaskTypes: []string{
		"market_loop.evolve_from_outcomes", "market_loop.evolve_failures",
		"market_loop.execute_evolution",
	}},
	{ID: "discovery", Order: 5, GoQueue: "io", TaskTypes: []string{
		"market_loop.scan_news", "market_loop.dispatch_news_processing_outbox",
	}},
	{ID: "recovery", Order: 6, GoQueue: "recovery", TaskTypes: []string{
		"market_loop.cleanup_model_audits", "market_loop.reconcile_research_leases",
		"market_loop.reconcile_asset_mapping_leases", "market_loop.recover_orphaned_news",
	}},
	{ID: "outcomes", Order: 7, GoQueue: "outcomes", TaskTypes: []string{
		"market_loop.evaluate_outcomes", "market_loop.refresh_event_market_factors",
	}},
	{ID: "masterdata", Order: 8, GoQueue: "masterdata", TaskTypes: []string{
		"market_loop.refresh_crypto_universe", "market_loop.refresh_asset_universe",
		"market_loop.refresh_macro_universe",
	}},
	{ID: "operations", Order: 9, GoQueue: "operations", TaskTypes: []string{
		"market_loop.dispatch_evolve_from_outcomes", "market_loop.monitor_health",
	}},
	{ID: "backfill", Order: 10, GoQueue: "backfill", TaskTypes: []string{
		"market_loop.backfill_asset_mappings",
	}},
	{ID: "maintenance", Order: 11, GoQueue: "maintenance", TaskTypes: []string{
		"market_loop.compact_research_backlog", "market_loop.reprocess_target_impacts_v2", "market_loop.seed_assets",
	}},
}

func WorkerLanes() []MigrationLane {
	lanes := make([]MigrationLane, len(workerLaneDefinitions))
	for index, lane := range workerLaneDefinitions {
		lanes[index] = lane
		lanes[index].TaskTypes = slices.Clone(lane.TaskTypes)
	}
	return lanes
}

func RuntimeStatus() WorkerRuntimeState {
	lanes := WorkerLanes()
	status := WorkerRuntimeState{
		Version: WorkerRuntimeVersion, Runtime: "go", QueueBackend: "postgresql",
		CutoverReady: true, Lanes: lanes, Order: make([]string, 0, len(lanes)),
	}
	for _, lane := range lanes {
		status.Order = append(status.Order, lane.ID)
	}
	return status
}

func RequireWorkerLane(laneID string) (MigrationLane, error) {
	laneID = strings.TrimSpace(laneID)
	if laneID == "" {
		return MigrationLane{}, fmt.Errorf("GO_WORKER_LANE is required")
	}
	for _, lane := range WorkerLanes() {
		if lane.ID == laneID {
			return lane, nil
		}
	}
	return MigrationLane{}, fmt.Errorf("unknown Go worker lane %q", laneID)
}

func ValidateLaneHandlers(lane MigrationLane, handlers map[string]Handler) error {
	missing := make([]string, 0)
	for _, taskType := range lane.TaskTypes {
		if handlers[taskType] == nil {
			missing = append(missing, taskType)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("Go worker lane %s has no handler for: %s", lane.ID, strings.Join(missing, ", "))
	}
	return nil
}
