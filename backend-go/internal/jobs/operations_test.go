package jobs

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestOperationsHandlersCoverLane(t *testing.T) {
	completed := []string{"extract", "mapping", "research", "evolution", "discovery", "recovery", "outcomes", "masterdata"}
	lane, err := ValidateBatchFourActivation("operations", completed)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewOperationsHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatalf("operations handlers are incomplete: %v", err)
	}
}

func TestOperationsSchedulerRequiresCutoverAndEvolution(t *testing.T) {
	completed := []string{"extract", "mapping", "research", "evolution", "discovery", "recovery", "outcomes", "masterdata"}
	if NewOperationsScheduler(config.Config{EvolutionEnabled: true, WorkerCompletedLanes: completed}, nil, nil).Enabled() {
		t.Fatal("operations scheduler enabled before lane cutover")
	}
	completed = append(completed, "operations")
	if NewOperationsScheduler(config.Config{WorkerCompletedLanes: completed}, nil, nil).Enabled() {
		t.Fatal("operations scheduler enabled while evolution is disabled")
	}
	if !NewOperationsScheduler(config.Config{EvolutionEnabled: true, WorkerCompletedLanes: completed}, nil, nil).Enabled() {
		t.Fatal("operations scheduler did not enable after cutover")
	}
}

func TestOperationsScheduleMatchesLegacyCadenceWithoutImmediateEvolution(t *testing.T) {
	want := map[string]time.Duration{dispatchEvolutionTask: 7 * 24 * time.Hour, monitorHealthTask: 5 * time.Minute}
	if len(operationsSchedules) != len(want) {
		t.Fatalf("got %d schedules, want %d", len(operationsSchedules), len(want))
	}
	for _, spec := range operationsSchedules {
		if want[spec.task] != spec.interval {
			t.Fatalf("unexpected cadence for %s: %s", spec.task, spec.interval)
		}
		if spec.task == dispatchEvolutionTask && !spec.startDelay {
			t.Fatal("weekly evolution must not run immediately at cutover")
		}
		if spec.task == monitorHealthTask && spec.startDelay {
			t.Fatal("health monitoring must start immediately")
		}
	}
}

func TestCalculateHealthPreservesLegacyThresholds(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-29 * time.Minute)
	snapshot := calculateHealth(9, 1, fresh, now, 10*time.Minute)
	if snapshot.unhealthy() || snapshot.FailureRate != .10 || snapshot.Samples != 10 || snapshot.DataStale {
		t.Fatalf("ten-percent boundary must remain healthy: %+v", snapshot)
	}
	snapshot = calculateHealth(8, 2, fresh, now, 10*time.Minute)
	if !snapshot.unhealthy() || snapshot.FailureRate != .20 {
		t.Fatalf("failure-rate gate did not trigger: %+v", snapshot)
	}
	exact := calculateHealth(10, 0, now.Add(-30*time.Minute), now, 10*time.Minute)
	if exact.DataStale {
		t.Fatalf("exactly three scan intervals must remain fresh: %+v", exact)
	}
	stale := calculateHealth(0, 0, now.Add(-30*time.Minute-time.Second), now, 10*time.Minute)
	if !stale.DataStale || !stale.unhealthy() {
		t.Fatalf("stale news must trigger the health gate: %+v", stale)
	}
}

func TestCalculateHealthTreatsMissingNewsAsNotStale(t *testing.T) {
	snapshot := calculateHealth(0, 0, time.Time{}, time.Now(), 10*time.Minute)
	if snapshot.DataStale || snapshot.unhealthy() {
		t.Fatalf("legacy monitor does not flag an empty news table: %+v", snapshot)
	}
}
