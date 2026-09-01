package jobs

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func stringPointer(value string) *string { return &value }

func timePointer(value time.Time) *time.Time { return &value }

func TestRecoverySchedulerFollowsLaneCutover(t *testing.T) {
	before := NewRecoveryScheduler(config.Config{WorkerCompletedLanes: []string{"extract", "mapping", "research", "evolution", "discovery"}}, nil, nil)
	if before.Enabled() {
		t.Fatal("recovery scheduler enabled before lane cutover")
	}
	after := NewRecoveryScheduler(config.Config{WorkerCompletedLanes: []string{"extract", "mapping", "research", "evolution", "discovery", "recovery"}}, nil, nil)
	if !after.Enabled() {
		t.Fatal("recovery scheduler did not enable after lane cutover")
	}
}

func TestRecoveryScheduleMatchesLegacyCadence(t *testing.T) {
	want := map[string]time.Duration{
		recoverOrphanedNewsTask:     time.Minute,
		reconcileMappingLeasesTask:  time.Minute,
		reconcileResearchLeasesTask: 5 * time.Minute,
		cleanupModelAuditsTask:      24 * time.Hour,
	}
	if len(recoverySchedules) != len(want) {
		t.Fatalf("got %d schedules, want %d", len(recoverySchedules), len(want))
	}
	for _, spec := range recoverySchedules {
		if want[spec.task] != spec.interval {
			t.Fatalf("unexpected schedule for %s: %s", spec.task, spec.interval)
		}
	}
}

func TestShouldRecoverNewsMatchesDurableStateRules(t *testing.T) {
	now := time.Now().UTC()
	staleCutoff := now.Add(-10 * time.Minute)
	tests := []struct {
		name        string
		state       orphanNewsState
		wantRecover bool
		wantStale   bool
	}{
		{name: "missing processing", state: orphanNewsState{}, wantRecover: true},
		{name: "already in event", state: orphanNewsState{Processed: true}},
		{name: "cancelled", state: orphanNewsState{Status: stringPointer("cancelled")}},
		{name: "fresh running", state: orphanNewsState{Status: stringPointer("running"), Heartbeat: timePointer(now)}, wantRecover: false},
		{name: "stale running", state: orphanNewsState{Status: stringPointer("running"), Heartbeat: timePointer(now.Add(-11 * time.Minute))}, wantRecover: true, wantStale: true},
		{name: "pending outbox", state: orphanNewsState{Status: stringPointer("dispatch_pending"), OutboxStatus: stringPointer("pending")}},
		{name: "failed extraction", state: orphanNewsState{Status: stringPointer("extraction_failed")}, wantRecover: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recover, stale := shouldRecoverNews(test.state, staleCutoff)
			if recover != test.wantRecover || stale != test.wantStale {
				t.Fatalf("got recover=%v stale=%v, want recover=%v stale=%v", recover, stale, test.wantRecover, test.wantStale)
			}
		})
	}
}

func TestRecoveryMappingStateDoesNotStrandCompletedMapping(t *testing.T) {
	now := time.Now().UTC()
	queued := analysisStep("asset_mapping_queue", "queued", "go-worker", "queued", nil)
	queued["occurred_at"] = iso(now.Add(-time.Minute))
	running := analysisStep("asset_mapping", "running", "go-worker", "running", nil)
	running["occurred_at"] = iso(now)
	event := map[string]any{"analysis_steps": []any{queued, running}, "candidates": []any{}}
	if !recoveryMappingActive(event) || recoveryMappingTerminal(event) {
		t.Fatal("running mapping must remain active")
	}

	completed := analysisStep("asset_mapping", "unmapped", "go-worker", "completed", nil)
	completed["occurred_at"] = iso(now)
	event["analysis_steps"] = []any{queued, completed}
	if recoveryMappingActive(event) || !recoveryMappingTerminal(event) {
		t.Fatal("completed unmapped event must proceed to event research")
	}
}
