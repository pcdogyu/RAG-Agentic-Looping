package jobs

import (
	"strings"
	"testing"
)

func TestRuntimeStatusIsGoOnlyAndComplete(t *testing.T) {
	status := RuntimeStatus()
	if status.Runtime != "go" || status.QueueBackend != "postgresql" || !status.CutoverReady {
		t.Fatalf("unexpected runtime status: %+v", status)
	}
	if status.Version != WorkerRuntimeVersion || len(status.Lanes) != 11 || len(status.Order) != len(status.Lanes) {
		t.Fatalf("unexpected lane manifest: %+v", status)
	}
	wantQueues := map[string]string{
		"extract": "extract", "mapping": "assist", "research": "research", "evolution": "code",
		"discovery": "io", "recovery": "recovery", "outcomes": "outcomes", "masterdata": "masterdata",
		"operations": "operations", "backfill": "backfill", "maintenance": "maintenance",
	}
	for index, lane := range status.Lanes {
		if lane.Order != index+1 || status.Order[index] != lane.ID || lane.GoQueue != wantQueues[lane.ID] || len(lane.TaskTypes) == 0 {
			t.Fatalf("invalid lane %d: %+v", index, lane)
		}
	}
}

func TestRequireWorkerLaneRejectsMissingAndUnknownLane(t *testing.T) {
	if _, err := RequireWorkerLane(""); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("missing lane error=%v", err)
	}
	if _, err := RequireWorkerLane("invalid-lane"); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown lane error=%v", err)
	}
	lane, err := RequireWorkerLane("extract")
	if err != nil || lane.GoQueue != "extract" {
		t.Fatalf("extract lane=%+v err=%v", lane, err)
	}
}

func TestValidateLaneHandlersRejectsMissingHandler(t *testing.T) {
	lane, err := RequireWorkerLane("extract")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, map[string]Handler{}); err == nil {
		t.Fatal("missing handlers were accepted")
	}
}
