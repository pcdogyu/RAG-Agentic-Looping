package jobs

import (
	"context"
	"strings"
	"testing"
)

func TestBatchFourMigrationOrderAndLaneMappings(t *testing.T) {
	status := BatchFourMigrationStatus([]string{"extract"})
	if status.Batch != 4 || status.NextLane != "mapping" || status.CutoverReady {
		t.Fatalf("unexpected migration status: %+v", status)
	}
	wantOrder := []string{"extract", "mapping", "research", "evolution"}
	if strings.Join(status.Order, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("unexpected order: %v", status.Order)
	}
	if status.Lanes[1].PythonModelLane != "assist" || status.Lanes[1].CeleryQueuePrefix != "mapping" || status.Lanes[1].GoQueue != "assist" {
		t.Fatalf("mapping aliases are not preserved: %+v", status.Lanes[1])
	}
	if status.Lanes[3].PythonModelLane != "code" || status.Lanes[3].CeleryQueuePrefix != "evolution" || status.Lanes[3].GoQueue != "code" {
		t.Fatalf("evolution aliases are not preserved: %+v", status.Lanes[3])
	}
}

func TestBatchFourRejectsSkippedLaneActivation(t *testing.T) {
	if _, err := ValidateBatchFourActivation("research", []string{"extract"}); err == nil || !strings.Contains(err.Error(), "next lane is mapping") {
		t.Fatalf("expected an out-of-order activation error, got %v", err)
	}
	if status := BatchFourMigrationStatus([]string{"extract", "research"}); status.ConfigurationError == "" {
		t.Fatalf("expected invalid completed-lane configuration: %+v", status)
	}
}

func TestBatchFourWorkerRequiresEveryLaneHandler(t *testing.T) {
	lane, err := ValidateBatchFourActivation("extract", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, map[string]Handler{}); err == nil || !strings.Contains(err.Error(), "extract_news_item") {
		t.Fatalf("expected missing handler gate, got %v", err)
	}
	handlers := map[string]Handler{}
	for _, taskType := range lane.TaskTypes {
		handlers[taskType] = func(_ context.Context, _ Job) (any, error) { return nil, nil }
	}
	if err := ValidateLaneHandlers(lane, handlers); err != nil {
		t.Fatalf("complete lane handlers were rejected: %v", err)
	}
}
