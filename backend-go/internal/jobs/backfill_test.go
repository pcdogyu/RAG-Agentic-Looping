package jobs

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestBackfillHandlersCoverLane(t *testing.T) {
	completed := []string{"extract", "mapping", "research", "evolution", "discovery", "recovery", "outcomes", "masterdata", "operations"}
	lane, err := ValidateBatchFourActivation("backfill", completed)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewBackfillHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatalf("backfill handlers are incomplete: %v", err)
	}
}

func TestMappingIsActiveMatchesLegacyQueueOrdering(t *testing.T) {
	stamp := time.Now().UTC()
	event := map[string]any{"analysis_steps": []any{
		analysisStep("asset_mapping", "completed", "test", "old", map[string]any{}),
		analysisStep("asset_mapping_queue", "queued", "test", "new", map[string]any{}),
	}}
	steps := anySlice(event["analysis_steps"])
	objectValue(steps[0])["occurred_at"] = iso(stamp.Add(-time.Minute))
	objectValue(steps[1])["occurred_at"] = iso(stamp)
	if !mappingIsActive(event) {
		t.Fatal("newer queued mapping should be active")
	}
	objectValue(steps[0])["occurred_at"] = iso(stamp.Add(time.Minute))
	if mappingIsActive(event) {
		t.Fatal("completed mapping newer than queue should not be active")
	}
	objectValue(steps[0])["status"] = "retrying"
	if !mappingIsActive(event) {
		t.Fatal("retrying mapping should remain active")
	}
}

func TestBackfillContinuationKeepsCursorAndProgress(t *testing.T) {
	runtime := &ExtractRuntime{}
	progress := backfillProgress{Scanned: 10, Queued: 3, Skipped: 6, Failed: 1}
	err := runtime.backfillContinuation(7, time.Unix(100, 0).UTC(), map[string]any{"cursor_id": "event"}, progress, 2*time.Second, "dispatching", nil)
	continuation, ok := err.(*continuationError)
	if !ok || continuation.Delay != 2*time.Second {
		t.Fatalf("unexpected continuation: %#v", err)
	}
	envelope := continuation.Payload.(taskEnvelope)
	if stringValue(envelope.Kwargs["cursor_id"]) != "event" || decodeBackfillProgress(envelope.Kwargs["stats"]) != progress {
		t.Fatalf("continuation lost state: %#v", envelope.Kwargs)
	}
}
