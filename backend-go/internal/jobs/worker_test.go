package jobs

import (
	"context"
	"testing"
)

func TestWorkerDrainContextSurvivesShutdownCancellation(t *testing.T) {
	claimCtx, stopClaims := context.WithCancel(context.WithValue(context.Background(), "lane", "research"))
	drainCtx := (&Worker{DrainOnShutdown: true}).executionContext(claimCtx)
	stopClaims()

	select {
	case <-claimCtx.Done():
	default:
		t.Fatal("claim context was not cancelled")
	}
	if err := drainCtx.Err(); err != nil {
		t.Fatalf("drain context was cancelled with active work: %v", err)
	}
	if value := drainCtx.Value("lane"); value != "research" {
		t.Fatalf("drain context lost request values: %#v", value)
	}
}

func TestWorkerWithoutDrainUsesShutdownContext(t *testing.T) {
	claimCtx, stopClaims := context.WithCancel(context.Background())
	defer stopClaims()
	if executionCtx := (&Worker{}).executionContext(claimCtx); executionCtx != claimCtx {
		t.Fatal("non-draining worker must keep the shutdown context")
	}
}
