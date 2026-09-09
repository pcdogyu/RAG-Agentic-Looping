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

func TestSingleResearchWorkerUsesPreferredLane(t *testing.T) {
	if mode := researchClaimMode(true, 1, 0); mode != "preferred" {
		t.Fatalf("single research worker mode=%q", mode)
	}
	if mode := researchClaimMode(true, 2, 0); mode != "fast" {
		t.Fatalf("first dual research worker mode=%q", mode)
	}
	if mode := researchClaimMode(true, 2, 1); mode != "preferred" {
		t.Fatalf("second dual research worker mode=%q", mode)
	}
}
