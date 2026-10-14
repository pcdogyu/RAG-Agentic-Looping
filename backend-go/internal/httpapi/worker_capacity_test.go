package httpapi

import "testing"

func TestLiveWorkerCapacitiesSumsLiveWorkerHeartbeats(t *testing.T) {
	rows := []workerCapacityRow{
		{Queues: []string{"extract"}, Concurrency: 4},
		{Queues: []string{" extract ", "assist"}, Concurrency: 2},
		{Queues: []string{"research"}, Concurrency: 0},
		{Queues: []string{" ", ""}, Concurrency: 3},
	}
	capacities := liveWorkerCapacities(rows)
	if capacities["extract"] != 6 || capacities["assist"] != 2 {
		t.Fatalf("unexpected advertised capacities: %#v", capacities)
	}
	if _, found := capacities["research"]; found {
		t.Fatalf("zero concurrency worker must not advertise capacity: %#v", capacities)
	}
	if _, found := capacities[" "]; found {
		t.Fatalf("blank queue name must be ignored: %#v", capacities)
	}
	if empty := liveWorkerCapacities(nil); empty == nil || len(empty) != 0 {
		t.Fatalf("expected empty non-nil capacity map, got %#v", empty)
	}
}

func TestResolveLaneCapacityPrefersAdvertisedWorkerConcurrency(t *testing.T) {
	observed := map[string]int{"extract": 4, "assist": 0}
	if got := resolveLaneCapacity(observed, "extract", 1); got != 4 {
		t.Fatalf("advertised worker capacity must win, got %d", got)
	}
	if got := resolveLaneCapacity(observed, "assist", 2); got != 2 {
		t.Fatalf("zero advertised capacity must fall back to configured ceiling %d, got %d", 2, got)
	}
	if got := resolveLaneCapacity(observed, "research", 3); got != 3 {
		t.Fatalf("missing lane must fall back to configured ceiling %d, got %d", 3, got)
	}
	if got := resolveLaneCapacity(nil, "code", 0); got != 1 {
		t.Fatalf("missing lane without configured ceiling must stay at 1, got %d", got)
	}
}
