package httpapi

import (
	"testing"
	"time"
)

func TestModelQueueSnapshotFresh(t *testing.T) {
	now := time.Date(2026, time.August, 31, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		generated any
		want      bool
	}{
		{name: "fresh", generated: now.Add(-5 * time.Second).Format(time.RFC3339Nano), want: true},
		{name: "stale", generated: now.Add(-time.Minute).Format(time.RFC3339Nano), want: false},
		{name: "implausibly future", generated: now.Add(time.Minute).Format(time.RFC3339Nano), want: false},
		{name: "missing", generated: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := map[string]any{"generated_at": tt.generated}
			if got := modelQueueSnapshotFresh(payload, now); got != tt.want {
				t.Fatalf("modelQueueSnapshotFresh() = %v, want %v", got, tt.want)
			}
		})
	}
}
