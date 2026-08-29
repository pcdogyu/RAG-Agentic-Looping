package jobs

import (
	"testing"
	"time"
)

func TestIntervalUsesPostgresCompatibleSeconds(t *testing.T) {
	if got := interval(1500 * time.Millisecond); got != "1.500000 seconds" {
		t.Fatalf("unexpected interval: %s", got)
	}
}
