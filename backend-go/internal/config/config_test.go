package config

import (
	"testing"
	"time"
)

func TestRecentResearchFilterDefaultsToFortyEightHours(t *testing.T) {
	t.Setenv("RECENT_RESEARCH_FILTER_HOURS", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.RecentResearchFilter; got != 48*time.Hour {
		t.Fatalf("recent research filter = %s, want 48h", got)
	}
	t.Setenv("RECENT_RESEARCH_FILTER_HOURS", "47")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.RecentResearchFilter; got != 47*time.Hour {
		t.Fatalf("configured recent research filter = %s, want 47h", got)
	}
}
