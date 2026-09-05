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

func TestResearchThinkingLimitsUseConfiguredDefaults(t *testing.T) {
	t.Setenv("OLLAMA_RESEARCH_CONTEXT_LENGTH", "")
	t.Setenv("OLLAMA_RESEARCH_MAX_OUTPUT_TOKENS", "")
	t.Setenv("OLLAMA_RESEARCH_FALLBACK_MAX_OUTPUT_TOKENS", "")
	t.Setenv("OLLAMA_RESEARCH_THINK", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ResearchContextLength != 32768 || cfg.ResearchMaxOutput != 16384 || cfg.ResearchFallbackMax != 8192 || !cfg.ResearchThink {
		t.Fatalf("unexpected research limits: context=%d primary=%d fallback=%d think=%v", cfg.ResearchContextLength, cfg.ResearchMaxOutput, cfg.ResearchFallbackMax, cfg.ResearchThink)
	}
}

func TestResearchHistoryDefaultsToNinetyDaysAndTwentyItems(t *testing.T) {
	t.Setenv("RESEARCH_HISTORY_WINDOW_DAYS", "")
	t.Setenv("RESEARCH_HISTORY_MAX_ITEMS", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ResearchHistoryWindow != 90*24*time.Hour || cfg.ResearchHistoryItems != 20 {
		t.Fatalf("unexpected research history config: window=%s items=%d", cfg.ResearchHistoryWindow, cfg.ResearchHistoryItems)
	}
}
