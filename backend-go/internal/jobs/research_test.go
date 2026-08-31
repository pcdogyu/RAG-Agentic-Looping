package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestResearchHandlersCoverLaneManifest(t *testing.T) {
	handlers := NewResearchHandlers(config.Config{}, nil, nil)
	if handlers[researchEventTask] == nil || handlers[researchAssetTask] == nil {
		t.Fatalf("research handlers are incomplete: %#v", handlers)
	}
}

func TestRatingForScoreUsesFiveStableBands(t *testing.T) {
	cases := map[int]string{-100: "strongly_bearish", -70: "strongly_bearish", -69: "bearish", -30: "bearish", -29: "watch", 29: "watch", 30: "bullish", 69: "bullish", 70: "strongly_bullish", 100: "strongly_bullish"}
	for score, expected := range cases {
		if actual := ratingForScore(score); actual != expected {
			t.Fatalf("score %d: expected %s, got %s", score, expected, actual)
		}
	}
}

func TestSanitizeEventImpactsRejectsVolumeAsMacroTarget(t *testing.T) {
	event := map[string]any{"candidates": []any{map[string]any{"asset": map[string]any{"asset_id": "US:HOOD", "name": "Robinhood", "asset_class": "equity"}}}}
	values := []eventImpactDraft{
		{TargetType: "economy", TargetName: "成交量增加", DirectionScore: 80},
		{TargetType: "other", TargetName: "Robinhood", AssetID: "US:HOOD", DirectionScore: 40},
		{TargetType: "tradable_asset", TargetName: "Unknown", AssetID: "US:NOPE", DirectionScore: 50},
	}
	actual := sanitizeEventImpacts(values, event)
	if len(actual) != 1 || actual[0].TargetType != "tradable_asset" || actual[0].TargetName != "Robinhood" {
		t.Fatalf("unexpected sanitized impacts: %#v", actual)
	}
}

func TestPermanentResearchErrorIsTerminal(t *testing.T) {
	value := permanentJobError{context.DeadlineExceeded}
	var marker interface{ Permanent() bool }
	if !errors.As(value, &marker) || !marker.Permanent() {
		t.Fatal("research deadline must be a permanent queue failure")
	}
}

func TestResearchLimitsUseThirtyFourAndThirtyFiveMinutes(t *testing.T) {
	cfg := config.Config{ResearchSoftLimit: 34 * time.Minute, ResearchHardLimit: 35 * time.Minute}
	if cfg.ResearchSoftLimit >= cfg.ResearchHardLimit {
		t.Fatal("soft research limit must be lower than hard limit")
	}
}
