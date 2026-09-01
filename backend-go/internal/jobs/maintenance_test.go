package jobs

import (
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestMaintenanceHandlersCoverLane(t *testing.T) {
	completed := []string{"extract", "mapping", "research", "evolution", "discovery", "recovery", "outcomes", "masterdata", "operations", "backfill"}
	lane, err := ValidateBatchFourActivation("maintenance", completed)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewMaintenanceHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatalf("maintenance handlers are incomplete: %v", err)
	}
}

func TestCurrentTargetImpactScoringAndActiveStatuses(t *testing.T) {
	if !eventRunHasCurrentScoring(map[string]any{"report": map[string]any{"scoring_version": currentEventScoringVersion}}) {
		t.Fatal("current Go event scoring was not recognized")
	}
	if eventRunHasCurrentScoring(map[string]any{"report": map[string]any{"scoring_version": "target-transmission-v2"}}) {
		t.Fatal("legacy target scoring was treated as current")
	}
	for _, status := range []string{"queued", "running", "verifying"} {
		if !activeResearchStatus(status) {
			t.Fatalf("status %s should be active", status)
		}
	}
	if activeResearchStatus("failed") || activeResearchStatus("completed") {
		t.Fatal("terminal research status was treated as active")
	}
}

func TestCuratedSeedAssetsMatchLegacyIdentitySet(t *testing.T) {
	assets := curatedSeedAssets()
	if len(assets) != 14 {
		t.Fatalf("got %d curated assets, want 14", len(assets))
	}
	byID := map[string]masterAsset{}
	for _, asset := range assets {
		byID[asset.ID] = asset
		if !asset.Active || asset.AssociationTier != "standard" {
			t.Fatalf("seed asset is not active standard identity: %+v", asset)
		}
	}
	hk, us := byID["equity:XHKG:09988"], byID["equity:NYSE:BABA"]
	if hk.IssuerID != "curated:alibaba-group" || us.IssuerID != hk.IssuerID || !containsString(hk.Products, "阿里云") || !containsString(us.Products, "Alibaba Cloud") {
		t.Fatalf("Alibaba product ownership or cross-listing identity is incomplete: hk=%+v us=%+v", hk, us)
	}
}
