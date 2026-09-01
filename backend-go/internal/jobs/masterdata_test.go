package jobs

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestMasterdataHandlersCoverMigrationManifest(t *testing.T) {
	completed := []string{"extract", "mapping", "research", "evolution", "discovery", "recovery", "outcomes"}
	lane, err := ValidateBatchFourActivation("masterdata", completed)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewMasterdataHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatalf("masterdata handlers are incomplete: %v", err)
	}
}

func TestMasterdataSchedulerFollowsLaneCutover(t *testing.T) {
	completed := []string{"extract", "mapping", "research", "evolution", "discovery", "recovery", "outcomes"}
	if NewMasterdataScheduler(config.Config{WorkerCompletedLanes: completed}, nil, nil).Enabled() {
		t.Fatal("masterdata scheduler enabled before lane cutover")
	}
	if !NewMasterdataScheduler(config.Config{WorkerCompletedLanes: append(completed, "masterdata")}, nil, nil).Enabled() {
		t.Fatal("masterdata scheduler did not enable after lane cutover")
	}
}

func TestMasterdataScheduleMatchesLegacyCadence(t *testing.T) {
	want := map[string]time.Duration{
		refreshCryptoUniverseTask: 6 * time.Hour,
		refreshAssetUniverseTask:  24 * time.Hour,
		refreshMacroUniverseTask:  24 * time.Hour,
	}
	if len(masterdataSchedules) != len(want) {
		t.Fatalf("got %d schedules, want %d", len(masterdataSchedules), len(want))
	}
	if masterdataSchedules[0].task != refreshAssetUniverseTask {
		t.Fatal("daily full refresh must claim the shared crypto interval first")
	}
	for _, spec := range masterdataSchedules {
		if want[spec.task] != spec.interval {
			t.Fatalf("unexpected schedule for %s: %s", spec.task, spec.interval)
		}
	}
}

func TestValidateMarketSnapshotRejectsPartialDuplicateAndNonADR(t *testing.T) {
	valid := masterAsset{ID: "equity:NASDAQ:ONE", Class: "equity", Market: "US", Symbol: "ONE", Name: "One", Exchange: "NASDAQ", Currency: "USD", Instrument: "common_stock"}
	if err := validateMarketSnapshot("US", []masterAsset{valid}, 1); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	if err := validateMarketSnapshot("US", []masterAsset{valid}, 2); err == nil {
		t.Fatal("partial snapshot was accepted")
	}
	if err := validateMarketSnapshot("US", []masterAsset{valid, valid}, 1); err == nil {
		t.Fatal("duplicate snapshot was accepted")
	}
	otc := valid
	otc.ID, otc.Symbol, otc.Exchange = "equity:OTC:ONEF", "ONEF", "OTC"
	if err := validateMarketSnapshot("US", []masterAsset{otc}, 1); err == nil {
		t.Fatal("non-ADR OTC identity was accepted")
	}
}

func TestNormalizeMasterIndustryPrefersLongestDetailedAlias(t *testing.T) {
	rules := []taxonomyRule{
		{ID: "sector:information_technology", Level: 1, Terms: []string{"Technology"}},
		{ID: "industry:diversified_technology", Parent: "sector:information_technology", Level: 2, Terms: []string{"technology"}},
		{ID: "industry:semiconductors", Parent: "sector:information_technology", Level: 2, Terms: []string{"semiconductor equipment"}},
	}
	sector, industry := normalizeMasterIndustry("Technology", "Semiconductor Equipment & Materials", rules)
	if sector != "sector:information_technology" || industry != "industry:semiconductors" {
		t.Fatalf("unexpected taxonomy match: %s %s", sector, industry)
	}
}

func TestCryptoManualOnlyMatchesStableWrappedAndOrdinaryCoins(t *testing.T) {
	if !cryptoManualOnly("tether", "USDT", "Tether") || !cryptoManualOnly("wrapped-bitcoin", "WBTC", "Wrapped Bitcoin") {
		t.Fatal("stable/wrapped assets must be manual-only")
	}
	if cryptoManualOnly("bitcoin", "BTC", "Bitcoin") {
		t.Fatal("ordinary ranked coin became manual-only")
	}
}

func TestMergeStoredMasterAssetPreservesManualAndCuratedMetadata(t *testing.T) {
	manualIndustry, manualTier, manualActive := "industry:software", "exact_only", false
	issuer := "curated:issuer"
	asset := masterAsset{Aliases: []string{"new"}, Industry: "industry:semiconductors", Sector: "sector:information_technology", AssociationTier: "standard", Active: true}
	mergeStoredMasterAsset(&asset, storedMasterAsset{Aliases: []string{"old"}, ManualIndustry: &manualIndustry, ManualSector: "sector:information_technology", ManualAssociation: &manualTier, ManualActive: &manualActive, IssuerID: &issuer})
	if asset.Industry != manualIndustry || asset.Sector != "sector:information_technology" || asset.AssociationTier != "standard" || asset.Active || asset.IssuerID != issuer {
		t.Fatalf("manual metadata was not preserved: %#v", asset)
	}
	if len(asset.Aliases) != 2 || asset.Aliases[0] != "old" || asset.Aliases[1] != "new" {
		t.Fatalf("aliases were not merged: %#v", asset.Aliases)
	}
}
