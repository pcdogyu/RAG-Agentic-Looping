package evaluation

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func price(value float64, day int, adjusted bool) PricePoint {
	stamp := time.Date(2025, 1, day, 21, 0, 0, 0, time.UTC)
	return PricePoint{SessionDate: stamp, AvailableAt: stamp, Close: &value, AdjustedClose: &value, Currency: "USD", CorporateActionAdjusted: adjusted}
}

func outcomePolicy(t *testing.T, objective string, horizon int) HorizonPolicy {
	t.Helper()
	policy, err := ResolveHorizonPolicy(objective, horizon)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestOutcomeStartsAfterSignalAndKeepsAbsoluteRelativeAndNetReturns(t *testing.T) {
	asset := []PricePoint{price(100, 2, true), price(105, 3, true), price(110, 4, true)}
	benchmark := []PricePoint{price(100, 2, true), price(102, 3, true), price(103, 4, true)}
	asset[0].TradabilityStatus, asset[1].TradabilityStatus = "tradable", "tradable"
	policy := outcomePolicy(t, "excess_up", 1)
	policy.ExecutionEnabled, policy.ExecutionSide, policy.ExecutionCostBPS = true, "long", 10
	result := BuildOutcomeLabel(time.Date(2025, 1, 2, 20, 0, 0, 0, time.UTC), asset, benchmark, policy)
	if result.Status != "mature" || result.RawReturn == nil || result.BenchmarkReturn == nil || result.ExcessReturn == nil || result.NetReturn == nil {
		t.Fatalf("result=%#v", result)
	}
	if *result.RawReturn <= 0 || *result.ExcessReturn <= 0 || *result.NetReturn >= *result.RawReturn {
		t.Fatalf("returns=%#v", result)
	}
}

func TestOutcomeRejectsUnadjustedCorporateActionSeries(t *testing.T) {
	result := BuildOutcomeLabel(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), []PricePoint{price(100, 2, false), price(50, 3, false)}, nil, outcomePolicy(t, "absolute_up", 1))
	if result.Status != "unavailable" || result.Reason != "adjusted_close_missing" {
		t.Fatalf("result=%#v", result)
	}
}

func TestOutcomeUsesAdjustedCloseAcrossSplitInsteadOfFalseRawPriceCrash(t *testing.T) {
	entryAt := time.Date(2025, 1, 2, 21, 0, 0, 0, time.UTC)
	exitAt := time.Date(2025, 1, 3, 21, 0, 0, 0, time.UTC)
	rawEntry, adjustedEntry, rawExit, adjustedExit := 100.0, 50.0, 51.0, 51.0
	asset := []PricePoint{
		{SessionDate: entryAt, AvailableAt: entryAt, Close: &rawEntry, AdjustedClose: &adjustedEntry, Currency: "USD", CorporateActionAdjusted: true},
		{SessionDate: exitAt, AvailableAt: exitAt, Close: &rawExit, AdjustedClose: &adjustedExit, Currency: "USD", CorporateActionAdjusted: true},
	}
	result := BuildOutcomeLabel(entryAt.Add(-time.Hour), asset, nil, outcomePolicy(t, "absolute_up", 1))
	if result.Status != "mature" || result.RawReturn == nil || math.Abs(*result.RawReturn-.02) > 1e-9 {
		t.Fatalf("split-adjusted result=%#v", result)
	}
}

func TestOutcomeUsesTotalReturnAdjustedCloseAcrossCashDividend(t *testing.T) {
	entryAt := time.Date(2025, 2, 3, 21, 0, 0, 0, time.UTC)
	exitAt := time.Date(2025, 2, 4, 21, 0, 0, 0, time.UTC)
	// The one-dollar ex-dividend price drop is not an economic loss. A
	// total-return-adjusted series keeps the two observations comparable.
	rawEntry, adjustedEntry, rawExit, adjustedExit := 100.0, 99.0, 99.0, 99.0
	asset := []PricePoint{
		{SessionDate: entryAt, AvailableAt: entryAt, Close: &rawEntry, AdjustedClose: &adjustedEntry, Currency: "USD", CorporateActionAdjusted: true},
		{SessionDate: exitAt, AvailableAt: exitAt, Close: &rawExit, AdjustedClose: &adjustedExit, Currency: "USD", CorporateActionAdjusted: true},
	}
	result := BuildOutcomeLabel(entryAt.Add(-time.Hour), asset, nil, outcomePolicy(t, "absolute_up", 1))
	if result.Status != "mature" || result.RawReturn == nil || math.Abs(*result.RawReturn) > 1e-12 {
		t.Fatalf("dividend-adjusted result=%#v", result)
	}
}

func TestOutcomeEntryMustBeStrictlyAfterSignalAvailability(t *testing.T) {
	signal := time.Date(2025, 1, 2, 21, 0, 0, 0, time.UTC)
	asset := []PricePoint{price(100, 2, true), price(105, 3, true), price(110, 4, true)}
	result := BuildOutcomeLabel(signal, asset, nil, outcomePolicy(t, "absolute_up", 1))
	if result.Status != "mature" || result.EntryAt == nil || !result.EntryAt.Equal(time.Date(2025, 1, 3, 21, 0, 0, 0, time.UTC)) || result.RawReturn == nil || *result.RawReturn <= 0 {
		t.Fatalf("same-time price was incorrectly used as executable entry: %#v", result)
	}
}

func TestAbsoluteGainCanBeRelativeUnderperformanceWithoutExecutionClaim(t *testing.T) {
	asset := []PricePoint{price(100, 2, true), price(102, 3, true)}
	benchmark := []PricePoint{price(100, 2, true), price(108, 3, true)}
	result := BuildOutcomeLabel(time.Date(2025, 1, 1, 23, 0, 0, 0, time.UTC), asset, benchmark, outcomePolicy(t, "excess_up", 1))
	if result.Status != "mature" || result.AbsoluteLabel != "up" || result.RelativeLabel != "underperform" || result.ObjectiveLabel != "underperform" || result.ExcessReturn == nil || math.Abs(*result.ExcessReturn+.06) > 1e-12 {
		t.Fatalf("absolute and relative truth was collapsed: %#v", result)
	}
	if result.NetReturn != nil || result.SimulationStatus != "not_configured" || !result.ResearchResultOnly {
		t.Fatalf("unconfigured execution was presented as realizable: %#v", result)
	}
}

func TestExecutionSimulationRequiresExplicitApplicableCostAssumptions(t *testing.T) {
	base := outcomePolicy(t, "absolute_up", 1)
	if _, err := WithExecutionAssumptions(base, ExecutionAssumptions{Enabled: true, Side: "long", BorrowBPS: 5}); err == nil {
		t.Fatal("long simulation accepted an inapplicable borrow cost")
	}
	policy, err := WithExecutionAssumptions(base, ExecutionAssumptions{Enabled: true, Side: "short", RoundTripCostBPS: 10, SlippageBPS: 5, BorrowBPS: 4, FundingBPS: 1})
	if err != nil {
		t.Fatal(err)
	}
	asset := []PricePoint{price(100, 2, true), price(102, 3, true)}
	asset[0].TradabilityStatus, asset[1].TradabilityStatus = "tradable", "tradable"
	result := BuildOutcomeLabel(time.Date(2025, 1, 1, 23, 0, 0, 0, time.UTC), asset, nil, policy)
	if result.Status != "mature" || result.GrossStrategyReturn == nil || math.Abs(*result.GrossStrategyReturn+.02) > 1e-12 || result.NetReturn == nil || math.Abs(*result.NetReturn+.022) > 1e-12 || result.ExecutionCostBPS == nil || *result.ExecutionCostBPS != 20 {
		t.Fatalf("explicit short costs were not applied independently: %#v", result)
	}
}

func TestExecutionSimulationRequiresExplicitEntryAndExitTradability(t *testing.T) {
	policy, err := WithExecutionAssumptions(outcomePolicy(t, "absolute_up", 1), ExecutionAssumptions{Enabled: true, Side: "long", RoundTripCostBPS: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, entry, exit string
	}{
		{name: "unknown entry", entry: "", exit: "tradable"},
		{name: "unknown exit", entry: "tradable", exit: "unknown"},
		{name: "suspended entry", entry: "suspended", exit: "tradable"},
		{name: "limit up entry", entry: "limit_up", exit: "tradable"},
		{name: "limit down exit", entry: "tradable", exit: "limit_down"},
	} {
		t.Run(test.name, func(t *testing.T) {
			asset := []PricePoint{price(100, 2, true), price(102, 3, true)}
			asset[0].TradabilityStatus, asset[1].TradabilityStatus = test.entry, test.exit
			result := BuildOutcomeLabel(time.Date(2025, 1, 1, 23, 0, 0, 0, time.UTC), asset, nil, policy)
			if result.Status != "mature" || result.RawReturn == nil || result.NetReturn != nil || result.GrossStrategyReturn != nil || result.ExecutionCostBPS != nil || result.SimulationStatus != "unavailable_tradability_evidence" || !result.ResearchResultOnly {
				t.Fatalf("non-tradable close became an executable result: %#v", result)
			}
		})
	}
}

func TestRelativeLabelRequiresBenchmarkOnTheExactAssetSessions(t *testing.T) {
	asset := []PricePoint{price(100, 2, true), price(102, 3, true)}
	benchmark := []PricePoint{price(100, 2, true), price(108, 4, true)}
	relative := BuildOutcomeLabel(time.Date(2025, 1, 1, 23, 0, 0, 0, time.UTC), asset, benchmark, outcomePolicy(t, "excess_up", 1))
	if relative.Status != "unavailable" || relative.Reason != "benchmark_unavailable_or_session_mismatch" || relative.RawReturn == nil || relative.AbsoluteLabel != "up" {
		t.Fatalf("misaligned relative target did not preserve only the known absolute result: %#v", relative)
	}
	absolute := BuildOutcomeLabel(time.Date(2025, 1, 1, 23, 0, 0, 0, time.UTC), asset, benchmark, outcomePolicy(t, "absolute_up", 1))
	if absolute.Status != "mature" || absolute.ObjectiveLabel != "up" || absolute.RelativeLabel != "unavailable" {
		t.Fatalf("optional benchmark mismatch blocked an absolute label: %#v", absolute)
	}
}

func TestOneFiveAndTwentySessionLabelsMatureIndependently(t *testing.T) {
	start := time.Date(2025, 3, 1, 21, 0, 0, 0, time.UTC)
	points := make([]PricePoint, 21)
	for index := range points {
		value := 100.0 + float64(index)
		stamp := start.AddDate(0, 0, index+1)
		points[index] = PricePoint{SessionDate: stamp, AvailableAt: stamp, AdjustedClose: &value, Currency: "USD", CorporateActionAdjusted: true}
	}
	for _, horizon := range []int{1, 5, 20} {
		available := points[:horizon]
		pending := BuildOutcomeLabel(start, available, nil, outcomePolicy(t, "absolute_up", horizon))
		if pending.Status != "unavailable" || pending.Reason != "label_not_mature" {
			t.Fatalf("horizon %d matured early: %#v", horizon, pending)
		}
		mature := BuildOutcomeLabel(start, points[:horizon+1], nil, outcomePolicy(t, "absolute_up", horizon))
		if mature.Status != "mature" || mature.ExitAt == nil || !mature.ExitAt.Equal(points[horizon].SessionDate) {
			t.Fatalf("horizon %d did not mature independently: %#v", horizon, mature)
		}
	}
}

func TestLaterBackfillDoesNotTurnPreSignalSessionIntoEntry(t *testing.T) {
	signal := time.Date(2025, 1, 3, 23, 0, 0, 0, time.UTC)
	fridayValue, mondayValue, tuesdayValue := 100.0, 101.0, 102.0
	backfilledFriday := PricePoint{SessionDate: time.Date(2025, 1, 3, 21, 0, 0, 0, time.UTC), AvailableAt: time.Date(2025, 1, 7, 22, 0, 0, 0, time.UTC), AdjustedClose: &fridayValue, Currency: "USD", CorporateActionAdjusted: true}
	monday := PricePoint{SessionDate: time.Date(2025, 1, 6, 21, 0, 0, 0, time.UTC), AvailableAt: time.Date(2025, 1, 6, 21, 1, 0, 0, time.UTC), AdjustedClose: &mondayValue, Currency: "USD", CorporateActionAdjusted: true}
	tuesday := PricePoint{SessionDate: time.Date(2025, 1, 7, 21, 0, 0, 0, time.UTC), AvailableAt: time.Date(2025, 1, 7, 21, 1, 0, 0, time.UTC), AdjustedClose: &tuesdayValue, Currency: "USD", CorporateActionAdjusted: true}
	result := BuildOutcomeLabel(signal, []PricePoint{backfilledFriday, monday, tuesday}, nil, outcomePolicy(t, "absolute_up", 1))
	if result.Status != "mature" || result.EntryAt == nil || !result.EntryAt.Equal(monday.SessionDate) {
		t.Fatalf("pre-signal backfill became an executable entry: %#v", result)
	}
}

func TestWalkForwardSeparatesMatureTrainCalibrationAndFutureTest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	records := []Record{}
	for i := 0; i < 180; i++ {
		signal := start.AddDate(0, 0, i)
		records = append(records, Record{ID: fmtID(i), EventCluster: fmtID(i), SignalAt: signal, LabelMatureAt: signal.AddDate(0, 0, 2)})
	}
	folds, err := WalkForward(records, 60*24*time.Hour, 30*24*time.Hour, 30*24*time.Hour, 30*24*time.Hour, 2*24*time.Hour)
	if err != nil || len(folds) == 0 {
		t.Fatalf("folds=%v err=%v", folds, err)
	}
	if len(folds[0].Train) == 0 || len(folds[0].Calibration) == 0 || len(folds[0].Test) == 0 {
		t.Fatalf("fold=%#v", folds[0])
	}
}

func TestLayeredReportDisclosesCoverageAndNoProbabilityMetrics(t *testing.T) {
	score, ret := .8, .1
	result := OutcomeLabel{Status: "mature", RawReturn: &ret}
	report := Report([]PredictionResult{{ID: "ok", Status: "predicted", Score: &score, Outcome: &result}, {ID: "rejected", Status: "rejected", Outcome: &result}, {ID: "technical", Status: "technical_failure"}})
	if report.Total != 3 || report.Mature != 2 || report.Predicted != 1 || report.Rejected != 1 || report.TechnicalFailures != 1 || report.Coverage != .5 || report.ProbabilityStatus != "unavailable" {
		t.Fatalf("report=%#v", report)
	}
}

func TestReportsRemainSeparatedByAssetClassAndMarket(t *testing.T) {
	value, raw := .8, .1
	result := ReportBySegment([]PredictionResult{
		{ID: "equity", AssetClass: "equity", Market: "US", Status: "predicted", Score: &value, Outcome: &OutcomeLabel{Status: "mature", RawReturn: &raw}},
		{ID: "crypto", AssetClass: "crypto", Market: "CRYPTO", Status: "rejected", Outcome: &OutcomeLabel{Status: "mature", RawReturn: &raw}},
	})
	if len(result) != 2 || result["equity:US"].Predicted != 1 || result["crypto:CRYPTO"].Rejected != 1 {
		t.Fatalf("segmented reports=%#v", result)
	}
}

func fmtID(value int) string { return fmt.Sprintf("event-%03d", value) }
