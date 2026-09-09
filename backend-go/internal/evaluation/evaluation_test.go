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

func TestOutcomeStartsAfterSignalAndKeepsAbsoluteRelativeAndNetReturns(t *testing.T) {
	asset := []PricePoint{price(100, 2, true), price(105, 3, true), price(110, 4, true)}
	benchmark := []PricePoint{price(100, 2, true), price(102, 3, true), price(103, 4, true)}
	result := BuildOutcomeLabel(time.Date(2025, 1, 2, 20, 0, 0, 0, time.UTC), asset, benchmark, HorizonPolicy{HorizonSessions: 2, NeutralBand: .01, ExecutionCostBPS: 10, EntryPolicy: "first_session_after_signal", PriceField: "adjusted_close"})
	if result.Status != "mature" || result.RawReturn == nil || result.BenchmarkReturn == nil || result.ExcessReturn == nil || result.NetReturn == nil {
		t.Fatalf("result=%#v", result)
	}
	if *result.RawReturn <= 0 || *result.ExcessReturn <= 0 || *result.NetReturn >= *result.RawReturn {
		t.Fatalf("returns=%#v", result)
	}
}

func TestOutcomeRejectsUnadjustedCorporateActionSeries(t *testing.T) {
	result := BuildOutcomeLabel(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), []PricePoint{price(100, 2, false), price(50, 3, false)}, nil, HorizonPolicy{HorizonSessions: 1, EntryPolicy: "first_session_after_signal", PriceField: "adjusted_close"})
	if result.Status != "unavailable" {
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
	result := BuildOutcomeLabel(entryAt.Add(-time.Hour), asset, nil, HorizonPolicy{HorizonSessions: 1, EntryPolicy: "first_session_after_signal", PriceField: "adjusted_close"})
	if result.Status != "mature" || result.RawReturn == nil || math.Abs(*result.RawReturn-.02) > 1e-9 {
		t.Fatalf("split-adjusted result=%#v", result)
	}
}

func TestOutcomeEntryMustBeStrictlyAfterSignalAvailability(t *testing.T) {
	signal := time.Date(2025, 1, 2, 21, 0, 0, 0, time.UTC)
	asset := []PricePoint{price(100, 2, true), price(105, 3, true), price(110, 4, true)}
	result := BuildOutcomeLabel(signal, asset, nil, HorizonPolicy{HorizonSessions: 1, EntryPolicy: "first_session_after_signal", PriceField: "adjusted_close"})
	if result.Status != "mature" || result.EntryAt == nil || !result.EntryAt.Equal(time.Date(2025, 1, 3, 21, 0, 0, 0, time.UTC)) || result.RawReturn == nil || *result.RawReturn <= 0 {
		t.Fatalf("same-time price was incorrectly used as executable entry: %#v", result)
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
