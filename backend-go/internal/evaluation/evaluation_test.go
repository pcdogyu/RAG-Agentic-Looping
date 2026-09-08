package evaluation

import (
	"fmt"
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

func fmtID(value int) string { return fmt.Sprintf("event-%03d", value) }
