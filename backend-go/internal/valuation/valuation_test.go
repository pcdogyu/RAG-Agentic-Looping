package valuation

import (
	"math"
	"testing"
)

func facts() ForecastFacts {
	return ForecastFacts{Currency: "USD", Unit: "millions", FCFF: 100, NetIncome: 80, DilutedShares: 10}
}

func TestCalculateDCFAndPEAreReproducible(t *testing.T) {
	result := Calculate(facts(), []DCFScenario{{Name: "base", WACC: .1, TerminalGrowth: .03, ProjectionYears: 1, NetDebt: 50, CostOfCapitalEvidenceIDs: []string{"wacc-source"}}}, []MultipleScenario{{Name: "base-pe", PriceEarningsMultiple: 20, ComparableEvidenceIDs: []string{"peer-source"}}})
	if result.Status != "available" || len(result.Scenarios) != 2 {
		t.Fatalf("result=%#v", result)
	}
	// PV = 100/1.1 + (100*1.03/(.1-.03))/1.1; equity then subtracts 50.
	want := (100/1.1 + (100*1.03/(.1-.03))/1.1 - 50) / 10
	if got := *result.Scenarios[0].ValuePerShare; math.Abs(got-want) > 1e-9 {
		t.Fatalf("dcf per share=%v want=%v", got, want)
	}
	if got := *result.Scenarios[1].ValuePerShare; got != 160 {
		t.Fatalf("pe per share=%v", got)
	}
	if result.RangeDescription == "" || result.Range["low"] == nil || result.Range["high"] == nil {
		t.Fatalf("scenario range missing: %#v", result)
	}
}

func TestCalculateRejectsMixedOrUnsupportedDCFInputs(t *testing.T) {
	result := Calculate(facts(), []DCFScenario{{Name: "bad", WACC: .03, TerminalGrowth: .03, ProjectionYears: 5, CostOfCapitalEvidenceIDs: []string{"source"}}}, nil)
	if result.Status != "unavailable" || result.Scenarios[0].Reason != "invalid_fcff_wacc_terminal_growth_assumptions" {
		t.Fatalf("invalid gordon assumptions accepted: %#v", result)
	}
	result = Calculate(ForecastFacts{Currency: "USD", Unit: "millions", FCFF: 100, NetIncome: 80}, nil, []MultipleScenario{{Name: "pe", PriceEarningsMultiple: 10, ComparableEvidenceIDs: []string{"source"}}})
	if result.Status != "unavailable" || result.Reason != "missing_or_invalid_forecast_facts" {
		t.Fatalf("missing shares accepted: %#v", result)
	}
}

func TestSensitivityKeepsInvalidCellsExplicit(t *testing.T) {
	items := Sensitivity(facts(), DCFScenario{Name: "base", ProjectionYears: 5, CostOfCapitalEvidenceIDs: []string{"source"}}, []float64{.08}, []float64{.03, .08})
	if len(items) != 2 || items[0].Status != "available" || items[1].Status != "unavailable" {
		t.Fatalf("sensitivity=%#v", items)
	}
}
