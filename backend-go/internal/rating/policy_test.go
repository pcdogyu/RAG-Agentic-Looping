package rating

import "testing"

func float(value float64) *float64 { return &value }

func TestFundamentalRatingUsesCurrentPriceNotEventDirection(t *testing.T) {
	policy := DefaultUSPolicy()
	// Even with a higher target, a price that already rose beyond it is sell.
	result := Evaluate(Input{Policy: policy, AsOfPrice: float(130), TargetPrice: float(100), BenchmarkReturn: float(0), ValuationRunID: "valuation-1"})
	if result.Status != "available" || result.Rating != "strong_sell" || result.ExpectedExcessReturn == nil {
		t.Fatalf("result=%#v", result)
	}
}

func TestFundamentalRatingNeverDefaultsMissingBenchmarkToZero(t *testing.T) {
	result := Evaluate(Input{Policy: DefaultUSPolicy(), AsOfPrice: float(100), TargetPrice: float(120)})
	if result.Status != "unavailable" || result.Reason != "missing_benchmark_return" {
		t.Fatalf("result=%#v", result)
	}
}

func TestFundamentalRatingCanUseAbsolutePolicy(t *testing.T) {
	policy := DefaultUSPolicy()
	policy.RelativeRequired = false
	policy.BenchmarkID = ""
	result := Evaluate(Input{Policy: policy, AsOfPrice: float(100), TargetPrice: float(110), ExpectedDividend: float(1)})
	if result.Status != "available" || result.Rating != "buy" || *result.ExpectedReturn != .11 {
		t.Fatalf("result=%#v", result)
	}
}

func TestFundamentalRevisionIsSeparateAndAuditable(t *testing.T) {
	previous := &State{Rating: "buy", ValuationRunID: "valuation-old"}
	revision := Revise(previous, Result{Status: "available", Rating: "sell", ValuationRunID: "valuation-new"})
	if revision.Action != "downgraded" || revision.PreviousRating != "buy" || revision.CurrentRating != "sell" {
		t.Fatalf("revision=%#v", revision)
	}
	if underReview := Revise(previous, Result{Status: "unavailable", Reason: "missing_benchmark_return"}); underReview.Action != "under_review" {
		t.Fatalf("unavailable fundamental data should not invent a rating: %#v", underReview)
	}
}
