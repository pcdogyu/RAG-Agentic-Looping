package consensus

import (
	"testing"
	"time"
)

func TestCompareActualUsesOnlyPreAnnouncementConsensus(t *testing.T) {
	period := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	announcement := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	actual := Actual{AssetID: "equity:NYSE:ONE", Metric: "revenue", FiscalPeriodEnd: period, AccountingBasis: "gaap", Value: 120, Currency: "USD", Unit: "millions", AvailableAt: announcement}
	estimates := []Estimate{
		{ID: "before", AssetID: actual.AssetID, Metric: "revenue", FiscalPeriodEnd: period, AccountingBasis: "gaap", Value: 130, Currency: "USD", Unit: "millions", AvailableAt: announcement.Add(-time.Hour)},
		{ID: "after", AssetID: actual.AssetID, Metric: "revenue", FiscalPeriodEnd: period, AccountingBasis: "gaap", Value: 119, Currency: "USD", Unit: "millions", AvailableAt: announcement.Add(time.Hour)},
	}
	result := CompareActualToPreAnnouncementConsensus(actual, estimates, announcement)
	if result.Status != "available" || result.EstimateID != "before" || result.SurpriseDirection != "below_consensus" || *result.Surprise != -10 {
		t.Fatalf("unexpected comparison: %#v", result)
	}
	// The actual can still be 20% higher year over year (e.g. previous-year
	// actual 100) while missing consensus. Surprise and YoY are deliberately
	// separate concepts and this comparison only reports the former.
}

func TestCompareActualRejectsMismatchedBasisAndFutureEstimate(t *testing.T) {
	period := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	announcement := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	actual := Actual{AssetID: "equity:NYSE:ONE", Metric: "eps", FiscalPeriodEnd: period, AccountingBasis: "gaap", Value: 1.2, Currency: "USD", Unit: "reported", AvailableAt: announcement}
	mismatch := Estimate{ID: "nongAAP", AssetID: actual.AssetID, Metric: "eps", FiscalPeriodEnd: period, AccountingBasis: "non_gaap", Value: 1.3, Currency: "USD", Unit: "reported", AvailableAt: announcement.Add(-time.Minute)}
	result := CompareActualToPreAnnouncementConsensus(actual, []Estimate{mismatch}, announcement)
	if result.Status != "unavailable" || result.Reason != "metric_period_currency_unit_or_basis_mismatch" {
		t.Fatalf("mismatched estimate was accepted: %#v", result)
	}
	futureOnly := mismatch
	futureOnly.AccountingBasis, futureOnly.AvailableAt = "gaap", announcement.Add(time.Minute)
	result = CompareActualToPreAnnouncementConsensus(actual, []Estimate{futureOnly}, announcement)
	if result.Status != "unavailable" || result.Reason != "no_pre_announcement_consensus" {
		t.Fatalf("future estimate leaked into comparison: %#v", result)
	}
}

func TestRevisionRequiresCompatibleEstimateSeries(t *testing.T) {
	period := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	previous := Estimate{AssetID: "equity:NYSE:ONE", Metric: "revenue", FiscalPeriodEnd: period, AccountingBasis: "gaap", Value: 100, Currency: "USD", Unit: "millions"}
	current := previous
	current.Value = 105
	delta, err := Revision(previous, current)
	if err != nil || delta != 5 {
		t.Fatalf("revision=%v err=%v", delta, err)
	}
	current.Currency = "EUR"
	if _, err := Revision(previous, current); err == nil {
		t.Fatal("currency-mismatched revision was accepted")
	}
}
