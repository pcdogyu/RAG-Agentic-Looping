package consensus

import (
	"testing"
	"time"
)

func TestAnnouncementAssessmentSeparatesGrowthFromConsensusSurprise(t *testing.T) {
	announcement := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	period := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	priorPeriod := time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC)
	actual := func(value float64) Actual {
		return Actual{AssetID: "equity:NYSE:ONE", Metric: "revenue", FiscalPeriod: "Q2", FiscalPeriodEnd: period, AccountingBasis: "us_gaap", Value: value, Currency: "USD", Unit: "reported", AvailableAt: announcement}
	}
	prior := actual(100)
	prior.FiscalPeriodEnd = priorPeriod
	estimate := Estimate{ID: "estimate", AssetID: "equity:NYSE:ONE", Metric: "revenue", FiscalPeriod: "Q2", FiscalPeriodEnd: period, AccountingBasis: "us_gaap", Statistic: "mean", Value: 120, Currency: "USD", Unit: "reported", AvailableAt: announcement.Add(-time.Hour)}
	high := estimate
	high.ID, high.Statistic, high.Value, high.AvailableAt = "later-high", "high", 105, announcement.Add(-30*time.Minute)
	growthBelow := AssessAnnouncement(actual(110), prior, []Estimate{estimate, high}, announcement)
	if growthBelow.Status != "available" || growthBelow.SemanticState != "growth_below_consensus" || growthBelow.YearOverYear.Direction != "growth" || growthBelow.Consensus.SurpriseDirection != "below_consensus" {
		t.Fatalf("growth-below assessment=%#v", growthBelow)
	}
	estimate.Value = 80
	declineAbove := AssessAnnouncement(actual(90), prior, []Estimate{estimate, high}, announcement)
	if declineAbove.SemanticState != "decline_above_consensus" || declineAbove.YearOverYear.Direction != "decline" || declineAbove.Consensus.SurpriseDirection != "above_consensus" {
		t.Fatalf("decline-above assessment=%#v", declineAbove)
	}
}

func TestYearOverYearWithholdsMisleadingPercentageForNonPositivePrior(t *testing.T) {
	current := Actual{AssetID: "asset", Metric: "eps", FiscalPeriod: "FY", FiscalPeriodEnd: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), AccountingBasis: "us_gaap", Value: 2, Currency: "USD", Unit: "reported"}
	prior := current
	prior.FiscalPeriodEnd, prior.Value = time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), -1
	result := CompareYearOverYear(current, prior)
	if result.Status != "available" || result.AbsoluteChange == nil || *result.AbsoluteChange != 3 || result.PercentageChange != nil || result.PercentageStatus != "unavailable_non_positive_prior" {
		t.Fatalf("non-positive prior comparison=%#v", result)
	}
}

func TestYearOverYearRejectsMissingOrMismatchedAccountingMetadata(t *testing.T) {
	current := Actual{AssetID: "asset", Metric: "revenue", FiscalPeriod: "FY", FiscalPeriodEnd: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), AccountingBasis: "us_gaap", Value: 2, Currency: "USD", Unit: "reported"}
	prior := current
	prior.FiscalPeriodEnd = time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	missing := current
	missing.AccountingBasis = ""
	if result := CompareYearOverYear(missing, prior); result.Status != "unavailable" || result.Reason != "missing_period_currency_unit_or_accounting_basis" {
		t.Fatalf("missing basis comparison=%#v", result)
	}
	mismatch := current
	mismatch.Currency = "EUR"
	if result := CompareYearOverYear(mismatch, prior); result.Status != "unavailable" || result.Reason != "incompatible_or_missing_year_over_year_actual" {
		t.Fatalf("mismatched currency comparison=%#v", result)
	}
}

func TestEstimateRevisionsReportAggregateDirectionWithoutInventingAnalystBehavior(t *testing.T) {
	period := time.Date(2027, 12, 31, 0, 0, 0, 0, time.UTC)
	firstTime := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	count, nextCount := 5, 7
	first := Estimate{ID: "first", AssetID: "asset", Metric: "revenue", FiscalPeriodEnd: period, AccountingBasis: "unknown", Statistic: "mean", Value: 100, AnalystCount: &count, Currency: "USD", Unit: "reported", AvailableAt: firstTime, SourceName: "FMP", SourceDocumentID: "doc"}
	second := first
	second.ID, second.Value, second.AnalystCount, second.AvailableAt = "second", 110, &nextCount, firstTime.Add(24*time.Hour)
	revisions := BuildEstimateRevisions([]Estimate{second, first})
	if len(revisions) != 1 || revisions[0].Direction != "raised" || revisions[0].AbsoluteChange != 10 || revisions[0].IndividualBehaviorStatus != "unavailable_aggregate_snapshots_only" || revisions[0].Coverage != "provider_aggregate_snapshot" {
		t.Fatalf("estimate revisions=%#v", revisions)
	}
}

func TestGuidanceRevisionKeepsOldNewBoundsAndRangeChange(t *testing.T) {
	low, high, newLow, newHigh := 90.0, 110.0, 105.0, 115.0
	firstTime := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	previous := Guidance{ID: "old", AssetID: "asset", Metric: "revenue", FiscalPeriodEnd: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), AccountingBasis: "us_gaap", LowValue: &low, HighValue: &high, Currency: "USD", Unit: "millions", AvailableAt: firstTime}
	current := previous
	current.ID, current.LowValue, current.HighValue, current.AvailableAt = "new", &newLow, &newHigh, firstTime.Add(24*time.Hour)
	result := CompareGuidanceRevision(previous, current)
	if result.Status != "available" || result.Direction != "raised" || result.RangeChange != "narrowed" || result.MidpointChange == nil || *result.MidpointChange != 10 || len(result.ChangedBounds) != 2 {
		t.Fatalf("guidance revision=%#v", result)
	}
}
