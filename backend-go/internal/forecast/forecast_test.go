package forecast

import (
	"testing"
	"time"
)

func pointer(value float64) *float64 { return &value }

func baseline() Inputs {
	return Inputs{Currency: "USD", Unit: "millions", Revenue: pointer(100), OperatingMargin: pointer(.2), TaxRate: pointer(.25), Depreciation: pointer(5), Capex: pointer(8), ChangeNWC: pointer(2), DilutedShares: pointer(10)}
}

func TestCalculateRequiresExplicitApprovedEvidenceLinkedAssumptions(t *testing.T) {
	result := Calculate(baseline(), []Assumption{{Field: "revenue_delta", Value: 50, Approved: true}})
	if result.Status != "unavailable" || result.Reason != "assumption_missing_evidence" {
		t.Fatalf("unsupported result: %#v", result)
	}
	result = Calculate(baseline(), []Assumption{{Field: "revenue_delta", Value: 50, EvidenceIDs: []string{"event-1"}, Approved: false}})
	if result.Status != "unavailable" || result.Reason != "unapproved_assumption" {
		t.Fatalf("unapproved assumption entered calculation: %#v", result)
	}
}

func TestCalculateDoesNotTurnOrderIntoRevenueWithoutAssumption(t *testing.T) {
	result := Calculate(baseline(), nil)
	if result.Status != "available" || result.Projected["revenue"] != float64(100) {
		t.Fatalf("baseline calculation=%#v", result)
	}
	result = Calculate(baseline(), []Assumption{{Field: "revenue_delta", Value: 20, EvidenceIDs: []string{"ev-order"}, Approved: true}})
	if result.Status != "available" || result.Projected["revenue"] != float64(120) || result.Projected["diluted_eps"] != float64(1.8) {
		t.Fatalf("explicit projection=%#v", result)
	}
}

func TestCalculateNeverTreatsMissingAsZero(t *testing.T) {
	input := baseline()
	input.Capex = nil
	result := Calculate(input, nil)
	if result.Status != "unavailable" || result.Reason != "missing_required_financial_inputs" || len(result.MissingFields) != 1 || result.MissingFields[0] != "capex" {
		t.Fatalf("missing input accepted: %#v", result)
	}
}

func TestBuildVersionRequiresVisibleFactReferencesAndStableIdentity(t *testing.T) {
	asOf := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	_, err := buildVersion(Submission{AssetID: "equity:NYSE:VRT", AsOf: asOf, Inputs: baseline()})
	if err == nil {
		t.Fatal("missing source references were accepted")
	}
	one, err := buildVersion(Submission{AssetID: "equity:NYSE:VRT", AsOf: asOf, Inputs: baseline(), FundamentalSnapshotIDs: []string{"b", "a", "a"}})
	if err != nil {
		t.Fatal(err)
	}
	two, err := buildVersion(Submission{AssetID: "equity:NYSE:VRT", AsOf: asOf, Inputs: baseline(), FundamentalSnapshotIDs: []string{"a", "b"}})
	if err != nil || one.ID != two.ID {
		t.Fatalf("source ordering changed deterministic identity: one=%s two=%s err=%v", one.ID, two.ID, err)
	}
}

func TestEventAssumptionRequiresPeriodAndProducesSingleLink(t *testing.T) {
	_, err := buildVersion(Submission{AssetID: "equity:NYSE:VRT", AsOf: time.Now(), Inputs: baseline(), FundamentalSnapshotIDs: []string{"snapshot"}, Assumptions: []Assumption{{Field: "revenue_delta", Value: 1, EventID: "event", EvidenceIDs: []string{"evidence"}, Approved: true}}})
	if err == nil {
		t.Fatal("event without fiscal period was accepted")
	}
	period := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	version, err := buildVersion(Submission{AssetID: "equity:NYSE:VRT", AsOf: time.Now(), Inputs: baseline(), FundamentalSnapshotIDs: []string{"snapshot"}, Assumptions: []Assumption{{Field: "revenue_delta", Value: 1, EventID: "event", EvidenceIDs: []string{"evidence"}, FiscalPeriodEnd: &period, Approved: true}}})
	if err != nil || len(versionLinks(version)) != 1 {
		t.Fatalf("event assumption link=%#v err=%v", versionLinks(version), err)
	}
}
