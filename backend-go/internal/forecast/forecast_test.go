package forecast

import "testing"

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
