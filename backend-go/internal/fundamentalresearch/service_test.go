package fundamentalresearch

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/valuation"
)

func TestScheduleDraftFromWorkflowRequiresApprovalAndDropsManualPrice(t *testing.T) {
	price := 123.45
	benchmark := .04
	input := Input{
		AssetID: "equity:XNAS:ACME",
		Valuation: ValuationPlan{MultipleScenarios: []valuation.MultipleScenario{{
			Name: "base", PriceEarningsMultiple: 18, ComparableEvidenceIDs: []string{"peer-review-1"},
		}}},
		Rating: RatingPlan{
			Policy: rating.DefaultUSPolicy(), AsOfPrice: &price, AsOfPriceEvidenceID: "manual-price-1",
			BenchmarkReturn: &benchmark, BenchmarkEvidenceID: "benchmark-review-1",
			ReasonCodes: []string{"analyst_review"}, EvidenceIDs: []string{"financial-1"},
		},
	}

	draft := scheduleDraftFromWorkflow(input, "forecast-1")
	if draft.AssetID != input.AssetID || draft.ForecastVersionID != "forecast-1" || draft.ApprovedBy != "" {
		t.Fatalf("draft controls=%#v", draft)
	}
	if draft.Rating.BenchmarkEvidenceID != "benchmark-review-1" || len(draft.Valuation.MultipleScenarios) != 1 {
		t.Fatalf("draft governed inputs=%#v", draft)
	}
	body, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "manual-price-1") || strings.Contains(string(body), "as_of_price") {
		t.Fatalf("draft leaked one-time price: %s", body)
	}
}
