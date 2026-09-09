package analystevidence

import (
	"strings"
	"testing"
	"time"
)

func validSubmission(kind string, values map[string]any) Submission {
	observed := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	return Submission{
		AssetID: "equity:XNAS:ACME", EvidenceType: kind, Title: "Reviewed input",
		Rationale: "Analyst reviewed the cited source and selected this governed input.", Values: values,
		ObservedAt: observed, AvailableAt: observed.Add(time.Hour), SourceName: "Issuer filing",
		SourceDocumentID: "document-1", SourceURL: "https://user:secret@example.test/report?token=secret#page=1",
		ApprovedBy: "reviewer", IdempotencyKey: "review-1",
	}
}

func TestValidateSubmissionRequiresTypedValuesAndPointInTimeApproval(t *testing.T) {
	approved := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	cases := []Submission{
		validSubmission(ValuationMultiple, map[string]any{"selected_multiple": 20.0}),
		validSubmission(CostOfCapital, map[string]any{"wacc": .1}),
		validSubmission(BenchmarkExpectation, map[string]any{"benchmark_id": "equity:AMEX:SPY", "expected_return": .05}),
		validSubmission(ForecastAssumption, map[string]any{"field": "revenue_growth", "value": .1}),
		validSubmission(RatingRationale, map[string]any{"reason_codes": []any{"analyst_review"}}),
		validSubmission(InvalidationRule, map[string]any{"rule_type": "revenue_growth"}),
	}
	for _, input := range cases {
		input = normalizeSubmission(input)
		if err := validateSubmission(input, approved); err != nil {
			t.Fatalf("kind=%s err=%v", input.EvidenceType, err)
		}
		if input.SourceURL != "https://example.test/report" || strings.Contains(input.SourceURL, "secret") {
			t.Fatalf("source URL was not sanitized: %s", input.SourceURL)
		}
	}
	invalid := validSubmission(BenchmarkExpectation, map[string]any{"benchmark_id": "equity:AMEX:SPY"})
	if err := validateSubmission(normalizeSubmission(invalid), approved); err == nil {
		t.Fatal("benchmark evidence without expected_return was accepted")
	}
	missingIdentity := validSubmission(BenchmarkExpectation, map[string]any{"expected_return": .05})
	if err := validateSubmission(normalizeSubmission(missingIdentity), approved); err == nil {
		t.Fatal("benchmark evidence without benchmark_id was accepted")
	}
	future := validSubmission(ValuationMultiple, map[string]any{"selected_multiple": 20.0})
	future.AvailableAt = approved.Add(time.Hour)
	if err := validateSubmission(normalizeSubmission(future), approved); err == nil {
		t.Fatal("future analyst evidence was accepted")
	}
}
