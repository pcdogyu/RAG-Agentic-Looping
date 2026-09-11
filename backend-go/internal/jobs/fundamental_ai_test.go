package jobs

import (
	"math"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/analystevidence"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentalai"
)

func TestFundamentalAISourceNetworkGuard(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.15.0.29", "169.254.1.2", "::1", "ff02::1"} {
		if safePublicIP(net.ParseIP(raw)) {
			t.Fatalf("private or special address was allowed: %s", raw)
		}
	}
	if !safePublicIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public address was rejected")
	}
}

func TestFundamentalAICanonicalURLRemovesCredentials(t *testing.T) {
	parsed, err := url.Parse("https://user:secret@example.com/report?API_KEY=secret&section=income#page=2")
	if err != nil {
		t.Fatal(err)
	}
	if got := canonicalAIURL(parsed); got != "https://example.com/report?section=income" {
		t.Fatalf("sensitive URL material was retained: %s", got)
	}
}

func TestFundamentalAIPolicyRequiresOriginalNumericEvidenceAndIndependence(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	official := fundamentalai.SourceSnapshot{ID: "official", SourceClass: "regulatory", SourceDomain: "sec.gov", ContentText: "Apple Inc AAPL reported a verifiable TTM selected P/E multiple of 20.", RetrievalStatus: "available", AvailableAt: now}
	draft := fundamentalAISuggestionDraft{EvidenceType: analystevidence.ValuationMultiple, Title: "Historical multiple", Rationale: "Original filing", SourceIDs: []string{"official"}, EvidenceQuote: "AAPL reported a verifiable TTM selected P/E multiple of 20", EvidenceLocation: "document text paragraph 1", NumericValue: floatPointer(20), Currency: "N/A", Unit: "multiple", Period: "TTM"}
	candidate, _ := validateFundamentalAISuggestion("equity:XNAS:AAPL", "AAPL", "Apple Inc", "USD", uuid.New(), draft, []fundamentalai.SourceSnapshot{official}, false)
	if candidate.Status != "proposed" {
		t.Fatalf("official original evidence was rejected: %#v", candidate.Validation)
	}
	draft.NumericValue = floatPointer(21)
	candidate, _ = validateFundamentalAISuggestion("equity:XNAS:AAPL", "AAPL", "Apple Inc", "USD", uuid.New(), draft, []fundamentalai.SourceSnapshot{official}, false)
	if candidate.Status != "insufficient_data" {
		t.Fatal("number absent from original text was accepted")
	}
	draft.NumericValue = floatPointer(20)
	public := official
	public.ID, public.SourceClass, public.SourceDomain = "public-one", "public_web", "example.com"
	draft.SourceIDs = []string{public.ID}
	candidate, _ = validateFundamentalAISuggestion("equity:XNAS:AAPL", "AAPL", "Apple Inc", "USD", uuid.New(), draft, []fundamentalai.SourceSnapshot{public}, false)
	if candidate.Status != "insufficient_data" {
		t.Fatal("one unofficial source was accepted")
	}
	second := public
	second.ID, second.SourceDomain = "public-two", "example.org"
	draft.SourceIDs = []string{public.ID, second.ID}
	candidate, _ = validateFundamentalAISuggestion("equity:XNAS:AAPL", "AAPL", "Apple Inc", "USD", uuid.New(), draft, []fundamentalai.SourceSnapshot{public, second}, true)
	if candidate.Status != "insufficient_data" {
		t.Fatal("conflicting sources were accepted")
	}
}

func TestFundamentalAIWACCIsDeterministic(t *testing.T) {
	inputs := map[string]float64{"risk_free_rate": .04, "beta": 1.2, "equity_risk_premium": .05, "debt_cost": .06, "tax_rate": .21, "equity_weight": .8, "debt_weight": .2}
	got, ok := deterministicAIWACC(inputs)
	want := .8*(.04+1.2*.05) + .2*.06*(1-.21)
	if !ok || math.Abs(got-want) > 1e-12 {
		t.Fatalf("WACC=%v ok=%v want=%v", got, ok, want)
	}
	inputs["debt_weight"] = .4
	if _, ok := deterministicAIWACC(inputs); ok {
		t.Fatal("invalid capital weights were accepted")
	}
}

func TestFundamentalAIFutureSourceIsUnavailable(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	sources := availableAISources([]fundamentalai.SourceSnapshot{{ID: "future", RetrievalStatus: "available", ContentText: "AAPL 20", AvailableAt: now, PublishedAt: &future}})
	if len(sources) != 0 {
		t.Fatal("future publication entered the AI evidence set")
	}
}

func TestFundamentalAIDetectsConflictingValuesWithoutModelAdmission(t *testing.T) {
	left, right := 18.0, 24.0
	conflicts := detectFundamentalAIConflicts([]fundamentalAISuggestionDraft{
		{EvidenceType: analystevidence.ValuationMultiple, NumericValue: &left},
		{EvidenceType: analystevidence.ValuationMultiple, NumericValue: &right},
	})
	if len(conflicts) != 1 {
		t.Fatalf("conflicting source values were not detected: %#v", conflicts)
	}
}

func TestFundamentalAIHoldoutUsesThirtyXNYSSessions(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, location)
	start := nextXNYSAfter(now, location)
	if start.In(location).Format("2006-01-02") != "2026-11-25" {
		t.Fatalf("unexpected first session: %s", start.In(location))
	}
	end := addXNYSSessions(start, 29, location)
	count := 1
	for current := start.In(location); current.Before(end.In(location)); {
		current = current.AddDate(0, 0, 1)
		if isXNYSSession(current) {
			count++
		}
	}
	if count != 30 || isXNYSSession(time.Date(2026, 11, 26, 16, 0, 0, 0, location)) {
		t.Fatalf("holdout boundary count=%d end=%s", count, end.In(location))
	}
}

func floatPointer(value float64) *float64 { return &value }
