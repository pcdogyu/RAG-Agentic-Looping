package consensus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFMPClientBuildsFirstObservedAnnualSnapshotsWithoutHistoricalBackfill(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "secret" || r.URL.Query().Get("symbol") != "NVDA" || r.URL.Query().Get("period") != "annual" || r.URL.Query().Get("limit") != "2" {
			t.Fatalf("unexpected request: header=%q query=%s", r.Header.Get("apikey"), r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"symbol": "NVDA", "date": "2027-01-31",
			"revenueLow": 100.0, "revenueAvg": 120.0, "revenueHigh": 140.0, "numAnalystsRevenue": 12.0,
			"epsLow": 4.0, "epsAvg": 5.0, "epsHigh": 6.0, "numAnalystsEps": 8.0,
		}})
	}))
	defer server.Close()
	observedAt := time.Date(2026, 9, 10, 3, 4, 5, 0, time.UTC)
	items, err := (FMPClient{BaseURL: server.URL, AccessToken: "secret", HTTPClient: server.Client(), Now: func() time.Time { return observedAt }}).
		FetchAnnual(context.Background(), "equity:XNAS:NVDA", "nvda", "usd", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 6 {
		t.Fatalf("got %d snapshots, want six low/mean/high revenue and EPS snapshots", len(items))
	}
	mean := items[1]
	if mean.Estimate.Metric != "revenue" || mean.Estimate.Statistic != "mean" || mean.Estimate.Value != 120 || mean.Estimate.AnalystCount == nil || *mean.Estimate.AnalystCount != 12 {
		t.Fatalf("unexpected normalized estimate: %#v", mean.Estimate)
	}
	if !mean.Estimate.AvailableAt.Equal(observedAt) || !mean.Estimate.PublishedAt.Equal(observedAt) || mean.Estimate.RevisionAt != nil || mean.Estimate.AccountingBasis != "unknown" {
		t.Fatalf("first-observed time contract changed: %#v", mean.Estimate)
	}
	contract := mean.SourcePayload["observation_contract"].(map[string]any)
	if contract["historical_backfill"] != false || contract["provider_publication_time_available"] != false || contract["published_at_basis"] != "first_observed_at" {
		t.Fatalf("observation contract=%#v", contract)
	}
	if strings.Contains(mean.Estimate.SourceURL, "secret") || mean.Estimate.SourceDocumentID != "analyst-estimates:NVDA:annual:2027-01-31" {
		t.Fatalf("unsafe or unstable provenance: %#v", mean.Estimate)
	}
}

func TestNormalizeFMPAnnualEstimatesRejectsSymbolMismatchAndMissingPeriod(t *testing.T) {
	now := time.Now().UTC()
	if _, err := normalizeFMPAnnualEstimates("asset", "NVDA", "USD", map[string]any{"symbol": "AMD", "date": "2027-01-31"}, "https://fmp.test", now); err == nil {
		t.Fatal("symbol mismatch was accepted")
	}
	if _, err := normalizeFMPAnnualEstimates("asset", "NVDA", "USD", map[string]any{"symbol": "NVDA"}, "https://fmp.test", now); err == nil {
		t.Fatal("missing fiscal period was accepted")
	}
}
