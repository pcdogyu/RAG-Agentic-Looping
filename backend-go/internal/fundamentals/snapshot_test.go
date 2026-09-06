package fundamentals

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNormalizeFMPStatementUsesAcceptedDateForAvailability(t *testing.T) {
	retrieved := time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	item, err := NormalizeFMPStatement("equity:NASDAQ:ACME", IncomeStatement, map[string]any{
		"symbol": "ACME", "date": "2026-06-30", "period": "Q2", "filingDate": "2026-08-01",
		"acceptedDate": "2026-08-01 16:05:03", "reportedCurrency": "USD", "revenue": 1234567,
		"finalLink": "https://www.sec.gov/Archives/example",
	}, "https://financialmodelingprep.com/stable/income-statement?symbol=ACME", retrieved)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 1, 16, 5, 3, 0, time.UTC)
	if !item.AvailableAt.Equal(want) || !item.PublishedAt.Equal(want) || item.Metrics["revenue"] != float64(1234567) {
		t.Fatalf("normalized snapshot=%#v", item)
	}
	if item.AccountingStd != "unknown" || item.Unit != "reported" || item.ID == "" {
		t.Fatalf("normalization metadata=%#v", item)
	}
}

func TestFMPClientFetchesAllStatementsWithProvenance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "secret" || r.URL.Query().Get("symbol") != "ACME" || r.URL.Query().Get("limit") != "2" {
			t.Fatalf("unexpected FMP request: %s %s", r.Header.Get("apikey"), r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"symbol": "ACME", "date": "2026-06-30", "period": "Q2", "acceptedDate": "2026-08-01 16:05:03", "reportedCurrency": "USD", "revenue": 12}})
	}))
	defer server.Close()
	client := FMPClient{BaseURL: server.URL, AccessToken: "secret", HTTPClient: server.Client(), Now: func() time.Time { return time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC) }}
	items, err := client.FetchStatements(context.Background(), "equity:NASDAQ:ACME", "acme", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].SourceName != "FMP" || items[0].SourceURL == "" || items[2].StatementType != CashFlow {
		t.Fatalf("FMP statement snapshots=%#v", items)
	}
}

func TestNormalizeFMPStatementRejectsMissingDisclosureTime(t *testing.T) {
	_, err := NormalizeFMPStatement("equity:NASDAQ:ACME", BalanceSheet, map[string]any{"date": "2026-06-30"}, "https://fmp.test", time.Now().UTC())
	if err == nil {
		t.Fatal("snapshot without filing/accepted time was accepted")
	}
}

func TestLatestAvailableExcludesFutureRevision(t *testing.T) {
	period := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	firstPublished := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	laterPublished := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	items := []Snapshot{
		{ID: "original", AssetID: "equity:NASDAQ:ACME", StatementType: IncomeStatement, ReportPeriodEnd: period, PublishedAt: firstPublished, AvailableAt: firstPublished},
		{ID: "restated", AssetID: "equity:NASDAQ:ACME", StatementType: IncomeStatement, ReportPeriodEnd: period, PublishedAt: laterPublished, AvailableAt: laterPublished},
	}
	beforeRestatement := LatestAvailable(items, time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC))
	if len(beforeRestatement) != 1 || beforeRestatement[0].ID != "original" {
		t.Fatalf("future revision leaked into historical cutoff: %#v", beforeRestatement)
	}
	afterRestatement := LatestAvailable(items, time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	if len(afterRestatement) != 1 || afterRestatement[0].ID != "restated" {
		t.Fatalf("latest revision was not selected: %#v", afterRestatement)
	}
}
