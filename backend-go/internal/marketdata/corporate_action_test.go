package marketdata

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNormalizeFMPDividendPreservesTermsAndFirstObservedAvailability(t *testing.T) {
	retrieved := time.Date(2026, 9, 9, 8, 30, 0, 0, time.UTC)
	value, err := NormalizeFMPDividend("equity:XNAS:AAPL", "us", "usd", map[string]any{
		"symbol": "AAPL", "date": "2026-08-10", "declarationDate": "2026-07-30",
		"recordDate": "2026-08-10", "paymentDate": "2026-08-13", "dividend": .26,
		"adjDividend": .26, "frequency": "Quarterly", "yield": .004,
	}, "https://user:secret@financialmodelingprep.com/stable/dividends?symbol=AAPL&apikey=secret#fragment", retrieved)
	if err != nil {
		t.Fatal(err)
	}
	if value.ActionType != CashDividend || value.CashAmount == nil || *value.CashAmount != .26 || value.AdjustedCashAmount == nil || value.ID == "" || value.EventKey == "" {
		t.Fatalf("normalized dividend=%#v", value)
	}
	if !value.AvailableAt.Equal(retrieved) || value.AnnouncementAt == nil || !value.AnnouncementAt.Equal(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("dividend time contract=%#v", value)
	}
	if strings.Contains(value.SourceURL, "secret") || strings.Contains(value.SourceURL, "apikey") || value.Metadata["contract_version"] != CorporateActionContractVersion {
		t.Fatalf("dividend provenance leaked credentials or lost version: %#v", value)
	}
}

func TestNormalizeFMPSplitClassifiesForwardAndReverseRatios(t *testing.T) {
	now := time.Date(2026, 9, 9, 8, 30, 0, 0, time.UTC)
	forward, err := NormalizeFMPSplit("equity:XNAS:FORWARD", "US", "USD", map[string]any{"symbol": "FORWARD", "date": "2026-08-01", "numerator": 4, "denominator": 1, "splitType": "Stock Split"}, "https://fmp.test/splits", now)
	if err != nil || forward.ActionType != StockSplit {
		t.Fatalf("forward=%#v err=%v", forward, err)
	}
	reverse, err := NormalizeFMPSplit("equity:XNAS:REVERSE", "US", "USD", map[string]any{"symbol": "REVERSE", "date": "2026-08-02", "numerator": 1, "denominator": 10, "splitType": "Reverse Split"}, "https://fmp.test/splits", now)
	if err != nil || reverse.ActionType != ReverseSplit {
		t.Fatalf("reverse=%#v err=%v", reverse, err)
	}
}

func TestCorporateActionContractRejectsContradictoryTerms(t *testing.T) {
	now := time.Now().UTC()
	numerator, denominator := 1.0, 10.0
	value := CorporateActionObservation{AssetID: "equity:XNAS:BAD", Market: "US", Currency: "USD", ActionType: StockSplit, EffectiveAt: now, ObservedAt: now, AvailableAt: now, RatioNumerator: &numerator, RatioDenominator: &denominator, TimePrecision: "date_only", SourceName: "test", SourceDocumentID: "bad"}
	if err := value.NormalizeAndValidate(); err == nil {
		t.Fatal("forward split accepted a reverse-split ratio")
	}
}

func TestFMPCorporateActionClientFetchesDividendAndSplitWithoutQuerySecret(t *testing.T) {
	retrieved := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "secret" || r.URL.Query().Get("symbol") != "ACME" || r.URL.Query().Get("limit") != "25" || r.URL.Query().Get("apikey") != "" {
			t.Fatalf("unexpected FMP request header=%q query=%q", r.Header.Get("apikey"), r.URL.RawQuery)
		}
		if strings.HasSuffix(r.URL.Path, "/dividends") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"symbol": "ACME", "date": "2026-08-10", "dividend": .5, "adjDividend": .5}})
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"symbol": "ACME", "date": "2026-08-20", "numerator": 2, "denominator": 1}})
	}))
	defer server.Close()
	client := FMPCorporateActionClient{BaseURL: server.URL, AccessToken: "secret", HTTPClient: server.Client(), Now: func() time.Time { return retrieved }}
	items, err := client.Fetch(context.Background(), "equity:XNAS:ACME", "US", "USD", "acme", 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ActionType != CashDividend || items[1].ActionType != StockSplit || !items[1].AvailableAt.Equal(retrieved) {
		t.Fatalf("corporate actions=%#v", items)
	}
}
