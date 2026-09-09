package marketdata

import (
	"testing"
	"time"
)

func TestPriceObservationNormalizesAndKeepsFieldSemantics(t *testing.T) {
	observed := time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)
	value := PriceObservation{
		AssetID: " equity:NASDAQ:CRWD ", Market: "us", Currency: "usd", ObservedAt: observed,
		AvailableAt: observed.Add(time.Hour), Price: 417.25, PriceField: "adjusted_close",
		TimePrecision: "daily_close", SourceName: "FMP", SourceDocumentID: "CRWD:2026-09-08",
	}
	if err := value.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if value.AssetID != "equity:NASDAQ:CRWD" || value.Market != "US" || value.Currency != "USD" {
		t.Fatalf("identity was not normalized: %#v", value)
	}
	if value.PriceField != "adjusted_close" || value.Metadata["contract_version"] != PriceContractVersion {
		t.Fatalf("price semantics were lost: %#v", value)
	}
	copy := value
	copy.ID = ""
	if err := copy.NormalizeAndValidate(); err != nil || copy.ID != value.ID {
		t.Fatalf("deterministic identity changed: first=%s second=%s err=%v", value.ID, copy.ID, err)
	}
}

func TestPriceObservationRejectsFutureAvailabilityAndInvalidPrice(t *testing.T) {
	now := time.Now().UTC()
	base := PriceObservation{AssetID: "equity:XNYS:ONE", Market: "US", Currency: "USD", ObservedAt: now, AvailableAt: now.Add(-time.Second), Price: 1, PriceField: "close", TimePrecision: "timestamped", SourceName: "test", SourceDocumentID: "one"}
	if err := base.NormalizeAndValidate(); err == nil {
		t.Fatal("observation available before its market timestamp was accepted")
	}
	base.AvailableAt, base.Price = now, 0
	if err := base.NormalizeAndValidate(); err == nil {
		t.Fatal("zero price was accepted")
	}
}

func TestCloseAndAdjustedCloseHaveDifferentIdentities(t *testing.T) {
	now := time.Now().UTC()
	base := PriceObservation{AssetID: "equity:XNYS:ONE", Market: "US", Currency: "USD", ObservedAt: now, AvailableAt: now, Price: 10, PriceField: "close", TimePrecision: "timestamped", SourceName: "test", SourceDocumentID: "one"}
	if err := base.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	adjusted := base
	adjusted.ID, adjusted.PriceField = "", "adjusted_close"
	if err := adjusted.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if base.ID == adjusted.ID {
		t.Fatal("close and adjusted close were treated as the same fact")
	}
}
