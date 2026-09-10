package marketdata

import (
	"testing"
	"time"
)

func TestResolveTradabilityRequiresAllLatestSourcesToAgree(t *testing.T) {
	session := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	tradable := TradabilityObservation{SessionDate: session, Status: Tradable, BuyExecutable: true, SellExecutable: true}
	if actual := ResolveTradability(nil, session); actual.Status != "unknown" || actual.Reason != "no_observation" {
		t.Fatalf("missing evidence resolved=%#v", actual)
	}
	if actual := ResolveTradability([]TradabilityObservation{tradable, tradable}, session); actual.Status != "tradable" || actual.SourceCount != 2 {
		t.Fatalf("agreeing evidence resolved=%#v", actual)
	}
	suspended := TradabilityObservation{SessionDate: session, Status: TradabilitySuspended}
	if actual := ResolveTradability([]TradabilityObservation{tradable, suspended}, session); actual.Status != "conflict" || actual.Reason != "source_disagreement" || actual.SourceCount != 2 {
		t.Fatalf("conflicting evidence resolved=%#v", actual)
	}
	invalid := TradabilityObservation{SessionDate: session, Status: Tradable}
	if actual := ResolveTradability([]TradabilityObservation{invalid}, session); actual.Status != "conflict" || actual.Reason != "invalid_execution_flags" {
		t.Fatalf("invalid executable flags resolved=%#v", actual)
	}
}

func TestParseTradabilityObservedAtKeepsPrecision(t *testing.T) {
	dateOnly, precision, err := parseTradabilityObservedAt("2026-09-09")
	if err != nil || precision != "date_only" || dateOnly.Hour() != 0 {
		t.Fatalf("date-only parse=%s precision=%s err=%v", dateOnly, precision, err)
	}
	timestamped, precision, err := parseTradabilityObservedAt("2026-09-09T20:00:00+08:00")
	if err != nil || precision != "timestamped" || timestamped.Location() != time.UTC || timestamped.Hour() != 12 {
		t.Fatalf("timestamp parse=%s precision=%s err=%v", timestamped, precision, err)
	}
}
