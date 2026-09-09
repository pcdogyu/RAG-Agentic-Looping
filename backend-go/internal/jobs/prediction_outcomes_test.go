package jobs

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
)

func TestIncompleteOutcomeLifecycleDisposition(t *testing.T) {
	signalAt := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	now := signalAt.AddDate(0, 0, 30)
	tests := []struct {
		name       string
		evidence   outcomeLifecycleEvidence
		horizon    int
		wantStatus string
		wantReason string
	}{
		{name: "ordinary missing sessions remain pending", wantStatus: "pending", wantReason: "awaiting_price_sessions", horizon: 5},
		{name: "explicit delisting is terminal", evidence: outcomeLifecycleEvidence{Actions: []marketdata.CorporateActionObservation{{ActionType: marketdata.Delisting, EffectiveAt: signalAt.AddDate(0, 0, 3)}}}, wantStatus: "unavailable", wantReason: "delisting_before_horizon_exit", horizon: 20},
		{name: "backfilled pre-signal delisting is retained", evidence: outcomeLifecycleEvidence{Actions: []marketdata.CorporateActionObservation{{ActionType: marketdata.Delisting, EffectiveAt: signalAt.AddDate(0, 0, -3)}}}, wantStatus: "unavailable", wantReason: "asset_delisted_at_signal", horizon: 20},
		{name: "active suspension waits for trading sessions", evidence: outcomeLifecycleEvidence{Actions: []marketdata.CorporateActionObservation{{ActionType: marketdata.Suspension, EffectiveAt: signalAt.AddDate(0, 0, 3)}}}, wantStatus: "pending", wantReason: "active_suspension", horizon: 20},
		{name: "suspension already active at signal still waits", evidence: outcomeLifecycleEvidence{Actions: []marketdata.CorporateActionObservation{{ActionType: marketdata.Suspension, EffectiveAt: signalAt.AddDate(0, 0, -3)}}}, wantStatus: "pending", wantReason: "active_suspension", horizon: 20},
		{name: "recent symbol change has a continuity grace", evidence: outcomeLifecycleEvidence{Actions: []marketdata.CorporateActionObservation{{ActionType: marketdata.SymbolChange, EffectiveAt: now.AddDate(0, 0, -5)}}}, wantStatus: "pending", wantReason: "symbol_change_continuity_grace", horizon: 1},
		{name: "old symbol change without enough prices becomes auditable", evidence: outcomeLifecycleEvidence{Actions: []marketdata.CorporateActionObservation{{ActionType: marketdata.SymbolChange, EffectiveAt: signalAt.AddDate(0, 0, 3)}}}, wantStatus: "unavailable", wantReason: "symbol_change_price_continuity_unavailable", horizon: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := classifyIncompleteOutcome(signalAt, test.horizon, now, test.evidence)
			if actual.Status != test.wantStatus || actual.Reason != test.wantReason {
				t.Fatalf("disposition=%#v", actual)
			}
		})
	}
}

func TestOutcomeLifecycleQualityDoesNotApplyCurrentMembershipToPastSignal(t *testing.T) {
	signalAt := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	now := signalAt.AddDate(0, 0, 30)
	evidence := outcomeLifecycleEvidence{
		AvailableAt: now,
		Actions: []marketdata.CorporateActionObservation{
			{ActionType: marketdata.CashDividend, EffectiveAt: signalAt.AddDate(0, 0, 2)},
			{ActionType: marketdata.Delisting, EffectiveAt: signalAt.AddDate(0, 0, 20)},
		},
		Memberships: []marketdata.SecurityUniverseMembership{
			{SnapshotID: "current", SnapshotStatus: "completed", MembershipStatus: "delisted", EffectiveAt: signalAt.AddDate(0, 0, 20), AvailableAt: signalAt.AddDate(0, 0, 20)},
			{SnapshotID: "historical", SnapshotStatus: "completed", MembershipStatus: "included", EffectiveAt: signalAt.AddDate(0, 0, -10), AvailableAt: signalAt.AddDate(0, 0, -10)},
		},
	}
	quality := outcomeLifecycleDataQuality(evidence, signalAt, now)
	if quality["security_universe_at_signal"] != "included" || quality["security_universe_at_evaluation"] != "delisted" {
		t.Fatalf("current membership leaked into historical signal: %#v", quality)
	}
	types, ok := quality["corporate_action_types"].([]string)
	if !ok || len(types) != 2 || types[0] != "cash_dividend" || types[1] != "delisting" {
		t.Fatalf("corporate action audit is not deterministic: %#v", quality)
	}
}
