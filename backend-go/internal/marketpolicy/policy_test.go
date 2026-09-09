package marketpolicy

import "testing"

func TestMarketPoliciesDoNotApplyEquityDCFToOtherAssets(t *testing.T) {
	if policy := Resolve("equity", "US"); !policy.Supported || !policy.FundamentalSupported || !policy.PredictionSupported || policy.FundamentalMethod != "fcff_wacc_or_pe" || policy.BenchmarkID != USBenchmarkAssetID {
		t.Fatalf("equity=%#v", policy)
	}
	for _, item := range []struct{ assetClass, market string }{{"crypto", "CRYPTO"}, {"commodity", "GLOBAL"}, {"etf", "US"}} {
		policy := Resolve(item.assetClass, item.market)
		if !policy.Supported || policy.FundamentalSupported || !policy.PredictionSupported || policy.FundamentalMethod == "fcff_wacc_or_pe" || policy.Reason == "" || len(policy.RequiredInputs) == 0 || policy.BenchmarkPolicy == "" || len(policy.ExecutionConstraints) == 0 {
			t.Fatalf("%s=%#v", item.assetClass, policy)
		}
	}
	if policy := Resolve("crypto", "US"); policy.Supported {
		t.Fatalf("unsupported crypto market was accepted: %#v", policy)
	}
	us, cn, hk := Resolve("equity", "US"), Resolve("equity", "CN"), Resolve("equity", "HK")
	if us.Currency == cn.Currency || us.TimeZone == cn.TimeZone || cn.Currency == hk.Currency || cn.TimeZone == hk.TimeZone {
		t.Fatalf("cross-currency or cross-time-zone policies were conflated: us=%#v cn=%#v hk=%#v", us, cn, hk)
	}
}

func TestFundamentalPolicyRejectsCrossMarketCurrencyAndBenchmark(t *testing.T) {
	policy := Resolve("equity", "US")
	if err := ValidateFundamental(policy, "USD", "US", "equity", USBenchmarkAssetID); err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct{ currency, market, class, benchmark string }{
		{"CNY", "US", "equity", USBenchmarkAssetID}, {"USD", "CN", "equity", USBenchmarkAssetID},
		{"USD", "US", "crypto", USBenchmarkAssetID}, {"USD", "US", "equity", "index:CN:000300"},
	} {
		if err := ValidateFundamental(policy, input.currency, input.market, input.class, input.benchmark); err == nil {
			t.Fatalf("cross-scope inputs were accepted: %#v", input)
		}
	}
	if err := ValidateFundamental(Resolve("crypto", "CRYPTO"), "USD", "CRYPTO", "crypto", "crypto:coingecko:bitcoin"); err == nil {
		t.Fatal("equity fundamental workflow was accepted for crypto")
	}
}

func TestNewAssetReadinessWaitsForCoreMarketAndScopeSpecificEvidence(t *testing.T) {
	crypto := ReadinessSegment{AssetClass: "crypto", ActiveAssets: 10, ApprovedModels: 1, PredictionRuns: 120, CalibratedRuns: 120, MatureOutcomes: 120, Reasons: []string{}}
	classifyReadiness(&crypto, false, 100)
	if crypto.AcceptanceStatus != "blocked" || len(crypto.Reasons) != 1 || crypto.Reasons[0] != "core_equity_market_not_yet_accepted" || crypto.AutomaticModelRelease {
		t.Fatalf("crypto bypassed core-market gate: %#v", crypto)
	}
	crypto.Reasons = []string{}
	classifyReadiness(&crypto, true, 100)
	if crypto.AcceptanceStatus != "eligible_for_human_acceptance" || crypto.EffectReportStatus != "scope_specific_evidence_available" || crypto.AutomaticModelRelease {
		t.Fatalf("scope-specific evidence did not reach human-only acceptance: %#v", crypto)
	}
	commodity := ReadinessSegment{AssetClass: "commodity", ActiveAssets: 3, PredictionRuns: 5, Reasons: []string{}}
	classifyReadiness(&commodity, true, 100)
	if commodity.EffectReportStatus != "partial_scope_specific_evidence" || commodity.AcceptanceStatus != "blocked" {
		t.Fatalf("partial commodity evidence was overstated: %#v", commodity)
	}
}
