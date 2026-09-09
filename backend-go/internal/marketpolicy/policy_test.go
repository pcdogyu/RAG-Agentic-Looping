package marketpolicy

import "testing"

func TestMarketPoliciesDoNotApplyEquityDCFToOtherAssets(t *testing.T) {
	if policy := Resolve("equity", "US"); !policy.Supported || !policy.FundamentalSupported || !policy.PredictionSupported || policy.FundamentalMethod != "fcff_wacc_or_pe" || policy.BenchmarkID == "" {
		t.Fatalf("equity=%#v", policy)
	}
	for _, item := range []struct{ assetClass, market string }{{"crypto", "CRYPTO"}, {"commodity", "GLOBAL"}, {"etf", "US"}} {
		policy := Resolve(item.assetClass, item.market)
		if !policy.Supported || policy.FundamentalSupported || !policy.PredictionSupported || policy.FundamentalMethod == "fcff_wacc_or_pe" || policy.Reason == "" || len(policy.RequiredInputs) == 0 {
			t.Fatalf("%s=%#v", item.assetClass, policy)
		}
	}
	if policy := Resolve("crypto", "US"); policy.Supported {
		t.Fatalf("unsupported crypto market was accepted: %#v", policy)
	}
}

func TestFundamentalPolicyRejectsCrossMarketCurrencyAndBenchmark(t *testing.T) {
	policy := Resolve("equity", "US")
	if err := ValidateFundamental(policy, "USD", "US", "equity", "equity:US:SPY"); err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct{ currency, market, class, benchmark string }{
		{"CNY", "US", "equity", "equity:US:SPY"}, {"USD", "CN", "equity", "equity:US:SPY"},
		{"USD", "US", "crypto", "equity:US:SPY"}, {"USD", "US", "equity", "index:CN:000300"},
	} {
		if err := ValidateFundamental(policy, input.currency, input.market, input.class, input.benchmark); err == nil {
			t.Fatalf("cross-scope inputs were accepted: %#v", input)
		}
	}
	if err := ValidateFundamental(Resolve("crypto", "CRYPTO"), "USD", "CRYPTO", "crypto", "crypto:coingecko:bitcoin"); err == nil {
		t.Fatal("equity fundamental workflow was accepted for crypto")
	}
}
