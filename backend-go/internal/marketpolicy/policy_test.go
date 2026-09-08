package marketpolicy

import "testing"

func TestMarketPoliciesDoNotApplyEquityDCFToOtherAssets(t *testing.T) {
	if policy := Resolve("equity", "US"); !policy.Supported || policy.FundamentalMethod != "fcff_wacc_or_pe" || policy.BenchmarkID == "" {
		t.Fatalf("equity=%#v", policy)
	}
	for _, assetClass := range []string{"crypto", "commodity", "etf"} {
		policy := Resolve(assetClass, "US")
		if policy.Supported || policy.FundamentalMethod == "fcff_wacc_or_pe" || policy.Reason == "" {
			t.Fatalf("%s=%#v", assetClass, policy)
		}
	}
}
