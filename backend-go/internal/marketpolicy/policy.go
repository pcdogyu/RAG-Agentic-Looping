// Package marketpolicy prevents equity-specific valuation and calibration
// assumptions from leaking across asset classes and markets.
package marketpolicy

import "strings"

type Policy struct {
	Version           string `json:"version"`
	AssetClass        string `json:"asset_class"`
	Market            string `json:"market"`
	Currency          string `json:"currency"`
	TimeZone          string `json:"time_zone"`
	Calendar          string `json:"calendar"`
	BenchmarkID       string `json:"benchmark_id,omitempty"`
	FundamentalMethod string `json:"fundamental_method"`
	PredictionScope   string `json:"prediction_scope"`
	Supported         bool   `json:"supported"`
	Reason            string `json:"reason,omitempty"`
}

func Resolve(assetClass, market string) Policy {
	assetClass, market = strings.ToLower(strings.TrimSpace(assetClass)), strings.ToUpper(strings.TrimSpace(market))
	base := Policy{Version: "market-policy-v1", AssetClass: assetClass, Market: market, PredictionScope: assetClass + ":" + market}
	switch {
	case assetClass == "equity" && market == "US":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod, base.Supported = "USD", "America/New_York", "XNYS", "equity:NYSEARCA:SPY", "fcff_wacc_or_pe", true
	case assetClass == "equity" && market == "CN":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod, base.Supported = "CNY", "Asia/Shanghai", "XSHG", "index:CN:000300", "fcff_wacc_or_pe", true
	case assetClass == "equity" && market == "HK":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod, base.Supported = "HKD", "Asia/Hong_Kong", "XHKG", "index:HK:HSI", "fcff_wacc_or_pe", true
	case assetClass == "etf":
		base.FundamentalMethod, base.Reason = "nav_holdings_and_tracking_error", "requires_etf_specific_inputs"
	case assetClass == "commodity":
		base.FundamentalMethod, base.Reason = "supply_demand_curve", "requires_commodity_specific_inputs"
	case assetClass == "crypto":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod, base.Reason = "USD", "UTC", "24x7", "crypto:coingecko:bitcoin", "network_tokenomics", "requires_crypto_specific_inputs"
	default:
		base.Reason = "unsupported_asset_market_policy"
	}
	return base
}
