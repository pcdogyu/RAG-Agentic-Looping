// Package marketpolicy prevents equity-specific valuation and calibration
// assumptions from leaking across asset classes and markets.
package marketpolicy

import (
	"fmt"
	"strings"
)

const (
	USBenchmarkAssetID     = "equity:AMEX:SPY"
	CNBenchmarkAssetID     = "index:CN:000300"
	HKBenchmarkAssetID     = "index:HK:HSI"
	CryptoBenchmarkAssetID = "crypto:coingecko:bitcoin"
)

type Policy struct {
	Version              string   `json:"version"`
	AssetClass           string   `json:"asset_class"`
	Market               string   `json:"market"`
	Currency             string   `json:"currency"`
	TimeZone             string   `json:"time_zone"`
	Calendar             string   `json:"calendar"`
	BenchmarkID          string   `json:"benchmark_id,omitempty"`
	BenchmarkPolicy      string   `json:"benchmark_policy"`
	FundamentalMethod    string   `json:"fundamental_method"`
	PredictionScope      string   `json:"prediction_scope"`
	OutcomePriceField    string   `json:"outcome_price_field"`
	FundamentalSupported bool     `json:"fundamental_supported"`
	PredictionSupported  bool     `json:"prediction_supported"`
	RequiredInputs       []string `json:"required_inputs"`
	ExecutionConstraints []string `json:"execution_constraints"`
	Supported            bool     `json:"supported"`
	Reason               string   `json:"reason,omitempty"`
}

func Resolve(assetClass, market string) Policy {
	assetClass, market = strings.ToLower(strings.TrimSpace(assetClass)), strings.ToUpper(strings.TrimSpace(market))
	base := Policy{Version: "market-policy-v3", AssetClass: assetClass, Market: market, PredictionScope: assetClass + ":" + market,
		OutcomePriceField: "adjusted_close", BenchmarkPolicy: "approved_point_in_time_mapping", RequiredInputs: []string{}, ExecutionConstraints: []string{}}
	switch {
	case assetClass == "equity" && market == "US":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod = "USD", "America/New_York", "XNYS", USBenchmarkAssetID, "fcff_wacc_or_pe"
	case assetClass == "equity" && market == "CN":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod = "CNY", "Asia/Shanghai", "XSHG", CNBenchmarkAssetID, "fcff_wacc_or_pe"
	case assetClass == "equity" && market == "HK":
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod = "HKD", "Asia/Hong_Kong", "XHKG", HKBenchmarkAssetID, "fcff_wacc_or_pe"
	case assetClass == "etf" && (market == "US" || market == "CN" || market == "HK"):
		base.Currency, base.TimeZone, base.Calendar = marketIdentity(market)
		base.FundamentalMethod, base.PredictionSupported, base.Supported = "nav_holdings_and_tracking_error", true, true
		base.RequiredInputs = []string{"point_in_time_nav", "holdings_snapshot", "tracking_error", "expense_ratio"}
		base.BenchmarkPolicy = "approved_point_in_time_fund_specific_mapping"
		base.ExecutionConstraints = []string{"exchange_calendar", "premium_discount_to_nav", "fund_liquidity", "expense_ratio"}
		base.Reason = "fundamental_rating_requires_etf_specific_valuation_plugin"
	case assetClass == "commodity" && (market == "GLOBAL" || market == "COMMODITY"):
		base.Currency, base.TimeZone, base.Calendar = "USD", "UTC", "commodity_sessions"
		base.FundamentalMethod, base.PredictionSupported, base.Supported = "supply_demand_curve", true, true
		base.RequiredInputs = []string{"contract_specification", "term_structure", "inventory", "supply_demand_balance"}
		base.BenchmarkPolicy = "approved_point_in_time_contract_or_spot_mapping"
		base.ExecutionConstraints = []string{"contract_multiplier", "expiry_and_roll", "session_calendar", "liquidity_and_slippage"}
		base.Reason = "fundamental_rating_not_applicable_to_commodity"
	case assetClass == "crypto" && (market == "CRYPTO" || market == "GLOBAL"):
		base.Currency, base.TimeZone, base.Calendar, base.BenchmarkID, base.FundamentalMethod = "USD", "UTC", "24x7", CryptoBenchmarkAssetID, "network_tokenomics"
		base.PredictionSupported, base.Supported = true, true
		base.RequiredInputs = []string{"point_in_time_supply", "network_activity", "protocol_fees", "tokenomics"}
		base.BenchmarkPolicy = "approved_point_in_time_crypto_mapping"
		base.ExecutionConstraints = []string{"24x7_calendar", "venue_liquidity", "funding_and_borrow", "custody_and_network_risk"}
		base.Reason = "fundamental_rating_requires_crypto_specific_valuation_plugin"
	default:
		base.Reason = "unsupported_asset_market_policy"
	}
	if assetClass == "equity" && base.FundamentalMethod != "" {
		base.FundamentalSupported, base.PredictionSupported, base.Supported = true, true, true
		base.RequiredInputs = []string{"point_in_time_financials", "forecast_assumptions", "as_of_price", "benchmark_return"}
		base.ExecutionConstraints = []string{"exchange_calendar", "lot_size", "currency", "liquidity_and_slippage"}
	}
	return base
}

// IsCanonicalBenchmarkAsset reports whether an exact master-data identity is
// frozen in a market policy as a benchmark. Canonical benchmark instruments
// may intentionally stay outside the active research universe while their
// immutable price observations still need to be collected.
func IsCanonicalBenchmarkAsset(assetID string) bool {
	switch strings.TrimSpace(assetID) {
	case USBenchmarkAssetID, CNBenchmarkAssetID, HKBenchmarkAssetID, CryptoBenchmarkAssetID:
		return true
	default:
		return false
	}
}

func ValidateFundamental(policy Policy, currency, ratingMarket, ratingAssetClass, benchmarkID string) error {
	if !policy.FundamentalSupported {
		return fmt.Errorf("%s", fallbackReason(policy.Reason, "fundamental_model_not_supported"))
	}
	if currency = strings.ToUpper(strings.TrimSpace(currency)); currency == "" || currency != policy.Currency {
		return fmt.Errorf("forecast_currency_outside_market_policy")
	}
	if !strings.EqualFold(strings.TrimSpace(ratingMarket), policy.Market) || !strings.EqualFold(strings.TrimSpace(ratingAssetClass), policy.AssetClass) {
		return fmt.Errorf("rating_policy_outside_asset_market_scope")
	}
	if policy.BenchmarkID != "" && strings.TrimSpace(benchmarkID) != policy.BenchmarkID {
		return fmt.Errorf("rating_benchmark_outside_market_policy")
	}
	return nil
}

func marketIdentity(market string) (currency, timeZone, calendar string) {
	switch market {
	case "US":
		return "USD", "America/New_York", "XNYS"
	case "CN":
		return "CNY", "Asia/Shanghai", "XSHG"
	case "HK":
		return "HKD", "Asia/Hong_Kong", "XHKG"
	default:
		return "", "", ""
	}
}

func fallbackReason(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
