// Package rating owns explicit fundamental-rating policy. It is intentionally
// independent from event-signal direction scores and does not infer a rating
// when valuation, price, horizon or benchmark inputs are missing.
package rating

import (
	"fmt"
	"strings"
)

const PolicyVersion = "fundamental-rating-v1"

type Policy struct {
	Version           string  `json:"version"`
	Market            string  `json:"market"`
	AssetClass        string  `json:"asset_class"`
	HorizonDays       int     `json:"horizon_days"`
	BenchmarkID       string  `json:"benchmark_id,omitempty"`
	RelativeRequired  bool    `json:"relative_required"`
	StrongBuyFloor    float64 `json:"strong_buy_floor"`
	BuyFloor          float64 `json:"buy_floor"`
	SellCeiling       float64 `json:"sell_ceiling"`
	StrongSellCeiling float64 `json:"strong_sell_ceiling"`
}

type Input struct {
	Policy           Policy   `json:"policy"`
	AsOfPrice        *float64 `json:"as_of_price"`
	TargetPrice      *float64 `json:"target_price"`
	ExpectedDividend *float64 `json:"expected_dividend,omitempty"`
	BenchmarkReturn  *float64 `json:"benchmark_return,omitempty"`
	ValuationRunID   string   `json:"valuation_run_id,omitempty"`
}

type Result struct {
	Status               string   `json:"status"`
	Reason               string   `json:"reason,omitempty"`
	PolicyVersion        string   `json:"policy_version"`
	Rating               string   `json:"rating,omitempty"`
	HorizonDays          int      `json:"horizon_days,omitempty"`
	BenchmarkID          string   `json:"benchmark_id,omitempty"`
	PriceReturn          *float64 `json:"price_return,omitempty"`
	DividendReturn       *float64 `json:"dividend_return,omitempty"`
	ExpectedReturn       *float64 `json:"expected_return,omitempty"`
	ExpectedExcessReturn *float64 `json:"expected_excess_return,omitempty"`
	ValuationRunID       string   `json:"valuation_run_id,omitempty"`
}

func DefaultUSPolicy() Policy {
	return Policy{Version: PolicyVersion, Market: "US", AssetClass: "equity", HorizonDays: 365, BenchmarkID: "equity:US:SPY", RelativeRequired: true, StrongBuyFloor: .20, BuyFloor: .05, SellCeiling: -.05, StrongSellCeiling: -.20}
}

func Evaluate(input Input) Result {
	policy := input.Policy
	if policy.Version == "" {
		policy.Version = PolicyVersion
	}
	result := Result{Status: "unavailable", PolicyVersion: policy.Version, HorizonDays: policy.HorizonDays, BenchmarkID: policy.BenchmarkID, ValuationRunID: strings.TrimSpace(input.ValuationRunID)}
	if err := validatePolicy(policy); err != nil {
		result.Reason = err.Error()
		return result
	}
	if input.AsOfPrice == nil || *input.AsOfPrice <= 0 || input.TargetPrice == nil {
		result.Reason = "missing_current_or_target_price"
		return result
	}
	dividend := 0.0
	if input.ExpectedDividend != nil {
		dividend = *input.ExpectedDividend
	}
	priceReturn := (*input.TargetPrice - *input.AsOfPrice) / *input.AsOfPrice
	dividendReturn := dividend / *input.AsOfPrice
	expectedReturn := priceReturn + dividendReturn
	result.PriceReturn, result.DividendReturn, result.ExpectedReturn = &priceReturn, &dividendReturn, &expectedReturn
	metric := expectedReturn
	if policy.RelativeRequired {
		if input.BenchmarkReturn == nil {
			result.Reason = "missing_benchmark_return"
			return result
		}
		excess := expectedReturn - *input.BenchmarkReturn
		result.ExpectedExcessReturn = &excess
		metric = excess
	}
	result.Status, result.Rating = "available", classify(metric, policy)
	return result
}

func validatePolicy(policy Policy) error {
	if strings.TrimSpace(policy.Version) == "" || strings.TrimSpace(policy.Market) == "" || strings.TrimSpace(policy.AssetClass) == "" || policy.HorizonDays < 1 {
		return fmt.Errorf("invalid_rating_policy")
	}
	if policy.RelativeRequired && strings.TrimSpace(policy.BenchmarkID) == "" {
		return fmt.Errorf("relative_rating_requires_benchmark")
	}
	if policy.StrongSellCeiling >= policy.SellCeiling || policy.SellCeiling >= policy.BuyFloor || policy.BuyFloor >= policy.StrongBuyFloor {
		return fmt.Errorf("invalid_rating_thresholds")
	}
	return nil
}

func classify(value float64, policy Policy) string {
	switch {
	case value >= policy.StrongBuyFloor:
		return "strong_buy"
	case value >= policy.BuyFloor:
		return "buy"
	case value <= policy.StrongSellCeiling:
		return "strong_sell"
	case value <= policy.SellCeiling:
		return "sell"
	default:
		return "hold"
	}
}
