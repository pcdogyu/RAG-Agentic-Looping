// Package valuation contains deterministic, source-input-only valuation math.
// Its scenarios are model ranges, never statistical confidence intervals or
// probabilities. Fetching facts, choosing comparables and assigning ratings
// belong to explicit callers and policies outside this package.
package valuation

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

const ModelVersion = "valuation-v1"

type ForecastFacts struct {
	Currency      string  `json:"currency"`
	Unit          string  `json:"unit"`
	FCFF          float64 `json:"fcff"`
	NetIncome     float64 `json:"net_income"`
	DilutedShares float64 `json:"diluted_shares"`
}

type DCFScenario struct {
	Name                     string   `json:"name"`
	WACC                     float64  `json:"wacc"`
	TerminalGrowth           float64  `json:"terminal_growth"`
	ProjectionYears          int      `json:"projection_years"`
	NetDebt                  float64  `json:"net_debt"`
	CostOfCapitalEvidenceIDs []string `json:"cost_of_capital_evidence_ids"`
}

type MultipleScenario struct {
	Name                  string   `json:"name"`
	PriceEarningsMultiple float64  `json:"price_earnings_multiple"`
	ComparableEvidenceIDs []string `json:"comparable_evidence_ids"`
}

type ScenarioResult struct {
	Name            string   `json:"name"`
	Method          string   `json:"method"`
	Status          string   `json:"status"`
	Reason          string   `json:"reason,omitempty"`
	EnterpriseValue *float64 `json:"enterprise_value,omitempty"`
	EquityValue     *float64 `json:"equity_value,omitempty"`
	ValuePerShare   *float64 `json:"value_per_share,omitempty"`
}

type SensitivityCell struct {
	WACC           float64  `json:"wacc"`
	TerminalGrowth float64  `json:"terminal_growth"`
	ValuePerShare  *float64 `json:"value_per_share,omitempty"`
	Status         string   `json:"status"`
	Reason         string   `json:"reason,omitempty"`
}

type Result struct {
	Status           string              `json:"status"`
	Reason           string              `json:"reason,omitempty"`
	ModelVersion     string              `json:"model_version"`
	Currency         string              `json:"currency,omitempty"`
	Unit             string              `json:"unit,omitempty"`
	Scenarios        []ScenarioResult    `json:"scenarios"`
	Sensitivity      []SensitivityCell   `json:"dcf_sensitivity"`
	Range            map[string]*float64 `json:"range"`
	RangeDescription string              `json:"range_description"`
}

// Calculate produces DCF (FCFF/WACC) and P/E scenario outputs from explicit
// forecast facts. It refuses to mix FCFE with WACC and refuses a Gordon-growth
// terminal value when WACC is not greater than terminal growth.
func Calculate(facts ForecastFacts, dcf []DCFScenario, multiples []MultipleScenario) Result {
	result := Result{Status: "unavailable", ModelVersion: ModelVersion, Currency: strings.ToUpper(strings.TrimSpace(facts.Currency)), Unit: strings.ToLower(strings.TrimSpace(facts.Unit)), Scenarios: []ScenarioResult{}, Sensitivity: []SensitivityCell{}, Range: map[string]*float64{}, RangeDescription: "scenario/model range; not a statistical confidence interval"}
	if err := validateFacts(facts); err != nil {
		result.Reason = err.Error()
		return result
	}
	for _, item := range dcf {
		result.Scenarios = append(result.Scenarios, calculateDCF(facts, item))
	}
	for _, item := range multiples {
		result.Scenarios = append(result.Scenarios, calculatePE(facts, item))
	}
	if len(result.Scenarios) == 0 {
		result.Reason = "no_valuation_scenarios"
		return result
	}
	shares := valuesPerShare(result.Scenarios)
	if len(shares) == 0 {
		result.Reason = "no_valid_valuation_scenarios"
		return result
	}
	sort.Float64s(shares)
	low, high := shares[0], shares[len(shares)-1]
	result.Range["low"] = &low
	result.Range["high"] = &high
	result.Status = "available"
	return result
}

func Sensitivity(facts ForecastFacts, scenario DCFScenario, waccValues, terminalGrowthValues []float64) []SensitivityCell {
	items := []SensitivityCell{}
	for _, wacc := range waccValues {
		for _, growth := range terminalGrowthValues {
			copy := scenario
			copy.WACC, copy.TerminalGrowth = wacc, growth
			out := calculateDCF(facts, copy)
			cell := SensitivityCell{WACC: wacc, TerminalGrowth: growth, Status: out.Status, Reason: out.Reason, ValuePerShare: out.ValuePerShare}
			items = append(items, cell)
		}
	}
	return items
}

func calculateDCF(facts ForecastFacts, scenario DCFScenario) ScenarioResult {
	result := ScenarioResult{Name: strings.TrimSpace(scenario.Name), Method: "fcff_wacc_dcf", Status: "unavailable"}
	if result.Name == "" || len(cleanIDs(scenario.CostOfCapitalEvidenceIDs)) == 0 {
		result.Reason = "dcf_scenario_requires_name_and_cost_of_capital_evidence"
		return result
	}
	if scenario.ProjectionYears < 1 || scenario.ProjectionYears > 30 || scenario.WACC <= 0 || scenario.WACC >= 1 || scenario.TerminalGrowth < -1 || scenario.WACC <= scenario.TerminalGrowth {
		result.Reason = "invalid_fcff_wacc_terminal_growth_assumptions"
		return result
	}
	if err := validateFacts(facts); err != nil {
		result.Reason = err.Error()
		return result
	}
	presentValue := 0.0
	fcff := facts.FCFF
	for year := 1; year <= scenario.ProjectionYears; year++ {
		presentValue += fcff / math.Pow(1+scenario.WACC, float64(year))
	}
	terminal := fcff * (1 + scenario.TerminalGrowth) / (scenario.WACC - scenario.TerminalGrowth)
	presentValue += terminal / math.Pow(1+scenario.WACC, float64(scenario.ProjectionYears))
	equity := presentValue - scenario.NetDebt
	perShare := equity / facts.DilutedShares
	result.Status = "available"
	result.EnterpriseValue, result.EquityValue, result.ValuePerShare = &presentValue, &equity, &perShare
	return result
}

func calculatePE(facts ForecastFacts, scenario MultipleScenario) ScenarioResult {
	result := ScenarioResult{Name: strings.TrimSpace(scenario.Name), Method: "price_earnings", Status: "unavailable"}
	if result.Name == "" || len(cleanIDs(scenario.ComparableEvidenceIDs)) == 0 {
		result.Reason = "multiple_scenario_requires_name_and_comparable_evidence"
		return result
	}
	if scenario.PriceEarningsMultiple <= 0 || scenario.PriceEarningsMultiple > 200 || facts.NetIncome <= 0 {
		result.Reason = "invalid_price_earnings_assumptions"
		return result
	}
	if err := validateFacts(facts); err != nil {
		result.Reason = err.Error()
		return result
	}
	equity := facts.NetIncome * scenario.PriceEarningsMultiple
	perShare := equity / facts.DilutedShares
	result.Status = "available"
	result.EquityValue, result.ValuePerShare = &equity, &perShare
	return result
}

func validateFacts(facts ForecastFacts) error {
	if strings.TrimSpace(facts.Currency) == "" || facts.DilutedShares <= 0 {
		return fmt.Errorf("missing_or_invalid_forecast_facts")
	}
	if strings.TrimSpace(facts.Unit) == "" || !validUnit(facts.Unit) {
		return fmt.Errorf("invalid_financial_unit")
	}
	return nil
}

func valuesPerShare(values []ScenarioResult) []float64 {
	items := []float64{}
	for _, item := range values {
		if item.Status == "available" && item.ValuePerShare != nil && !math.IsNaN(*item.ValuePerShare) && !math.IsInf(*item.ValuePerShare, 0) {
			items = append(items, *item.ValuePerShare)
		}
	}
	return items
}

func cleanIDs(values []string) []string {
	seen := map[string]struct{}{}
	items := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			items = append(items, value)
		}
	}
	return items
}

func validUnit(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "reported" || value == "thousands" || value == "millions" || value == "billions"
}
