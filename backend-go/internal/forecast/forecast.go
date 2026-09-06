// Package forecast contains deterministic financial projection math. It does
// not source numbers or decide assumptions: callers must supply time-stamped
// facts and explicitly approved, evidence-linked changes.
package forecast

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const ModelVersion = "financial-projection-v1"

type Inputs struct {
	Currency        string   `json:"currency"`
	Unit            string   `json:"unit"`
	Revenue         *float64 `json:"revenue"`
	OperatingMargin *float64 `json:"operating_margin"`
	TaxRate         *float64 `json:"tax_rate"`
	Depreciation    *float64 `json:"depreciation"`
	Capex           *float64 `json:"capex"`
	ChangeNWC       *float64 `json:"change_nwc"`
	DilutedShares   *float64 `json:"diluted_shares"`
}

type Assumption struct {
	Field           string     `json:"field"`
	Value           float64    `json:"value"`
	EventID         string     `json:"event_id,omitempty"`
	EvidenceIDs     []string   `json:"evidence_ids"`
	Condition       string     `json:"condition,omitempty"`
	FiscalPeriodEnd *time.Time `json:"fiscal_period_end,omitempty"`
	Approved        bool       `json:"approved"`
}

type Result struct {
	Status        string         `json:"status"`
	Reason        string         `json:"reason,omitempty"`
	ModelVersion  string         `json:"model_version"`
	Currency      string         `json:"currency,omitempty"`
	Unit          string         `json:"unit,omitempty"`
	Facts         map[string]any `json:"facts"`
	Assumptions   []Assumption   `json:"assumptions"`
	Projected     map[string]any `json:"projected"`
	MissingFields []string       `json:"missing_fields"`
}

// Calculate applies approved explicit assumptions to a baseline. Percentages
// are decimal values (0.10 means ten percent); changes to margins and tax rate
// are percentage-point deltas. An unapproved assumption is rejected, rather
// than silently entering a valuation or rating downstream.
func Calculate(input Inputs, assumptions []Assumption) Result {
	result := Result{Status: "unavailable", ModelVersion: ModelVersion, Currency: strings.ToUpper(strings.TrimSpace(input.Currency)), Unit: strings.ToLower(strings.TrimSpace(input.Unit)), Facts: map[string]any{}, Assumptions: append([]Assumption{}, assumptions...), Projected: map[string]any{}, MissingFields: missingInputs(input)}
	if len(result.MissingFields) > 0 {
		result.Reason = "missing_required_financial_inputs"
		return result
	}
	if result.Unit == "" {
		result.Unit = "reported"
	}
	if result.Unit != "reported" && result.Unit != "thousands" && result.Unit != "millions" && result.Unit != "billions" {
		result.Reason = "invalid_financial_unit"
		return result
	}
	values := map[string]float64{"revenue": *input.Revenue, "operating_margin": *input.OperatingMargin, "tax_rate": *input.TaxRate, "depreciation": *input.Depreciation, "capex": *input.Capex, "change_nwc": *input.ChangeNWC, "diluted_shares": *input.DilutedShares}
	for _, assumption := range assumptions {
		if !assumption.Approved {
			result.Reason = "unapproved_assumption"
			return result
		}
		if len(assumption.EvidenceIDs) == 0 {
			result.Reason = "assumption_missing_evidence"
			return result
		}
		switch assumption.Field {
		case "revenue_growth":
			values["revenue"] *= 1 + assumption.Value
		case "revenue_delta":
			values["revenue"] += assumption.Value
		case "operating_margin_delta":
			values["operating_margin"] += assumption.Value
		case "tax_rate_delta":
			values["tax_rate"] += assumption.Value
		case "capex_delta":
			values["capex"] += assumption.Value
		case "change_nwc_delta":
			values["change_nwc"] += assumption.Value
		case "diluted_shares_delta":
			values["diluted_shares"] += assumption.Value
		default:
			result.Reason = "unsupported_assumption_field"
			return result
		}
	}
	if values["revenue"] < 0 || values["diluted_shares"] <= 0 || values["operating_margin"] < -1 || values["operating_margin"] > 1 || values["tax_rate"] < 0 || values["tax_rate"] > 1 {
		result.Reason = "invalid_projected_financial_inputs"
		return result
	}
	operatingIncome := values["revenue"] * values["operating_margin"]
	tax := math.Max(operatingIncome, 0) * values["tax_rate"]
	netIncome := operatingIncome - tax
	freeCashFlow := netIncome + values["depreciation"] - values["capex"] - values["change_nwc"]
	eps := netIncome / values["diluted_shares"]
	result.Status = "available"
	result.Facts = map[string]any{"revenue": *input.Revenue, "operating_margin": *input.OperatingMargin, "tax_rate": *input.TaxRate, "depreciation": *input.Depreciation, "capex": *input.Capex, "change_nwc": *input.ChangeNWC, "diluted_shares": *input.DilutedShares}
	result.Projected = map[string]any{"revenue": values["revenue"], "operating_income": operatingIncome, "tax": tax, "net_income": netIncome, "free_cash_flow": freeCashFlow, "diluted_eps": eps, "diluted_shares": values["diluted_shares"]}
	return result
}

func missingInputs(input Inputs) []string {
	values := []struct {
		name  string
		value *float64
	}{{"revenue", input.Revenue}, {"operating_margin", input.OperatingMargin}, {"tax_rate", input.TaxRate}, {"depreciation", input.Depreciation}, {"capex", input.Capex}, {"change_nwc", input.ChangeNWC}, {"diluted_shares", input.DilutedShares}}
	missing := []string{}
	for _, item := range values {
		if item.value == nil {
			missing = append(missing, item.name)
		}
	}
	return missing
}

func ValidateAssumption(value Assumption) error {
	if strings.TrimSpace(value.Field) == "" || len(value.EvidenceIDs) == 0 {
		return fmt.Errorf("forecast assumption requires field and evidence_ids")
	}
	if strings.TrimSpace(value.EventID) != "" && value.FiscalPeriodEnd == nil {
		return fmt.Errorf("event-linked forecast assumption requires fiscal_period_end")
	}
	return nil
}
