package fundamentalresearch

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
)

const PreparationVersion = "fundamental-research-preparation-v1"

type FactualFieldLineage struct {
	SnapshotID string             `json:"snapshot_id"`
	Metrics    []string           `json:"metrics"`
	RawValues  map[string]float64 `json:"raw_values"`
	Transform  string             `json:"transform"`
}

type PreparationControls struct {
	AutomaticAssumptions bool `json:"automatic_assumptions"`
	AutomaticValuation   bool `json:"automatic_valuation"`
	AutomaticRating      bool `json:"automatic_rating"`
	AnalystApproval      bool `json:"analyst_approval_required"`
}

type Preparation struct {
	Version               string                         `json:"version"`
	AssetID               string                         `json:"asset_id"`
	AsOf                  time.Time                      `json:"as_of"`
	Status                string                         `json:"status"`
	Reason                string                         `json:"reason,omitempty"`
	StatementPeriodEnd    *time.Time                     `json:"statement_period_end,omitempty"`
	FiscalPeriod          string                         `json:"fiscal_period,omitempty"`
	SnapshotIDs           []string                       `json:"fundamental_snapshot_ids"`
	FactualInputs         forecast.Inputs                `json:"factual_inputs"`
	FieldLineage          map[string]FactualFieldLineage `json:"field_lineage"`
	MissingFields         []string                       `json:"missing_fields"`
	AnalystInputsRequired []string                       `json:"analyst_inputs_required"`
	Controls              PreparationControls            `json:"controls"`
	WorkflowTemplate      *Input                         `json:"workflow_template,omitempty"`
	MarketPolicy          marketpolicy.Policy            `json:"market_policy"`
}

type PreparationService struct{ db *pgxpool.Pool }

func NewPreparationService(db *pgxpool.Pool) *PreparationService {
	return &PreparationService{db: db}
}

// Prepare builds a read-only, point-in-time analyst worksheet from one common
// reporting period. It normalizes provider cash-flow signs but never chooses a
// forecast assumption, valuation scenario, benchmark expectation, or rating.
func (s *PreparationService) Prepare(ctx context.Context, assetID string, cutoff time.Time) (Preparation, error) {
	assetID = strings.TrimSpace(assetID)
	result := Preparation{
		Version: PreparationVersion, AssetID: assetID, AsOf: cutoff.UTC(), Status: "unavailable",
		SnapshotIDs: []string{}, FieldLineage: map[string]FactualFieldLineage{}, MissingFields: []string{},
		AnalystInputsRequired: analystInputsRequired(),
		Controls:              PreparationControls{AutomaticAssumptions: false, AutomaticValuation: false, AutomaticRating: false, AnalystApproval: true},
	}
	if s.db == nil || assetID == "" || cutoff.IsZero() {
		return result, fmt.Errorf("fundamental preparation store, asset_id and as_of are required")
	}
	var assetClass, market, currency string
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency FROM assets WHERE id=$1 AND active=true`, assetID).Scan(&assetClass, &market, &currency); err != nil {
		return result, fmt.Errorf("load active asset for fundamental preparation: %w", err)
	}
	result.MarketPolicy = marketpolicy.Resolve(assetClass, market)
	if !result.MarketPolicy.FundamentalSupported {
		result.Status, result.Reason = "not_applicable", result.MarketPolicy.Reason
		return result, nil
	}
	if !strings.EqualFold(result.MarketPolicy.Market, "US") || !strings.EqualFold(result.MarketPolicy.AssetClass, "equity") {
		result.Status, result.Reason = "not_applicable", "first_fundamental_preparation_market_is_us_equity_only"
		return result, nil
	}
	if !strings.EqualFold(strings.TrimSpace(currency), result.MarketPolicy.Currency) {
		result.Status, result.Reason = "not_applicable", "asset_currency_outside_market_policy"
		return result, nil
	}
	snapshots, err := fundamentals.NewStore(s.db).ListAvailable(ctx, assetID, cutoff.UTC(), 100)
	if err != nil {
		return result, err
	}
	selected := commonStatementPeriod(snapshots)
	if len(selected) != 3 {
		result.Reason = "no_common_financial_statement_period"
		result.MissingFields = missingCommonPeriodFields(snapshots)
		return result, nil
	}
	income, balance, cashFlow := selected[fundamentals.IncomeStatement], selected[fundamentals.BalanceSheet], selected[fundamentals.CashFlow]
	period := income.ReportPeriodEnd.UTC()
	result.StatementPeriodEnd, result.FiscalPeriod = &period, income.FiscalPeriod
	result.SnapshotIDs = []string{income.ID, balance.ID, cashFlow.ID}
	sort.Strings(result.SnapshotIDs)
	if !sameFinancialUnits(income, balance, cashFlow) {
		result.Reason = "financial_statement_currency_or_unit_mismatch"
		result.MissingFields = []string{"statement.currency_and_unit_consistency"}
		return result, nil
	}
	inputs, lineage, missing := deriveFactualInputs(income, cashFlow)
	result.FactualInputs, result.FieldLineage, result.MissingFields = inputs, lineage, missing
	result.WorkflowTemplate = &Input{
		AssetID: assetID, AsOf: cutoff.UTC(),
		Forecast:  ForecastPlan{Inputs: inputs, FundamentalSnapshotIDs: append([]string{}, result.SnapshotIDs...), Assumptions: []forecast.Assumption{}},
		Valuation: ValuationPlan{},
		Rating:    RatingPlan{Policy: rating.DefaultUSPolicy(), ReasonCodes: []string{}, ChangedAssumptions: map[string]any{}, EvidenceIDs: append([]string{}, result.SnapshotIDs...), InvalidationRules: []rating.InvalidationRule{}},
	}
	if len(missing) > 0 {
		result.Reason = "missing_factual_forecast_inputs"
		return result, nil
	}
	result.Status = "analyst_review_required"
	result.Reason = "factual_inputs_ready_but_assumptions_valuation_benchmark_and_rating_require_approval"
	return result, nil
}

func sameFinancialUnits(items ...fundamentals.Snapshot) bool {
	if len(items) == 0 {
		return false
	}
	currency := strings.ToUpper(strings.TrimSpace(items[0].Currency))
	unit := strings.ToLower(strings.TrimSpace(items[0].Unit))
	if currency == "" || unit == "" {
		return false
	}
	for _, item := range items[1:] {
		if !strings.EqualFold(strings.TrimSpace(item.Currency), currency) || !strings.EqualFold(strings.TrimSpace(item.Unit), unit) {
			return false
		}
	}
	return true
}

func commonStatementPeriod(items []fundamentals.Snapshot) map[fundamentals.StatementType]fundamentals.Snapshot {
	periods := map[string]map[fundamentals.StatementType]fundamentals.Snapshot{}
	keys := []string{}
	for _, item := range items {
		key := item.ReportPeriodEnd.UTC().Format("2006-01-02") + "|" + strings.ToUpper(strings.TrimSpace(item.FiscalPeriod))
		if periods[key] == nil {
			periods[key] = map[fundamentals.StatementType]fundamentals.Snapshot{}
			keys = append(keys, key)
		}
		if _, exists := periods[key][item.StatementType]; !exists {
			periods[key][item.StatementType] = item
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	for _, key := range keys {
		candidate := periods[key]
		if candidate[fundamentals.IncomeStatement].ID != "" && candidate[fundamentals.BalanceSheet].ID != "" && candidate[fundamentals.CashFlow].ID != "" {
			return candidate
		}
	}
	return map[fundamentals.StatementType]fundamentals.Snapshot{}
}

func missingCommonPeriodFields(items []fundamentals.Snapshot) []string {
	present := map[fundamentals.StatementType]bool{}
	for _, item := range items {
		present[item.StatementType] = true
	}
	missing := []string{}
	for _, kind := range []fundamentals.StatementType{fundamentals.IncomeStatement, fundamentals.BalanceSheet, fundamentals.CashFlow} {
		if !present[kind] {
			missing = append(missing, "statement."+string(kind))
		}
	}
	if len(missing) == 0 {
		return []string{"statement.common_report_period"}
	}
	return missing
}

func deriveFactualInputs(income, cashFlow fundamentals.Snapshot) (forecast.Inputs, map[string]FactualFieldLineage, []string) {
	inputs := forecast.Inputs{Currency: strings.ToUpper(strings.TrimSpace(income.Currency)), Unit: strings.ToLower(strings.TrimSpace(income.Unit))}
	lineage := map[string]FactualFieldLineage{}
	missing := []string{}
	revenue, revenueOK := metricNumber(income.Metrics["revenue"])
	if revenueOK {
		inputs.Revenue = numberPointer(revenue)
		lineage["revenue"] = directLineage(income, "revenue", revenue)
	}
	operatingIncome, operatingIncomeOK := metricNumber(income.Metrics["operatingIncome"])
	if revenueOK && revenue != 0 && operatingIncomeOK {
		value := operatingIncome / revenue
		inputs.OperatingMargin = numberPointer(value)
		lineage["operating_margin"] = ratioLineage(income, "operatingIncome", operatingIncome, "revenue", revenue)
	}
	pretax, pretaxOK := metricNumber(income.Metrics["incomeBeforeTax"])
	tax, taxOK := metricNumber(income.Metrics["incomeTaxExpense"])
	if pretaxOK && pretax > 0 && taxOK && tax >= 0 {
		value := tax / pretax
		if value >= 0 && value <= 1 {
			inputs.TaxRate = numberPointer(value)
			lineage["tax_rate"] = ratioLineage(income, "incomeTaxExpense", tax, "incomeBeforeTax", pretax)
		}
	}
	depreciation, depreciationOK := metricNumber(cashFlow.Metrics["depreciationAndAmortization"])
	depreciationSnapshot := cashFlow
	if !depreciationOK {
		depreciation, depreciationOK = metricNumber(income.Metrics["depreciationAndAmortization"])
		depreciationSnapshot = income
	}
	if depreciationOK {
		inputs.Depreciation = numberPointer(depreciation)
		lineage["depreciation"] = directLineage(depreciationSnapshot, "depreciationAndAmortization", depreciation)
	}
	capex, capexOK := metricNumber(cashFlow.Metrics["capitalExpenditure"])
	capexMetric := "capitalExpenditure"
	if !capexOK {
		capex, capexOK = metricNumber(cashFlow.Metrics["investmentsInPropertyPlantAndEquipment"])
		capexMetric = "investmentsInPropertyPlantAndEquipment"
	}
	if capexOK {
		value := math.Abs(capex)
		inputs.Capex = numberPointer(value)
		lineage["capex"] = FactualFieldLineage{SnapshotID: cashFlow.ID, Metrics: []string{capexMetric}, RawValues: map[string]float64{capexMetric: capex}, Transform: "absolute_cash_outflow"}
	}
	changeNWC, changeNWCOK := metricNumber(cashFlow.Metrics["changeInWorkingCapital"])
	if changeNWCOK {
		value := -changeNWC
		inputs.ChangeNWC = numberPointer(value)
		lineage["change_nwc"] = FactualFieldLineage{SnapshotID: cashFlow.ID, Metrics: []string{"changeInWorkingCapital"}, RawValues: map[string]float64{"changeInWorkingCapital": changeNWC}, Transform: "invert_provider_cash_flow_sign"}
	}
	shares, sharesOK := metricNumber(income.Metrics["weightedAverageShsOutDil"])
	if sharesOK && shares > 0 {
		inputs.DilutedShares = numberPointer(shares)
		lineage["diluted_shares"] = directLineage(income, "weightedAverageShsOutDil", shares)
	}
	checks := []struct {
		name  string
		value *float64
	}{{"revenue", inputs.Revenue}, {"operating_margin", inputs.OperatingMargin}, {"tax_rate", inputs.TaxRate}, {"depreciation", inputs.Depreciation}, {"capex", inputs.Capex}, {"change_nwc", inputs.ChangeNWC}, {"diluted_shares", inputs.DilutedShares}}
	for _, check := range checks {
		if check.value == nil {
			missing = append(missing, check.name)
		}
	}
	return inputs, lineage, missing
}

func directLineage(snapshot fundamentals.Snapshot, metric string, value float64) FactualFieldLineage {
	return FactualFieldLineage{SnapshotID: snapshot.ID, Metrics: []string{metric}, RawValues: map[string]float64{metric: value}, Transform: "identity"}
}

func ratioLineage(snapshot fundamentals.Snapshot, numerator string, numeratorValue float64, denominator string, denominatorValue float64) FactualFieldLineage {
	return FactualFieldLineage{SnapshotID: snapshot.ID, Metrics: []string{numerator, denominator}, RawValues: map[string]float64{numerator: numeratorValue, denominator: denominatorValue}, Transform: numerator + "_divided_by_" + denominator}
}

func metricNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case float32:
		value := float64(typed)
		return value, !math.IsNaN(value) && !math.IsInf(value, 0)
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	default:
		return 0, false
	}
}

func numberPointer(value float64) *float64 { return &value }

func analystInputsRequired() []string {
	return []string{
		"forecast_assumption_review",
		"valuation_scenarios_and_evidence",
		"current_adjusted_price_evidence",
		"benchmark_expected_return_and_evidence",
		"rating_reason_codes",
		"rating_invalidation_rules",
		"schedule_approved_by",
	}
}
