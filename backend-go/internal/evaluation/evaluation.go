// Package evaluation defines point-in-time labels, rolling splits and layered
// reports for future-sample model evaluation.
package evaluation

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const OutcomeLabelDefinitionVersion = "prediction-outcome-label-v1"

type HorizonPolicy struct {
	Version          string  `json:"version"`
	Objective        string  `json:"objective"`
	HorizonSessions  int     `json:"horizon_sessions"`
	NeutralBand      float64 `json:"neutral_band"`
	ExecutionEnabled bool    `json:"execution_enabled"`
	ExecutionSide    string  `json:"execution_side,omitempty"`
	ExecutionCostBPS float64 `json:"execution_cost_bps"`
	SlippageBPS      float64 `json:"slippage_bps"`
	BorrowBPS        float64 `json:"borrow_bps"`
	FundingBPS       float64 `json:"funding_bps"`
	EntryPolicy      string  `json:"entry_policy"`
	ExitPolicy       string  `json:"exit_policy"`
	PriceField       string  `json:"price_field"`
	AlphaDefinition  string  `json:"alpha_definition"`
}

type ExecutionAssumptions struct {
	Enabled          bool    `json:"enabled"`
	Side             string  `json:"side,omitempty"`
	RoundTripCostBPS float64 `json:"round_trip_cost_bps,omitempty"`
	SlippageBPS      float64 `json:"slippage_bps,omitempty"`
	BorrowBPS        float64 `json:"borrow_bps,omitempty"`
	FundingBPS       float64 `json:"funding_bps,omitempty"`
}

func WithExecutionAssumptions(policy HorizonPolicy, input ExecutionAssumptions) (HorizonPolicy, error) {
	policy.ExecutionEnabled = input.Enabled
	policy.ExecutionSide = strings.ToLower(strings.TrimSpace(input.Side))
	policy.ExecutionCostBPS = input.RoundTripCostBPS
	policy.SlippageBPS = input.SlippageBPS
	policy.BorrowBPS = input.BorrowBPS
	policy.FundingBPS = input.FundingBPS
	if err := ValidateHorizonPolicy(policy); err != nil {
		return HorizonPolicy{}, err
	}
	return policy, nil
}

type PricePoint struct {
	SessionDate             time.Time `json:"session_date"`
	AvailableAt             time.Time `json:"available_at"`
	Close                   *float64  `json:"close,omitempty"`
	AdjustedClose           *float64  `json:"adjusted_close,omitempty"`
	Currency                string    `json:"currency"`
	CorporateActionAdjusted bool      `json:"corporate_action_adjusted"`
}

type OutcomeLabel struct {
	Status               string     `json:"status"`
	Reason               string     `json:"reason,omitempty"`
	DefinitionVersion    string     `json:"definition_version"`
	Objective            string     `json:"objective"`
	HorizonSessions      int        `json:"horizon_sessions"`
	PriceField           string     `json:"price_field,omitempty"`
	TimePrecision        string     `json:"time_precision,omitempty"`
	AlphaDefinition      string     `json:"alpha_definition,omitempty"`
	EntryAt              *time.Time `json:"entry_at,omitempty"`
	ExitAt               *time.Time `json:"exit_at,omitempty"`
	LabelAvailableAt     *time.Time `json:"label_available_at,omitempty"`
	EntryPrice           *float64   `json:"entry_price,omitempty"`
	ExitPrice            *float64   `json:"exit_price,omitempty"`
	RawReturn            *float64   `json:"raw_return,omitempty"`
	BenchmarkReturn      *float64   `json:"benchmark_return,omitempty"`
	ExcessReturn         *float64   `json:"excess_return,omitempty"`
	RiskAdjustedResidual *float64   `json:"risk_adjusted_residual,omitempty"`
	GrossStrategyReturn  *float64   `json:"gross_strategy_return,omitempty"`
	NetReturn            *float64   `json:"net_return,omitempty"`
	ExecutionCostBPS     *float64   `json:"execution_cost_bps,omitempty"`
	AbsoluteLabel        string     `json:"absolute_label,omitempty"`
	RelativeLabel        string     `json:"relative_label,omitempty"`
	ObjectiveLabel       string     `json:"objective_label,omitempty"`
	SimulationStatus     string     `json:"simulation_status"`
	ResearchResultOnly   bool       `json:"research_result_only"`
	RiskAdjustmentStatus string     `json:"risk_adjustment_status"`
}

func ResolveHorizonPolicy(objective string, horizonSessions int) (HorizonPolicy, error) {
	objective = strings.ToLower(strings.TrimSpace(objective))
	if objective != "absolute_up" && objective != "excess_up" {
		return HorizonPolicy{}, fmt.Errorf("objective must be absolute_up or excess_up")
	}
	bands := map[int]float64{1: .005, 5: .01, 20: .02}
	band, ok := bands[horizonSessions]
	if !ok {
		return HorizonPolicy{}, fmt.Errorf("horizon_sessions must be one of 1, 5 or 20")
	}
	return HorizonPolicy{Version: OutcomeLabelDefinitionVersion, Objective: objective, HorizonSessions: horizonSessions, NeutralBand: band,
		EntryPolicy: "first_observable_session_after_signal", ExitPolicy: "nth_observable_session_after_entry", PriceField: "adjusted_close",
		AlphaDefinition: "arithmetic_asset_total_return_minus_benchmark_total_return"}, nil
}

func ValidateHorizonPolicy(policy HorizonPolicy) error {
	frozen, err := ResolveHorizonPolicy(policy.Objective, policy.HorizonSessions)
	if err != nil {
		return err
	}
	if policy.Version != frozen.Version || policy.NeutralBand != frozen.NeutralBand || policy.EntryPolicy != frozen.EntryPolicy || policy.ExitPolicy != frozen.ExitPolicy || policy.PriceField != frozen.PriceField || policy.AlphaDefinition != frozen.AlphaDefinition {
		return fmt.Errorf("outcome label policy differs from frozen definition %s", OutcomeLabelDefinitionVersion)
	}
	if !policy.ExecutionEnabled {
		return nil
	}
	policy.ExecutionSide = strings.ToLower(strings.TrimSpace(policy.ExecutionSide))
	if policy.ExecutionSide != "long" && policy.ExecutionSide != "short" {
		return fmt.Errorf("enabled execution simulation requires side long or short")
	}
	for _, value := range []float64{policy.ExecutionCostBPS, policy.SlippageBPS, policy.BorrowBPS, policy.FundingBPS} {
		if value < 0 || value > 10000 || math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("execution costs must be finite and between 0 and 10000 bps")
		}
	}
	if policy.ExecutionSide != "short" && policy.BorrowBPS != 0 {
		return fmt.Errorf("borrow_bps only applies to short execution simulations")
	}
	return nil
}

func BuildOutcomeLabel(signalAvailableAt time.Time, asset, benchmark []PricePoint, policy HorizonPolicy) OutcomeLabel {
	result := OutcomeLabel{Status: "unavailable", DefinitionVersion: policy.Version, Objective: policy.Objective, HorizonSessions: policy.HorizonSessions,
		PriceField: policy.PriceField, TimePrecision: "daily_close", AlphaDefinition: policy.AlphaDefinition,
		SimulationStatus: "not_configured", ResearchResultOnly: true, RiskAdjustmentStatus: "not_configured", RelativeLabel: "unavailable"}
	if signalAvailableAt.IsZero() || ValidateHorizonPolicy(policy) != nil {
		result.Reason = "invalid_label_policy"
		return result
	}
	asset = eligibleSessions(asset, signalAvailableAt)
	if len(asset) <= policy.HorizonSessions {
		result.Reason = "label_not_mature"
		return result
	}
	entry, exit := asset[0], asset[policy.HorizonSessions]
	entryPrice, okEntry := selectedPrice(entry, policy.PriceField)
	exitPrice, okExit := selectedPrice(exit, policy.PriceField)
	if !okEntry || !okExit || entryPrice <= 0 {
		result.Reason = "adjusted_close_missing"
		return result
	}
	raw := exitPrice/entryPrice - 1
	labelAvailableAt := exit.AvailableAt
	if entry.AvailableAt.After(labelAvailableAt) {
		labelAvailableAt = entry.AvailableAt
	}
	result.EntryAt, result.ExitAt, result.LabelAvailableAt = timePointer(entry.SessionDate), timePointer(exit.SessionDate), timePointer(labelAvailableAt)
	result.EntryPrice, result.ExitPrice, result.RawReturn = &entryPrice, &exitPrice, &raw
	result.AbsoluteLabel = classifyOutcome(raw, policy.NeutralBand, "up", "down")
	if len(benchmark) > 0 {
		benchmark = eligibleSessions(benchmark, signalAvailableAt)
		leftPoint, leftOK := priceOnSession(benchmark, entry.SessionDate)
		rightPoint, rightOK := priceOnSession(benchmark, exit.SessionDate)
		if leftOK && rightOK && strings.EqualFold(entry.Currency, leftPoint.Currency) && strings.EqualFold(entry.Currency, rightPoint.Currency) {
			left, okLeft := selectedPrice(leftPoint, policy.PriceField)
			right, okRight := selectedPrice(rightPoint, policy.PriceField)
			if okLeft && okRight && left > 0 {
				value := right/left - 1
				excess := raw - value
				result.BenchmarkReturn, result.ExcessReturn = &value, &excess
				result.RelativeLabel = classifyOutcome(excess, policy.NeutralBand, "outperform", "underperform")
				if leftPoint.AvailableAt.After(*result.LabelAvailableAt) {
					result.LabelAvailableAt = timePointer(leftPoint.AvailableAt)
				}
				if rightPoint.AvailableAt.After(*result.LabelAvailableAt) {
					result.LabelAvailableAt = timePointer(rightPoint.AvailableAt)
				}
			}
		}
	}
	if policy.Objective == "excess_up" && result.ExcessReturn == nil {
		result.Reason = "benchmark_unavailable_or_session_mismatch"
		return result
	}
	result.ObjectiveLabel = result.AbsoluteLabel
	if policy.Objective == "excess_up" {
		result.ObjectiveLabel = result.RelativeLabel
	}
	if policy.ExecutionEnabled {
		gross := raw
		if strings.EqualFold(policy.ExecutionSide, "short") {
			gross = -raw
		}
		cost := policy.ExecutionCostBPS + policy.SlippageBPS + policy.BorrowBPS + policy.FundingBPS
		net := gross - cost/10000
		result.GrossStrategyReturn, result.NetReturn, result.ExecutionCostBPS = &gross, &net, &cost
		result.SimulationStatus, result.ResearchResultOnly = "simulated_with_pre_registered_assumptions", false
	}
	result.Status = "mature"
	return result
}

func eligibleSessions(values []PricePoint, signal time.Time) []PricePoint {
	result := []PricePoint{}
	for _, value := range values {
		if value.SessionDate.IsZero() || value.AvailableAt.IsZero() || !value.SessionDate.After(signal) {
			continue
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SessionDate.Before(result[j].SessionDate) })
	return result
}

func priceOnSession(values []PricePoint, session time.Time) (PricePoint, bool) {
	date := session.UTC().Format("2006-01-02")
	for _, value := range values {
		if value.SessionDate.UTC().Format("2006-01-02") == date {
			return value, true
		}
	}
	return PricePoint{}, false
}

func classifyOutcome(value, band float64, positive, negative string) string {
	if value > band {
		return positive
	}
	if value < -band {
		return negative
	}
	return "neutral"
}

func selectedPrice(point PricePoint, field string) (float64, bool) {
	if field == "adjusted_close" {
		if point.AdjustedClose == nil || !point.CorporateActionAdjusted {
			return 0, false
		}
		return *point.AdjustedClose, true
	}
	if field != "close" || point.Close == nil {
		return 0, false
	}
	return *point.Close, true
}

type Record struct {
	ID            string    `json:"id"`
	EventCluster  string    `json:"event_cluster"`
	SignalAt      time.Time `json:"signal_at"`
	LabelMatureAt time.Time `json:"label_mature_at"`
}
type Fold struct {
	Index             int       `json:"index"`
	Train             []string  `json:"train"`
	Calibration       []string  `json:"calibration"`
	Test              []string  `json:"test"`
	TrainCutoff       time.Time `json:"train_cutoff"`
	CalibrationCutoff time.Time `json:"calibration_cutoff"`
	TestCutoff        time.Time `json:"test_cutoff"`
}

func WalkForward(records []Record, trainWindow, calibrationWindow, testWindow, step, embargo time.Duration) ([]Fold, error) {
	if trainWindow <= 0 || calibrationWindow <= 0 || testWindow <= 0 || step <= 0 || embargo < 0 {
		return nil, fmt.Errorf("positive rolling windows and non-negative embargo are required")
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("records are required")
	}
	ordered := append([]Record{}, records...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].SignalAt.Before(ordered[j].SignalAt) })
	start, end := ordered[0].SignalAt.UTC(), ordered[len(ordered)-1].SignalAt.UTC()
	folds := []Fold{}
	for trainEnd := start.Add(trainWindow); ; trainEnd = trainEnd.Add(step) {
		calibrationStart, calibrationEnd := trainEnd.Add(embargo), trainEnd.Add(embargo).Add(calibrationWindow)
		testStart, testEnd := calibrationEnd.Add(embargo), calibrationEnd.Add(embargo).Add(testWindow)
		if testEnd.After(end.Add(time.Nanosecond)) {
			break
		}
		fold := Fold{Index: len(folds), Train: []string{}, Calibration: []string{}, Test: []string{}, TrainCutoff: trainEnd, CalibrationCutoff: calibrationEnd, TestCutoff: testEnd}
		clusters := map[string]string{}
		for _, record := range ordered {
			partition := ""
			switch {
			case !record.SignalAt.Before(start) && record.SignalAt.Before(trainEnd) && !record.LabelMatureAt.After(trainEnd):
				partition = "train"
			case !record.SignalAt.Before(calibrationStart) && record.SignalAt.Before(calibrationEnd) && !record.LabelMatureAt.After(calibrationEnd):
				partition = "calibration"
			case !record.SignalAt.Before(testStart) && record.SignalAt.Before(testEnd) && !record.LabelMatureAt.After(testEnd):
				partition = "test"
			}
			if partition == "" {
				continue
			}
			cluster := strings.TrimSpace(record.EventCluster)
			if cluster == "" {
				cluster = "id:" + record.ID
			}
			if prior, exists := clusters[cluster]; exists && prior != partition {
				continue
			}
			clusters[cluster] = partition
			switch partition {
			case "train":
				fold.Train = append(fold.Train, record.ID)
			case "calibration":
				fold.Calibration = append(fold.Calibration, record.ID)
			case "test":
				fold.Test = append(fold.Test, record.ID)
			}
		}
		if len(fold.Train) > 0 && len(fold.Calibration) > 0 && len(fold.Test) > 0 {
			folds = append(folds, fold)
		}
	}
	if len(folds) == 0 {
		return nil, fmt.Errorf("no complete walk-forward folds")
	}
	return folds, nil
}

type PredictionResult struct {
	ID          string        `json:"id"`
	AssetClass  string        `json:"asset_class"`
	Market      string        `json:"market"`
	Status      string        `json:"status"`
	Score       *float64      `json:"score,omitempty"`
	Probability *float64      `json:"probability,omitempty"`
	Outcome     *OutcomeLabel `json:"outcome,omitempty"`
}

// ReportBySegment prevents a strong result in one asset class or market from
// masking weak or uncalibrated behavior in another. It never pools segments.
func ReportBySegment(results []PredictionResult) map[string]LayeredReport {
	grouped := map[string][]PredictionResult{}
	for _, item := range results {
		assetClass := strings.ToLower(strings.TrimSpace(item.AssetClass))
		market := strings.ToUpper(strings.TrimSpace(item.Market))
		if assetClass == "" {
			assetClass = "unknown"
		}
		if market == "" {
			market = "UNKNOWN"
		}
		key := assetClass + ":" + market
		grouped[key] = append(grouped[key], item)
	}
	reports := map[string]LayeredReport{}
	for key, values := range grouped {
		reports[key] = Report(values)
	}
	return reports
}

type LayeredReport struct {
	Total             int      `json:"total"`
	Mature            int      `json:"mature"`
	Predicted         int      `json:"predicted"`
	Rejected          int      `json:"rejected"`
	TechnicalFailures int      `json:"technical_failures"`
	Coverage          float64  `json:"coverage"`
	MeanRawReturn     *float64 `json:"mean_raw_return,omitempty"`
	MeanExcessReturn  *float64 `json:"mean_excess_return,omitempty"`
	RankIC            *float64 `json:"rank_ic,omitempty"`
	ProbabilityStatus string   `json:"probability_status"`
}

func Report(results []PredictionResult) LayeredReport {
	report := LayeredReport{Total: len(results), ProbabilityStatus: "unavailable"}
	scores, returns := []float64{}, []float64{}
	raw, excess := []float64{}, []float64{}
	calibrated := true
	for _, item := range results {
		if item.Status == "rejected" {
			report.Rejected++
		}
		if item.Status == "technical_failure" {
			report.TechnicalFailures++
		}
		if item.Outcome == nil || item.Outcome.Status != "mature" {
			continue
		}
		report.Mature++
		if item.Score == nil {
			continue
		}
		report.Predicted++
		if item.Outcome.RawReturn != nil {
			raw = append(raw, *item.Outcome.RawReturn)
			scores = append(scores, *item.Score)
			returns = append(returns, *item.Outcome.RawReturn)
		}
		if item.Outcome.ExcessReturn != nil {
			excess = append(excess, *item.Outcome.ExcessReturn)
		}
		calibrated = calibrated && item.Probability != nil
	}
	if report.Mature > 0 {
		report.Coverage = float64(report.Predicted) / float64(report.Mature)
	}
	report.MeanRawReturn = meanPointer(raw)
	report.MeanExcessReturn = meanPointer(excess)
	report.RankIC = correlationPointer(scores, returns)
	if report.Predicted > 0 && calibrated {
		report.ProbabilityStatus = "calibrated"
	}
	return report
}

func meanPointer(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	total := 0.0
	for _, v := range values {
		total += v
	}
	result := total / float64(len(values))
	return &result
}
func correlationPointer(left, right []float64) *float64 {
	if len(left) < 2 || len(left) != len(right) {
		return nil
	}
	lm, rm := *meanPointer(left), *meanPointer(right)
	num, ld, rd := 0.0, 0.0, 0.0
	for i := range left {
		a, b := left[i]-lm, right[i]-rm
		num += a * b
		ld += a * a
		rd += b * b
	}
	if ld == 0 || rd == 0 {
		return nil
	}
	v := num / math.Sqrt(ld*rd)
	return &v
}
func timePointer(value time.Time) *time.Time { copy := value.UTC(); return &copy }
