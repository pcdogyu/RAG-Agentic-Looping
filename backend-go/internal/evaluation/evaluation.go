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

type HorizonPolicy struct {
	HorizonSessions  int     `json:"horizon_sessions"`
	NeutralBand      float64 `json:"neutral_band"`
	ExecutionCostBPS float64 `json:"execution_cost_bps"`
	EntryPolicy      string  `json:"entry_policy"`
	PriceField       string  `json:"price_field"`
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
	Status          string     `json:"status"`
	Reason          string     `json:"reason,omitempty"`
	HorizonSessions int        `json:"horizon_sessions"`
	EntryAt         *time.Time `json:"entry_at,omitempty"`
	ExitAt          *time.Time `json:"exit_at,omitempty"`
	RawReturn       *float64   `json:"raw_return,omitempty"`
	BenchmarkReturn *float64   `json:"benchmark_return,omitempty"`
	ExcessReturn    *float64   `json:"excess_return,omitempty"`
	NetReturn       *float64   `json:"net_return,omitempty"`
	Direction       string     `json:"direction,omitempty"`
}

func BuildOutcomeLabel(signalAvailableAt time.Time, asset, benchmark []PricePoint, policy HorizonPolicy) OutcomeLabel {
	result := OutcomeLabel{Status: "unavailable", HorizonSessions: policy.HorizonSessions}
	if signalAvailableAt.IsZero() || policy.HorizonSessions < 1 || policy.EntryPolicy != "first_session_after_signal" {
		result.Reason = "invalid_label_policy"
		return result
	}
	asset = eligiblePrices(asset, signalAvailableAt, policy.PriceField)
	if len(asset) <= policy.HorizonSessions {
		result.Reason = "label_not_mature"
		return result
	}
	entry, exit := asset[0], asset[policy.HorizonSessions]
	entryPrice, okEntry := selectedPrice(entry, policy.PriceField)
	exitPrice, okExit := selectedPrice(exit, policy.PriceField)
	if !okEntry || !okExit || entryPrice <= 0 {
		result.Reason = "missing_or_invalid_asset_price"
		return result
	}
	raw := exitPrice/entryPrice - 1
	result.EntryAt, result.ExitAt, result.RawReturn = timePointer(entry.AvailableAt), timePointer(exit.AvailableAt), &raw
	if len(benchmark) > 0 {
		benchmark = eligiblePrices(benchmark, signalAvailableAt, policy.PriceField)
		if len(benchmark) <= policy.HorizonSessions || !strings.EqualFold(entry.Currency, benchmark[0].Currency) {
			result.Reason = "benchmark_unavailable_or_currency_mismatch"
			return result
		}
		left, okLeft := selectedPrice(benchmark[0], policy.PriceField)
		right, okRight := selectedPrice(benchmark[policy.HorizonSessions], policy.PriceField)
		if !okLeft || !okRight || left <= 0 {
			result.Reason = "benchmark_unavailable_or_currency_mismatch"
			return result
		}
		value := right/left - 1
		excess := raw - value
		result.BenchmarkReturn, result.ExcessReturn = &value, &excess
	}
	net := raw - policy.ExecutionCostBPS/10000
	result.NetReturn = &net
	result.Status = "mature"
	metric := raw
	if result.ExcessReturn != nil {
		metric = *result.ExcessReturn
	}
	if metric > policy.NeutralBand {
		result.Direction = "up"
	} else if metric < -policy.NeutralBand {
		result.Direction = "down"
	} else {
		result.Direction = "neutral"
	}
	return result
}

func eligiblePrices(values []PricePoint, signal time.Time, field string) []PricePoint {
	result := []PricePoint{}
	for _, value := range values {
		if value.AvailableAt.IsZero() || !value.AvailableAt.After(signal) {
			continue
		}
		if _, ok := selectedPrice(value, field); !ok {
			continue
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SessionDate.Before(result[j].SessionDate) })
	return result
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
	Status      string        `json:"status"`
	Score       *float64      `json:"score,omitempty"`
	Probability *float64      `json:"probability,omitempty"`
	Outcome     *OutcomeLabel `json:"outcome,omitempty"`
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
