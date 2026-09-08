// Package calibration fits calibrators only on a dedicated sample and refuses
// predictions outside their registered market and horizon scope.
package calibration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type Observation struct {
	SampleID     string    `json:"sample_id"`
	EventCluster string    `json:"event_cluster,omitempty"`
	Score        float64   `json:"score"`
	Label        bool      `json:"label"`
	ObservedAt   time.Time `json:"observed_at"`
}

type Scope struct {
	Market          string   `json:"market"`
	HorizonSessions int      `json:"horizon_sessions"`
	EventTypes      []string `json:"event_types"`
}

type Model struct {
	Version            string     `json:"version"`
	SourceModelVersion string     `json:"source_model_version"`
	Method             string     `json:"method"`
	Intercept          float64    `json:"intercept"`
	Slope              float64    `json:"slope"`
	TrainedFrom        time.Time  `json:"trained_from"`
	TrainedUntil       time.Time  `json:"trained_until"`
	SampleCount        int        `json:"sample_count"`
	Scope              Scope      `json:"scope"`
	ValidUntil         *time.Time `json:"valid_until,omitempty"`
}

func FitPlatt(sourceVersion string, observations []Observation, scope Scope) (Model, error) {
	if strings.TrimSpace(sourceVersion) == "" || strings.TrimSpace(scope.Market) == "" || scope.HorizonSessions < 1 || len(observations) < 30 {
		return Model{}, fmt.Errorf("source model, scope and at least 30 independent calibration observations are required")
	}
	ordered := append([]Observation{}, observations...)
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].ObservedAt.Equal(ordered[j].ObservedAt) {
			return ordered[i].ObservedAt.Before(ordered[j].ObservedAt)
		}
		left := strings.TrimSpace(ordered[i].EventCluster) + "|" + strings.TrimSpace(ordered[i].SampleID)
		right := strings.TrimSpace(ordered[j].EventCluster) + "|" + strings.TrimSpace(ordered[j].SampleID)
		return left < right
	})
	positive := 0
	independent := map[string]bool{}
	for _, item := range ordered {
		if item.ObservedAt.IsZero() || math.IsNaN(item.Score) || math.IsInf(item.Score, 0) {
			return Model{}, fmt.Errorf("calibration observations require finite scores and observed_at")
		}
		key := strings.TrimSpace(item.EventCluster)
		if key == "" {
			key = strings.TrimSpace(item.SampleID)
		}
		if key == "" || independent[key] {
			return Model{}, fmt.Errorf("calibration observations require unique sample_id or event_cluster")
		}
		independent[key] = true
		if item.Label {
			positive++
		}
	}
	if positive == 0 || positive == len(ordered) {
		return Model{}, fmt.Errorf("calibration sample requires both outcomes")
	}
	intercept := math.Log(float64(positive) / float64(len(ordered)-positive))
	slope := 1.0
	for iteration := 0; iteration < 1200; iteration++ {
		gradientIntercept, gradientSlope := 0.0, 0.0
		for _, item := range ordered {
			predicted := sigmoid(intercept + slope*item.Score)
			actual := 0.0
			if item.Label {
				actual = 1
			}
			delta := predicted - actual
			gradientIntercept += delta
			gradientSlope += delta * item.Score
		}
		rate := .03 / math.Sqrt(float64(iteration)+1)
		intercept -= rate * gradientIntercept / float64(len(ordered))
		slope -= rate * gradientSlope / float64(len(ordered))
	}
	model := Model{SourceModelVersion: strings.TrimSpace(sourceVersion), Method: "platt", Intercept: intercept, Slope: slope, TrainedFrom: ordered[0].ObservedAt.UTC(), TrainedUntil: ordered[len(ordered)-1].ObservedAt.UTC(), SampleCount: len(independent), Scope: normalizeScope(scope)}
	identity, _ := json.Marshal(struct {
		Model        Model         `json:"model"`
		Observations []Observation `json:"observations"`
	}{Model: model, Observations: ordered})
	sum := sha256.Sum256(identity)
	model.Version = "platt-" + hex.EncodeToString(sum[:])[:20]
	return model, nil
}

type Result struct {
	Status             string   `json:"status"`
	Reason             string   `json:"reason,omitempty"`
	Probability        *float64 `json:"probability,omitempty"`
	CalibrationVersion string   `json:"calibration_version,omitempty"`
}

func (model Model) Apply(sourceVersion string, score float64, market string, horizon int, eventType string, asOf time.Time) Result {
	result := Result{Status: "unavailable"}
	if sourceVersion != model.SourceModelVersion {
		result.Reason = "source_model_version_mismatch"
		return result
	}
	if !strings.EqualFold(strings.TrimSpace(market), model.Scope.Market) || horizon != model.Scope.HorizonSessions {
		result.Reason = "outside_calibration_scope"
		return result
	}
	if len(model.Scope.EventTypes) > 0 && !containsFold(model.Scope.EventTypes, eventType) {
		result.Reason = "outside_event_type_scope"
		return result
	}
	if asOf.IsZero() || asOf.Before(model.TrainedUntil) {
		result.Reason = "invalid_prediction_time"
		return result
	}
	if model.ValidUntil != nil && asOf.After(*model.ValidUntil) {
		result.Reason = "calibration_expired"
		return result
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		result.Reason = "invalid_score"
		return result
	}
	probability := math.Max(1e-6, math.Min(1-1e-6, sigmoid(model.Intercept+model.Slope*score)))
	result.Status, result.Probability, result.CalibrationVersion = "calibrated", &probability, model.Version
	return result
}

type Metrics struct {
	SampleCount int     `json:"sample_count"`
	Brier       float64 `json:"brier_score"`
	LogLoss     float64 `json:"log_loss"`
	ECE         float64 `json:"expected_calibration_error"`
	Bins        []Bin   `json:"bins"`
}
type Bin struct {
	Lower           float64 `json:"lower"`
	Upper           float64 `json:"upper"`
	Count           int     `json:"count"`
	MeanProbability float64 `json:"mean_probability"`
	Frequency       float64 `json:"frequency"`
}

func Evaluate(probabilities []float64, labels []bool, binCount int) (Metrics, error) {
	if len(probabilities) == 0 || len(probabilities) != len(labels) {
		return Metrics{}, fmt.Errorf("probabilities and labels must be non-empty and aligned")
	}
	if binCount < 2 {
		binCount = 10
	}
	metrics := Metrics{SampleCount: len(labels), Bins: make([]Bin, binCount)}
	positive := make([]int, binCount)
	sums := make([]float64, binCount)
	for index, probability := range probabilities {
		if probability <= 0 || probability >= 1 || math.IsNaN(probability) {
			return Metrics{}, fmt.Errorf("calibrated probabilities must be strictly between zero and one")
		}
		y := 0.0
		if labels[index] {
			y = 1
		}
		metrics.Brier += math.Pow(probability-y, 2)
		metrics.LogLoss -= y*math.Log(probability) + (1-y)*math.Log(1-probability)
		bucket := min(binCount-1, int(probability*float64(binCount)))
		metrics.Bins[bucket].Count++
		sums[bucket] += probability
		if labels[index] {
			positive[bucket]++
		}
	}
	metrics.Brier /= float64(len(labels))
	metrics.LogLoss /= float64(len(labels))
	for index := range metrics.Bins {
		bin := &metrics.Bins[index]
		bin.Lower = float64(index) / float64(binCount)
		bin.Upper = float64(index+1) / float64(binCount)
		if bin.Count > 0 {
			bin.MeanProbability = sums[index] / float64(bin.Count)
			bin.Frequency = float64(positive[index]) / float64(bin.Count)
			metrics.ECE += float64(bin.Count) / float64(len(labels)) * math.Abs(bin.Frequency-bin.MeanProbability)
		}
	}
	return metrics, nil
}

func normalizeScope(scope Scope) Scope {
	scope.Market = strings.ToUpper(strings.TrimSpace(scope.Market))
	values := []string{}
	seen := map[string]bool{}
	for _, value := range scope.EventTypes {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			values = append(values, value)
		}
	}
	sort.Strings(values)
	scope.EventTypes = values
	return scope
}
func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}
func sigmoid(value float64) float64 {
	if value >= 0 {
		z := math.Exp(-value)
		return 1 / (1 + z)
	}
	z := math.Exp(value)
	return z / (1 + z)
}
