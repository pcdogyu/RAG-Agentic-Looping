// Package signals owns reproducible short-horizon feature snapshots and
// interpretable baselines. It never treats an event direction score as a
// calibrated probability and never learns preprocessing from future samples.
package signals

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

type Feature struct {
	Name        string    `json:"name"`
	Value       *float64  `json:"value"`
	AvailableAt time.Time `json:"available_at"`
	SourceIDs   []string  `json:"source_ids"`
}

const (
	FeatureConsensusSurprise = "consensus_surprise_z"
	FeatureNewInformation    = "new_information_score"
	FeatureBusinessExposure  = "business_exposure_share"
	FeaturePriceReaction     = "pre_signal_price_reaction"
)

// CoreFeatureNames returns the stable P1 feature contract. Values still need
// point-in-time availability and source IDs; absent inputs remain missing.
func CoreFeatureNames() []string {
	return []string{FeatureBusinessExposure, FeatureConsensusSurprise, FeatureNewInformation, FeaturePriceReaction}
}

type Snapshot struct {
	AsOf          time.Time           `json:"as_of"`
	Values        map[string]*float64 `json:"values"`
	SourceIDs     map[string][]string `json:"source_ids"`
	Missing       []string            `json:"missing"`
	FutureBlocked []string            `json:"future_blocked"`
}

func BuildSnapshot(asOf time.Time, required []string, features []Feature) Snapshot {
	result := Snapshot{AsOf: asOf.UTC(), Values: map[string]*float64{}, SourceIDs: map[string][]string{}, Missing: []string{}, FutureBlocked: []string{}}
	for _, feature := range features {
		name := strings.TrimSpace(feature.Name)
		if name == "" {
			continue
		}
		if feature.AvailableAt.IsZero() || feature.AvailableAt.After(result.AsOf) {
			result.FutureBlocked = append(result.FutureBlocked, name)
			continue
		}
		if feature.Value == nil || math.IsNaN(*feature.Value) || math.IsInf(*feature.Value, 0) {
			continue
		}
		value := *feature.Value
		result.Values[name] = &value
		result.SourceIDs[name] = clean(feature.SourceIDs)
	}
	for _, name := range clean(required) {
		if result.Values[name] == nil {
			result.Missing = append(result.Missing, name)
		}
	}
	sort.Strings(result.Missing)
	sort.Strings(result.FutureBlocked)
	return result
}

type Sample struct {
	ID            string              `json:"id"`
	EventCluster  string              `json:"event_cluster"`
	SignalAt      time.Time           `json:"signal_at"`
	LabelMatureAt time.Time           `json:"label_mature_at"`
	Values        map[string]*float64 `json:"values"`
	Label         bool                `json:"label"`
}

type BinaryModel struct {
	Version         string             `json:"version"`
	Objective       string             `json:"objective"`
	HorizonSessions int                `json:"horizon_sessions"`
	TrainingCutoff  time.Time          `json:"training_cutoff"`
	FeatureNames    []string           `json:"feature_names"`
	Means           map[string]float64 `json:"means"`
	Scales          map[string]float64 `json:"scales"`
	Coefficients    map[string]float64 `json:"coefficients"`
	Intercept       float64            `json:"intercept"`
	SampleCount     int                `json:"sample_count"`
}

type FitOptions struct {
	Objective       string
	HorizonSessions int
	TrainingCutoff  time.Time
	Iterations      int
	LearningRate    float64
	L2              float64
}

func FitBinary(samples []Sample, names []string, options FitOptions) (BinaryModel, error) {
	names = clean(names)
	if len(names) == 0 || strings.TrimSpace(options.Objective) == "" || options.HorizonSessions < 1 || options.TrainingCutoff.IsZero() {
		return BinaryModel{}, fmt.Errorf("features, horizon and training cutoff are required")
	}
	eligible := []Sample{}
	clusters := map[string]bool{}
	for _, sample := range samples {
		if sample.SignalAt.IsZero() || sample.LabelMatureAt.IsZero() || sample.SignalAt.After(sample.LabelMatureAt) || sample.LabelMatureAt.After(options.TrainingCutoff) {
			continue
		}
		complete := true
		for _, name := range names {
			if sample.Values[name] == nil {
				complete = false
				break
			}
		}
		if complete {
			cluster := strings.TrimSpace(sample.EventCluster)
			if cluster == "" {
				cluster = "id:" + strings.TrimSpace(sample.ID)
			}
			if cluster == "id:" || clusters[cluster] {
				return BinaryModel{}, fmt.Errorf("training samples require unique ids or event clusters")
			}
			clusters[cluster] = true
			eligible = append(eligible, sample)
		}
	}
	if len(eligible) < max(8, len(names)*3) {
		return BinaryModel{}, fmt.Errorf("insufficient mature complete training samples")
	}
	sort.Slice(eligible, func(i, j int) bool {
		if !eligible[i].SignalAt.Equal(eligible[j].SignalAt) {
			return eligible[i].SignalAt.Before(eligible[j].SignalAt)
		}
		return eligible[i].ID < eligible[j].ID
	})
	means, scales := map[string]float64{}, map[string]float64{}
	for _, name := range names {
		for _, sample := range eligible {
			means[name] += *sample.Values[name]
		}
		means[name] /= float64(len(eligible))
		for _, sample := range eligible {
			delta := *sample.Values[name] - means[name]
			scales[name] += delta * delta
		}
		scales[name] = math.Sqrt(scales[name] / float64(len(eligible)))
		if scales[name] < 1e-12 {
			scales[name] = 1
		}
	}
	iterations := options.Iterations
	if iterations < 1 {
		iterations = 600
	}
	rate := options.LearningRate
	if rate <= 0 || rate > 1 {
		rate = .05
	}
	coefficients := map[string]float64{}
	intercept := 0.0
	for iteration := 0; iteration < iterations; iteration++ {
		gradient := map[string]float64{}
		interceptGradient := 0.0
		for _, sample := range eligible {
			logit := intercept
			for _, name := range names {
				logit += coefficients[name] * ((*sample.Values[name] - means[name]) / scales[name])
			}
			prediction := sigmoid(logit)
			actual := 0.0
			if sample.Label {
				actual = 1
			}
			errorValue := prediction - actual
			interceptGradient += errorValue
			for _, name := range names {
				gradient[name] += errorValue * ((*sample.Values[name] - means[name]) / scales[name])
			}
		}
		intercept -= rate * interceptGradient / float64(len(eligible))
		for _, name := range names {
			coefficients[name] -= rate * (gradient[name]/float64(len(eligible)) + options.L2*coefficients[name])
		}
	}
	model := BinaryModel{Objective: strings.TrimSpace(options.Objective), HorizonSessions: options.HorizonSessions, TrainingCutoff: options.TrainingCutoff.UTC(), FeatureNames: names, Means: means, Scales: scales, Coefficients: coefficients, Intercept: intercept, SampleCount: len(eligible)}
	identity, _ := json.Marshal(struct {
		Model        BinaryModel `json:"model"`
		Samples      []Sample    `json:"samples"`
		Iterations   int         `json:"iterations"`
		LearningRate float64     `json:"learning_rate"`
		L2           float64     `json:"l2"`
	}{Model: model, Samples: eligible, Iterations: iterations, LearningRate: rate, L2: options.L2})
	sum := sha256.Sum256(identity)
	model.Version = "interpretable-logistic-" + hex.EncodeToString(sum[:])[:16]
	return model, nil
}

type Prediction struct {
	Status            string             `json:"status"`
	Reason            string             `json:"reason,omitempty"`
	ModelVersion      string             `json:"model_version"`
	RawScore          *float64           `json:"raw_score,omitempty"`
	Probability       *float64           `json:"probability,omitempty"`
	CalibrationStatus string             `json:"calibration_status"`
	Contributions     map[string]float64 `json:"contributions"`
}

func (model BinaryModel) Predict(snapshot Snapshot) Prediction {
	result := Prediction{Status: "unavailable", ModelVersion: model.Version, CalibrationStatus: "uncalibrated", Contributions: map[string]float64{}}
	if snapshot.AsOf.Before(model.TrainingCutoff) {
		result.Reason = "prediction_as_of_precedes_training_cutoff"
		return result
	}
	logit := model.Intercept
	for _, name := range model.FeatureNames {
		value := snapshot.Values[name]
		if value == nil {
			result.Reason = "missing_feature:" + name
			return result
		}
		contribution := model.Coefficients[name] * ((*value - model.Means[name]) / model.Scales[name])
		result.Contributions[name] = contribution
		logit += contribution
	}
	result.Status, result.RawScore = "uncalibrated", &logit
	return result
}

func sigmoid(value float64) float64 {
	if value >= 0 {
		z := math.Exp(-value)
		return 1 / (1 + z)
	}
	z := math.Exp(value)
	return z / (1 + z)
}
func clean(values []string) []string {
	seen := map[string]bool{}
	result := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
