package evaluation

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/calibration"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

const (
	DevelopmentExperimentContractVersion = "baseline-calibration-experiment-v1"
	FeatureLLMDirection                  = "llm_direction_score"
)

type ExperimentSample struct {
	ID            string              `json:"id"`
	EventCluster  string              `json:"event_cluster"`
	Partition     string              `json:"partition"`
	SignalAt      time.Time           `json:"signal_at"`
	LabelMatureAt time.Time           `json:"label_mature_at"`
	Values        map[string]*float64 `json:"values"`
	Label         bool                `json:"label"`
	RawReturn     *float64            `json:"raw_return,omitempty"`
	ExcessReturn  *float64            `json:"excess_return,omitempty"`
}

type VariantMetrics struct {
	SampleCount       int                  `json:"sample_count"`
	Correct           int                  `json:"correct"`
	Accuracy          *float64             `json:"accuracy,omitempty"`
	PositiveRate      *float64             `json:"positive_rate,omitempty"`
	PredictedPositive *float64             `json:"predicted_positive_rate,omitempty"`
	RankIC            *float64             `json:"rank_ic,omitempty"`
	Probability       *calibration.Metrics `json:"probability,omitempty"`
}

type VariantPrediction struct {
	PredictionRunID string    `json:"prediction_run_id"`
	EventCluster    string    `json:"event_cluster"`
	SignalAt        time.Time `json:"signal_at"`
	RawScore        float64   `json:"raw_score"`
	Probability     *float64  `json:"probability,omitempty"`
	Label           bool      `json:"label"`
	Correct         bool      `json:"correct"`
	RawReturn       *float64  `json:"raw_return,omitempty"`
	ExcessReturn    *float64  `json:"excess_return,omitempty"`
}

type ExperimentVariant struct {
	Name             string               `json:"name"`
	Kind             string               `json:"kind"`
	Status           string               `json:"status"`
	Reason           string               `json:"reason,omitempty"`
	FeatureNames     []string             `json:"feature_names"`
	TrainingCount    int                  `json:"training_count"`
	CalibrationCount int                  `json:"calibration_count"`
	TestCount        int                  `json:"test_count"`
	Model            *signals.BinaryModel `json:"model,omitempty"`
	Calibrator       *calibration.Model   `json:"calibrator,omitempty"`
	Metrics          VariantMetrics       `json:"metrics"`
	Predictions      []VariantPrediction  `json:"predictions"`
	ArtifactDigest   string               `json:"artifact_digest"`
}

type ExperimentFold struct {
	Index    int                 `json:"index"`
	Variants []ExperimentVariant `json:"variants"`
}

type IncrementalComparison struct {
	FoldIndex      int      `json:"fold_index"`
	FullVariant    string   `json:"full_variant"`
	AblatedVariant string   `json:"ablated_variant"`
	FeatureGroup   string   `json:"feature_group"`
	Comparable     bool     `json:"comparable"`
	Reason         string   `json:"reason,omitempty"`
	AccuracyDelta  *float64 `json:"accuracy_delta,omitempty"`
	BrierDelta     *float64 `json:"brier_delta,omitempty"`
}

type DevelopmentExperiment struct {
	ID                     string                  `json:"id"`
	ContractVersion        string                  `json:"contract_version"`
	DatasetID              string                  `json:"dataset_id"`
	DatasetManifestDigest  string                  `json:"dataset_manifest_digest"`
	AssetClass             string                  `json:"asset_class"`
	Market                 string                  `json:"market"`
	Objective              string                  `json:"objective"`
	HorizonSessions        int                     `json:"horizon_sessions"`
	FeatureGroups          map[string][]string     `json:"feature_groups"`
	PreprocessingPolicy    string                  `json:"preprocessing_policy"`
	FinalHoldoutAccessed   bool                    `json:"final_holdout_accessed"`
	Folds                  []ExperimentFold        `json:"folds"`
	IncrementalComparisons []IncrementalComparison `json:"incremental_comparisons"`
	ArtifactDigest         string                  `json:"artifact_digest"`
}

func DefaultExperimentFeatureGroups() map[string][]string {
	return map[string][]string{
		"price":        {signals.FeaturePriceReaction},
		"fundamental":  {signals.FeatureBusinessExposure},
		"news":         {signals.FeatureNewInformation},
		"expectations": {signals.FeatureConsensusSurprise},
	}
}

func RunDevelopmentExperiment(dataset StoredDataset, samples map[int]map[string][]ExperimentSample) (DevelopmentExperiment, error) {
	if dataset.Manifest.ID == "" || dataset.Manifest.ManifestDigest == "" || len(dataset.Manifest.Folds) == 0 {
		return DevelopmentExperiment{}, fmt.Errorf("a persisted walk-forward dataset is required")
	}
	if dataset.AssetClass != "equity" {
		return DevelopmentExperiment{}, fmt.Errorf("development experiment asset class is unsupported")
	}
	if _, err := ResolveHorizonPolicy(dataset.Objective, dataset.HorizonSessions); err != nil {
		return DevelopmentExperiment{}, err
	}
	groups := DefaultExperimentFeatureGroups()
	result := DevelopmentExperiment{ContractVersion: DevelopmentExperimentContractVersion, DatasetID: dataset.Manifest.ID,
		DatasetManifestDigest: dataset.Manifest.ManifestDigest, AssetClass: dataset.AssetClass, Market: dataset.Market, Objective: dataset.Objective,
		HorizonSessions: dataset.HorizonSessions, FeatureGroups: groups, PreprocessingPolicy: "training_partition_only_zscore_v1",
		FinalHoldoutAccessed: false, Folds: []ExperimentFold{}, IncrementalComparisons: []IncrementalComparison{}}
	for _, fold := range dataset.Manifest.Folds {
		partitions := samples[fold.Index]
		for partition, values := range partitions {
			if partition != "train" && partition != "calibration" && partition != "test" && len(values) > 0 {
				return DevelopmentExperiment{}, fmt.Errorf("development experiment cannot access partition %s", partition)
			}
		}
		if len(partitions["train"]) == 0 || len(partitions["calibration"]) == 0 || len(partitions["test"]) == 0 {
			return DevelopmentExperiment{}, fmt.Errorf("fold %d experiment samples are incomplete", fold.Index)
		}
		if err := validateExperimentPartitions(partitions); err != nil {
			return DevelopmentExperiment{}, fmt.Errorf("fold %d: %w", fold.Index, err)
		}
		variants := runFoldVariants(dataset, fold, partitions, groups)
		result.Folds = append(result.Folds, ExperimentFold{Index: fold.Index, Variants: variants})
		result.IncrementalComparisons = append(result.IncrementalComparisons, compareAblations(fold.Index, variants)...)
	}
	result.ArtifactDigest = digestValue(struct {
		Contract, Dataset, DatasetDigest string
		Groups                           map[string][]string
		Policy                           string
		Folds                            []ExperimentFold
		Comparisons                      []IncrementalComparison
	}{result.ContractVersion, result.DatasetID, result.DatasetManifestDigest, result.FeatureGroups, result.PreprocessingPolicy, result.Folds, result.IncrementalComparisons})
	result.ID = "experiment-" + result.ArtifactDigest[:32]
	return result, nil
}

func validateExperimentPartitions(partitions map[string][]ExperimentSample) error {
	clusters := map[string]string{}
	identities := map[string]bool{}
	for _, partition := range []string{"train", "calibration", "test"} {
		for _, sample := range partitions[partition] {
			cluster, id := strings.TrimSpace(sample.EventCluster), strings.TrimSpace(sample.ID)
			if id == "" || cluster == "" || identities[id] {
				return fmt.Errorf("experiment samples require unique ids and event clusters")
			}
			identities[id] = true
			if prior, exists := clusters[cluster]; exists && prior != partition {
				return fmt.Errorf("event cluster crosses %s and %s", prior, partition)
			}
			clusters[cluster] = partition
		}
	}
	return nil
}

func runFoldVariants(dataset StoredDataset, fold Fold, partitions map[string][]ExperimentSample, groups map[string][]string) []ExperimentVariant {
	train, calibrationRows, test := orderedExperimentSamples(partitions["train"]), orderedExperimentSamples(partitions["calibration"]), orderedExperimentSamples(partitions["test"])
	variants := []ExperimentVariant{historicalRateVariant(dataset.Objective, train, test), scoreRuleVariant("simple_rule", "simple_rule", simpleRuleFeature(train, test), dataset.Objective, train, test),
		scoreRuleVariant("llm_direction", "llm_direction_score", FeatureLLMDirection, dataset.Objective, train, test)}
	fullFeatures := experimentFeatureNames(groups)
	commonTrain := completeExperimentSamples(train, fullFeatures)
	commonCalibration := completeExperimentSamples(calibrationRows, fullFeatures)
	commonTest := completeExperimentSamples(test, fullFeatures)
	sets := []struct {
		name     string
		features []string
	}{
		{"logistic_full", fullFeatures},
		{"ablate_price", removeFeatureGroup(fullFeatures, groups["price"])},
		{"ablate_fundamental", removeFeatureGroup(fullFeatures, groups["fundamental"])},
		{"ablate_news", removeFeatureGroup(fullFeatures, groups["news"])},
		{"ablate_expectations", removeFeatureGroup(fullFeatures, groups["expectations"])},
	}
	for _, set := range sets {
		variants = append(variants, logisticVariant(set.name, set.features, dataset, fold, commonTrain, commonCalibration, commonTest))
	}
	return variants
}

func orderedExperimentSamples(values []ExperimentSample) []ExperimentSample {
	result := append([]ExperimentSample{}, values...)
	sort.Slice(result, func(i, j int) bool {
		if !result[i].SignalAt.Equal(result[j].SignalAt) {
			return result[i].SignalAt.Before(result[j].SignalAt)
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func historicalRateVariant(objective string, train, test []ExperimentSample) ExperimentVariant {
	variant := ExperimentVariant{Name: "historical_rate", Kind: "constant_historical_rate", Status: "unavailable", TrainingCount: len(train), TestCount: len(test), FeatureNames: []string{}}
	if len(train) == 0 || len(test) == 0 {
		variant.Reason = "insufficient_train_or_test_samples"
		return finishVariant(variant)
	}
	positive := 0
	for _, sample := range train {
		if sample.Label {
			positive++
		}
	}
	probability := (float64(positive) + 1) / (float64(len(train)) + 2)
	scores, probabilities := make([]float64, len(test)), make([]float64, len(test))
	for index := range test {
		scores[index], probabilities[index] = probability-.5, probability
	}
	variant.Status = "evaluated"
	variant.Metrics, variant.Predictions = evaluateVariant(test, scores, probabilities, objective)
	return finishVariant(variant)
}

func scoreRuleVariant(name, kind, feature, objective string, train, test []ExperimentSample) ExperimentVariant {
	variant := ExperimentVariant{Name: name, Kind: kind, Status: "unavailable", TrainingCount: len(train), TestCount: len(test)}
	if feature == "" {
		variant.Reason = "required_feature_unavailable"
		return finishVariant(variant)
	}
	variant.FeatureNames = []string{feature}
	eligible := completeExperimentSamples(test, []string{feature})
	if len(eligible) == 0 {
		variant.Reason = "required_feature_unavailable"
		return finishVariant(variant)
	}
	scores := make([]float64, len(eligible))
	for index, sample := range eligible {
		scores[index] = *sample.Values[feature]
	}
	variant.Status, variant.TestCount = "evaluated", len(eligible)
	variant.Metrics, variant.Predictions = evaluateVariant(eligible, scores, nil, objective)
	return finishVariant(variant)
}

func logisticVariant(name string, features []string, dataset StoredDataset, fold Fold, train, calibrationRows, test []ExperimentSample) ExperimentVariant {
	variant := ExperimentVariant{Name: name, Kind: "interpretable_logistic", Status: "unavailable", FeatureNames: features,
		TrainingCount: len(train), CalibrationCount: len(calibrationRows), TestCount: len(test)}
	if len(features) == 0 || len(train) == 0 || len(test) == 0 {
		variant.Reason = "insufficient_complete_feature_samples"
		return finishVariant(variant)
	}
	trainingSamples := toSignalSamples(train)
	model, err := signals.FitBinary(trainingSamples, features, signals.FitOptions{Objective: dataset.Objective, HorizonSessions: dataset.HorizonSessions,
		TrainingCutoff: fold.TrainCutoff, Iterations: 800, LearningRate: .04, L2: .01})
	if err != nil {
		variant.Reason = "model_fit_failed:" + err.Error()
		return finishVariant(variant)
	}
	variant.Model = &model
	calibrationObservations := []calibration.Observation{}
	for _, sample := range calibrationRows {
		score, ok := modelScore(model, sample)
		if ok {
			calibrationObservations = append(calibrationObservations, calibration.Observation{SampleID: sample.ID, EventCluster: sample.EventCluster, Score: score, Label: sample.Label, ObservedAt: sample.LabelMatureAt})
		}
	}
	calibrator, calibrationErr := calibration.FitPlatt(model.Version, calibrationObservations, calibration.Scope{AssetClass: dataset.AssetClass, Market: dataset.Market, HorizonSessions: dataset.HorizonSessions})
	if calibrationErr == nil {
		variant.Calibrator = &calibrator
	}
	scores, probabilities, evaluated := []float64{}, []float64{}, []ExperimentSample{}
	for _, sample := range test {
		score, ok := modelScore(model, sample)
		if !ok {
			continue
		}
		evaluated, scores = append(evaluated, sample), append(scores, score)
		if calibrationErr == nil {
			applied := calibrator.Apply(model.Version, score, dataset.AssetClass, dataset.Market, dataset.HorizonSessions, "", sample.SignalAt)
			if applied.Status != "calibrated" || applied.Probability == nil {
				calibrationErr = fmt.Errorf("calibrator rejected future test sample: %s", applied.Reason)
				probabilities = nil
			} else if probabilities != nil {
				probabilities = append(probabilities, *applied.Probability)
			}
		}
	}
	if len(evaluated) == 0 {
		variant.Reason = "no_complete_future_test_samples"
		return finishVariant(variant)
	}
	variant.Status, variant.TestCount = "evaluated", len(evaluated)
	if calibrationErr != nil {
		variant.Status = "evaluated_uncalibrated"
		variant.Reason = "independent_calibration_unavailable:" + calibrationErr.Error()
		probabilities = nil
	}
	variant.Metrics, variant.Predictions = evaluateVariant(evaluated, scores, probabilities, dataset.Objective)
	return finishVariant(variant)
}

func evaluateVariant(samples []ExperimentSample, scores, probabilities []float64, objective string) (VariantMetrics, []VariantPrediction) {
	metrics := VariantMetrics{SampleCount: len(samples)}
	predictions := []VariantPrediction{}
	if len(samples) == 0 || len(scores) != len(samples) {
		return metrics, predictions
	}
	positive, predictedPositive := 0, 0
	returns := make([]float64, 0, len(samples))
	returnScores := make([]float64, 0, len(samples))
	labels := make([]bool, len(samples))
	for index, sample := range samples {
		labels[index] = sample.Label
		if sample.Label {
			positive++
		}
		prediction := scores[index] >= 0
		if len(probabilities) == len(samples) {
			prediction = probabilities[index] >= .5
		}
		if prediction {
			predictedPositive++
		}
		if prediction == sample.Label {
			metrics.Correct++
		}
		item := VariantPrediction{PredictionRunID: sample.ID, EventCluster: sample.EventCluster, SignalAt: sample.SignalAt.UTC(), RawScore: scores[index],
			Label: sample.Label, Correct: prediction == sample.Label, RawReturn: sample.RawReturn, ExcessReturn: sample.ExcessReturn}
		if len(probabilities) == len(samples) {
			value := probabilities[index]
			item.Probability = &value
		}
		predictions = append(predictions, item)
		value := sample.RawReturn
		if objective == "excess_up" {
			value = sample.ExcessReturn
		}
		if value != nil {
			returns, returnScores = append(returns, *value), append(returnScores, scores[index])
		}
	}
	accuracy, positiveRate, predictedRate := float64(metrics.Correct)/float64(len(samples)), float64(positive)/float64(len(samples)), float64(predictedPositive)/float64(len(samples))
	metrics.Accuracy, metrics.PositiveRate, metrics.PredictedPositive = &accuracy, &positiveRate, &predictedRate
	metrics.RankIC = correlationPointer(returnScores, returns)
	if len(probabilities) == len(samples) {
		if probabilityMetrics, err := calibration.Evaluate(probabilities, labels, 10); err == nil {
			metrics.Probability = &probabilityMetrics
		}
	}
	return metrics, predictions
}

func compareAblations(foldIndex int, variants []ExperimentVariant) []IncrementalComparison {
	byName := map[string]ExperimentVariant{}
	for _, variant := range variants {
		byName[variant.Name] = variant
	}
	full := byName["logistic_full"]
	result := []IncrementalComparison{}
	for _, group := range []string{"price", "fundamental", "news", "expectations"} {
		ablated := byName["ablate_"+group]
		comparison := IncrementalComparison{FoldIndex: foldIndex, FullVariant: full.Name, AblatedVariant: ablated.Name, FeatureGroup: group}
		if full.Metrics.Accuracy == nil || ablated.Metrics.Accuracy == nil || full.Metrics.SampleCount != ablated.Metrics.SampleCount {
			comparison.Reason = "same_cohort_future_metrics_unavailable"
		} else {
			comparison.Comparable = true
			value := *full.Metrics.Accuracy - *ablated.Metrics.Accuracy
			comparison.AccuracyDelta = &value
			if full.Metrics.Probability != nil && ablated.Metrics.Probability != nil {
				brier := ablated.Metrics.Probability.Brier - full.Metrics.Probability.Brier
				comparison.BrierDelta = &brier
			}
		}
		result = append(result, comparison)
	}
	return result
}

func finishVariant(variant ExperimentVariant) ExperimentVariant {
	if variant.Predictions == nil {
		variant.Predictions = []VariantPrediction{}
	}
	copy := variant
	copy.ArtifactDigest = ""
	variant.ArtifactDigest = digestValue(copy)
	return variant
}

func modelScore(model signals.BinaryModel, sample ExperimentSample) (float64, bool) {
	prediction := model.Predict(signals.Snapshot{AsOf: sample.SignalAt, Values: sample.Values})
	returnValue := 0.0
	if prediction.RawScore == nil {
		return returnValue, false
	}
	return *prediction.RawScore, true
}

func toSignalSamples(values []ExperimentSample) []signals.Sample {
	result := make([]signals.Sample, 0, len(values))
	for _, sample := range values {
		result = append(result, signals.Sample{ID: sample.ID, EventCluster: sample.EventCluster, SignalAt: sample.SignalAt,
			LabelMatureAt: sample.LabelMatureAt, Values: sample.Values, Label: sample.Label})
	}
	return result
}

func completeExperimentSamples(values []ExperimentSample, features []string) []ExperimentSample {
	result := []ExperimentSample{}
	for _, sample := range values {
		complete := true
		for _, feature := range features {
			value := sample.Values[feature]
			if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) {
				complete = false
				break
			}
		}
		if complete {
			result = append(result, sample)
		}
	}
	return result
}

func simpleRuleFeature(train, test []ExperimentSample) string {
	for _, name := range []string{signals.FeatureConsensusSurprise, signals.FeaturePriceReaction, signals.FeatureNewInformation, signals.FeatureBusinessExposure} {
		if len(completeExperimentSamples(train, []string{name})) > 0 && len(completeExperimentSamples(test, []string{name})) > 0 {
			return name
		}
	}
	return ""
}

func experimentFeatureNames(groups map[string][]string) []string {
	values := []string{}
	for _, group := range []string{"price", "fundamental", "news", "expectations"} {
		values = append(values, groups[group]...)
	}
	seen, result := map[string]bool{}, []string{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" && !seen[value] {
			seen[value], result = true, append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func removeFeatureGroup(all, group []string) []string {
	removed := map[string]bool{}
	for _, item := range group {
		removed[item] = true
	}
	result := []string{}
	for _, item := range all {
		if !removed[item] {
			result = append(result, item)
		}
	}
	return result
}
