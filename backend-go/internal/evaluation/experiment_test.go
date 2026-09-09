package evaluation

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

func TestDevelopmentExperimentUsesIndependentFutureCohortsAndReportsAblations(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fold := Fold{Index: 0, TrainCutoff: base.AddDate(0, 0, 60), CalibrationCutoff: base.AddDate(0, 0, 102), TestCutoff: base.AddDate(0, 0, 145)}
	dataset := StoredDataset{AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		Manifest: DatasetManifest{ID: "dataset-test", ManifestDigest: "manifest-test", Folds: []Fold{fold}}}
	partitions := map[string][]ExperimentSample{"train": {}, "calibration": {}, "test": {}}
	for index := 0; index < 50; index++ {
		partitions["train"] = append(partitions["train"], experimentFixtureSample("train", index, base.AddDate(0, 0, index), index%2 == 0))
	}
	for index := 0; index < 40; index++ {
		partitions["calibration"] = append(partitions["calibration"], experimentFixtureSample("calibration", index, base.AddDate(0, 0, 62+index), index%2 == 0))
		partitions["test"] = append(partitions["test"], experimentFixtureSample("test", index, base.AddDate(0, 0, 104+index), index%2 == 0))
	}
	experiment, err := RunDevelopmentExperiment(dataset, map[int]map[string][]ExperimentSample{0: partitions})
	if err != nil {
		t.Fatal(err)
	}
	if experiment.FinalHoldoutAccessed || experiment.ID == "" || experiment.ArtifactDigest == "" || len(experiment.IncrementalComparisons) != 4 {
		t.Fatalf("experiment contract is incomplete: %#v", experiment)
	}
	variants := map[string]ExperimentVariant{}
	for _, variant := range experiment.Folds[0].Variants {
		variants[variant.Name] = variant
	}
	full := variants["logistic_full"]
	if full.Status != "evaluated" || full.Model == nil || full.Calibrator == nil || full.Metrics.Probability == nil || full.Metrics.Probability.SampleCount != 40 {
		t.Fatalf("independent calibration/future evaluation did not complete: %#v", full)
	}
	if variants["historical_rate"].Metrics.Accuracy == nil || variants["simple_rule"].Metrics.Accuracy == nil {
		t.Fatalf("simple future baselines are missing: %#v", variants)
	}
	if variants["llm_direction"].Status != "unavailable" || variants["llm_direction"].Reason != "required_feature_unavailable" {
		t.Fatalf("missing LLM score was not disclosed: %#v", variants["llm_direction"])
	}
	for _, comparison := range experiment.IncrementalComparisons {
		if !comparison.Comparable || comparison.AccuracyDelta == nil {
			t.Fatalf("ablation was not evaluated on the same future cohort: %#v", comparison)
		}
	}
	slices.Reverse(partitions["train"])
	slices.Reverse(partitions["calibration"])
	slices.Reverse(partitions["test"])
	repeated, err := RunDevelopmentExperiment(dataset, map[int]map[string][]ExperimentSample{0: partitions})
	if err != nil || repeated.ID != experiment.ID {
		t.Fatalf("experiment was not reproducible after input reorder: first=%s repeated=%s err=%v", experiment.ID, repeated.ID, err)
	}
}

func TestDevelopmentExperimentRejectsSealedAndCrossPartitionSamples(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	dataset := StoredDataset{AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		Manifest: DatasetManifest{ID: "dataset-test", ManifestDigest: "manifest-test", Folds: []Fold{{Index: 0}}}}
	partitions := map[string][]ExperimentSample{
		"train":         {experimentFixtureSample("train", 0, base, true)},
		"calibration":   {experimentFixtureSample("calibration", 0, base.AddDate(0, 0, 2), false)},
		"test":          {experimentFixtureSample("test", 0, base.AddDate(0, 0, 4), true)},
		"final_holdout": {experimentFixtureSample("final_holdout", 0, base.AddDate(0, 0, 6), true)},
	}
	if _, err := RunDevelopmentExperiment(dataset, map[int]map[string][]ExperimentSample{0: partitions}); err == nil {
		t.Fatal("sealed final holdout input was accepted by a development experiment")
	}
	delete(partitions, "final_holdout")
	partitions["calibration"][0].EventCluster = partitions["train"][0].EventCluster
	if _, err := RunDevelopmentExperiment(dataset, map[int]map[string][]ExperimentSample{0: partitions}); err == nil {
		t.Fatal("cross-partition event cluster was accepted by a development experiment")
	}
}

func experimentFixtureSample(partition string, index int, signal time.Time, label bool) ExperimentSample {
	sign := -1.0
	if label {
		sign = 1
	}
	raw := sign * .01
	values := map[string]*float64{}
	for offset, name := range []string{signals.FeaturePriceReaction, signals.FeatureBusinessExposure, signals.FeatureNewInformation, signals.FeatureConsensusSurprise} {
		value := sign * (1 + float64(offset)/10)
		values[name] = &value
	}
	return ExperimentSample{ID: fmt.Sprintf("%s-%02d", partition, index), EventCluster: fmt.Sprintf("%s-cluster-%02d", partition, index),
		Partition: partition, SignalAt: signal, LabelMatureAt: signal.AddDate(0, 0, 1), Values: values, Label: label, RawReturn: &raw}
}
