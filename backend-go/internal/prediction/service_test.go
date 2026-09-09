package prediction

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

func TestSourceOverlapDetectsLineageReplacement(t *testing.T) {
	if got := setOverlap(map[string]bool{"sec": true, "fmp": true}, map[string]bool{"sec": true, "new-vendor": true}); got != 1.0/3.0 {
		t.Fatalf("source overlap=%v", got)
	}
	if got := setOverlap(map[string]bool{}, map[string]bool{}); got != 1 {
		t.Fatalf("empty lineage overlap=%v", got)
	}
}

func TestPredictionRunIdentityIncludesModelCalibrationAndFeatures(t *testing.T) {
	now := time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC)
	value := .4
	base := Run{
		AssetID: "equity:XNAS:AAPL", EventID: "00000000-0000-0000-0000-000000000001",
		SignalAvailableAt: now, HorizonSessions: 5, Objective: "excess_up", ModelVersion: "model-v1",
		FeatureSnapshot: signals.Snapshot{AsOf: now, Values: map[string]*float64{"surprise": &value}, SourceIDs: map[string][]string{"surprise": {"consensus-1"}}, Missing: []string{}, FutureBlocked: []string{}},
	}
	first := predictionRunID(base)
	changedModel := base
	changedModel.ModelVersion = "model-v2"
	if first == predictionRunID(changedModel) {
		t.Fatal("model version was absent from prediction identity")
	}
	changedCalibration := base
	changedCalibration.CalibrationVersion = "calibration-v2"
	if first == predictionRunID(changedCalibration) {
		t.Fatal("calibration version was absent from prediction identity")
	}
	changedValue := .8
	changedFeatures := base
	changedFeatures.FeatureSnapshot.Values = map[string]*float64{"surprise": &changedValue}
	if first == predictionRunID(changedFeatures) {
		t.Fatal("feature snapshot was absent from prediction identity")
	}
	equity := base
	equity.AssetClass = "equity"
	if first != predictionRunID(equity) {
		t.Fatal("explicit equity scope changed the legacy prediction identity")
	}
	crypto := base
	crypto.AssetClass = "crypto"
	if first == predictionRunID(crypto) {
		t.Fatal("non-equity asset class was absent from prediction identity")
	}
	otherListing := base
	otherListing.AssetID = "equity:XHKG:09988"
	if first == predictionRunID(otherListing) {
		t.Fatal("different listings of the same issuer shared a prediction identity")
	}
}

func TestModelRegistrationValidationBindsArtifactAndFiniteSchema(t *testing.T) {
	model := signals.BinaryModel{Version: "model-v1", Objective: "excess_up", HorizonSessions: 5, TrainingCutoff: time.Now().UTC(), FeatureNames: []string{"surprise"}, Means: map[string]float64{"surprise": 0}, Scales: map[string]float64{"surprise": 1}, Coefficients: map[string]float64{"surprise": .4}, SampleCount: 30}
	if err := validateModel(model); err != nil {
		t.Fatal(err)
	}
	if ModelArtifactDigest(model) == "" {
		t.Fatal("model artifact digest is empty")
	}
	model.Scales["surprise"] = 0
	if err := validateModel(model); err == nil {
		t.Fatal("zero feature scale was accepted")
	}
}

func TestModelRegistrationValidationRejectsUnfrozenOutcomeContracts(t *testing.T) {
	base := signals.BinaryModel{Version: "model-v1", Objective: "excess_up", HorizonSessions: 5, TrainingCutoff: time.Now().UTC(), FeatureNames: []string{"surprise"}, Means: map[string]float64{"surprise": 0}, Scales: map[string]float64{"surprise": 1}, Coefficients: map[string]float64{"surprise": .4}, SampleCount: 30}
	unsupportedHorizon := base
	unsupportedHorizon.HorizonSessions = 2
	if err := validateModel(unsupportedHorizon); err == nil {
		t.Fatal("calendar-like two-session horizon was accepted outside the frozen label definition")
	}
	unsupportedObjective := base
	unsupportedObjective.Objective = "direction_score"
	if err := validateModel(unsupportedObjective); err == nil {
		t.Fatal("heuristic direction score was accepted as a trained outcome objective")
	}
}

func TestFixedRuleCollectionBaselineIsUntrainedAndStrictlyFrozen(t *testing.T) {
	model := signals.BinaryModel{
		Kind: signals.ModelKindFixedRule, Version: "fixed-v1", Objective: "absolute_up", HorizonSessions: 5,
		TrainingCutoff: time.Now().UTC(), FeatureNames: []string{signals.FeatureLLMDirectionScore},
		Means: map[string]float64{signals.FeatureLLMDirectionScore: 0}, Scales: map[string]float64{signals.FeatureLLMDirectionScore: 1},
		Coefficients: map[string]float64{signals.FeatureLLMDirectionScore: 1}, SampleCount: 0,
	}
	if err := validateModel(model); err != nil {
		t.Fatal(err)
	}
	model.Coefficients[signals.FeatureLLMDirectionScore] = .9
	if err := validateModel(model); err == nil {
		t.Fatal("modified fixed-rule coefficient was accepted")
	}
	model.Kind = signals.ModelKindLearnedLogistic
	if err := validateModel(model); err == nil {
		t.Fatal("zero-sample learned model was accepted")
	}
}
