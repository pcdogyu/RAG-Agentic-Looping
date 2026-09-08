package prediction

import (
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

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
