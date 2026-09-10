package evaluation

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/calibration"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

func TestFinalHoldoutEvaluationUsesOnePreapprovedArtifactAndRedactsSamples(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	model := signals.BinaryModel{Kind: signals.ModelKindLearnedLogistic, Version: "frozen-model", Objective: "absolute_up", HorizonSessions: 1,
		TrainingCutoff: base, FeatureNames: []string{"signal"}, Means: map[string]float64{"signal": 0}, Scales: map[string]float64{"signal": 1},
		Coefficients: map[string]float64{"signal": 1}, SampleCount: 40}
	calibrator := calibration.Model{Version: "frozen-calibrator", SourceModelVersion: model.Version, Method: "platt", Slope: 1,
		TrainedUntil: base.Add(time.Hour), Scope: calibration.Scope{AssetClass: "equity", Market: "US", HorizonSessions: 1}}
	variant := ExperimentVariant{Name: "logistic_full", Kind: "interpretable_logistic", Status: "evaluated", Model: &model,
		Calibrator: &calibrator, ArtifactDigest: "frozen-variant-digest"}
	dataset := StoredDataset{Manifest: DatasetManifest{ID: "dataset-final", ContractVersion: WalkForwardDatasetContractVersion, ManifestDigest: "dataset-digest",
		Config: WalkForwardConfig{HoldoutReservationID: "holdout-final"}, FinalHoldout: FinalHoldoutDescriptor{IncludedCount: 2},
		Folds: []Fold{{Index: 0}, {Index: 1}}}, AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1}
	experiment := StoredExperiment{Experiment: DevelopmentExperiment{ID: "experiment-final", ContractVersion: DevelopmentExperimentContractVersion, DatasetID: dataset.Manifest.ID,
		AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		Folds: []ExperimentFold{{Index: 0}, {Index: 1, Variants: []ExperimentVariant{variant}}}}}
	development := StoredPerformanceReport{Report: LayeredPerformanceReport{ID: "performance-final", ContractVersion: LayeredPerformanceReportContractVersion,
		DatasetID: dataset.Manifest.ID, ExperimentID: experiment.Experiment.ID, AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		ArtifactDigest: "development-report-digest", SelectionDecision: "no_automatic_model_selection"}}
	input := FinalHoldoutEvaluationInput{DatasetID: dataset.Manifest.ID, ExperimentID: experiment.Experiment.ID,
		DevelopmentReportID: development.Report.ID, FoldIndex: 1, VariantName: variant.Name, VariantArtifactDigest: variant.ArtifactDigest,
		ApprovedBy: "independent reviewer", ApprovalReason: "locked after development review"}
	positive, negative := 1.0, -1.0
	samples := []ExperimentSample{
		{ID: "secret-positive", EventCluster: "cluster-positive", SignalAt: base.Add(48 * time.Hour), LabelMatureAt: base.Add(72 * time.Hour), Values: map[string]*float64{"signal": &positive}, Label: true},
		{ID: "secret-negative", EventCluster: "cluster-negative", SignalAt: base.Add(72 * time.Hour), LabelMatureAt: base.Add(96 * time.Hour), Values: map[string]*float64{"signal": &negative}, Label: false},
	}
	report, err := buildFinalHoldoutEvaluation(dataset, experiment, development, input, "request-digest", samples, base.Add(120*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "evaluated" || report.Metrics.SampleCount != 2 || report.Metrics.Correct != 2 || report.ArtifactDigest == "" || report.PredictionEvidenceDigest == "" {
		t.Fatalf("unexpected final holdout report: %#v", report)
	}
	if !report.FinalHoldoutAccessed || report.SampleIDsShown || report.AutomaticModelSelection || !report.SelectionLockedBeforeHoldoutAccess {
		t.Fatalf("final holdout governance flags are incorrect: %#v", report)
	}
	body, _ := json.Marshal(report)
	if strings.Contains(string(body), "secret-positive") || strings.Contains(string(body), "secret-negative") {
		t.Fatalf("sealed sample identifiers leaked through the aggregate report: %s", body)
	}
}

func TestFinalHoldoutEvaluationRejectsUnlockedOrChangedSelection(t *testing.T) {
	dataset := StoredDataset{Manifest: DatasetManifest{ID: "dataset", ContractVersion: WalkForwardDatasetContractVersion, ManifestDigest: "digest", Config: WalkForwardConfig{HoldoutReservationID: "holdout"},
		FinalHoldout: FinalHoldoutDescriptor{IncludedCount: 1}, Folds: []Fold{{Index: 2}}}, AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1}
	variant := ExperimentVariant{Name: "simple_rule", Kind: "simple_rule", Status: "evaluated", FeatureNames: []string{"signal"}, ArtifactDigest: "locked-digest"}
	experiment := StoredExperiment{Experiment: DevelopmentExperiment{ID: "experiment", ContractVersion: DevelopmentExperimentContractVersion, DatasetID: "dataset",
		AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1, Folds: []ExperimentFold{{Index: 2, Variants: []ExperimentVariant{variant}}}}}
	development := StoredPerformanceReport{Report: LayeredPerformanceReport{ID: "report", ContractVersion: LayeredPerformanceReportContractVersion,
		DatasetID: "dataset", ExperimentID: "experiment", AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		ArtifactDigest: "report-digest", SelectionDecision: "no_automatic_model_selection"}}
	value := 1.0
	samples := []ExperimentSample{{ID: "secret", EventCluster: "cluster", SignalAt: time.Now().UTC(), Values: map[string]*float64{"signal": &value}, Label: true}}
	input := FinalHoldoutEvaluationInput{DatasetID: "dataset", ExperimentID: "experiment", DevelopmentReportID: "report", FoldIndex: 1,
		VariantName: "simple_rule", VariantArtifactDigest: "locked-digest", ApprovedBy: "reviewer", ApprovalReason: "approved"}
	if _, err := buildFinalHoldoutEvaluation(dataset, experiment, development, input, "request", samples, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "latest development fold") {
		t.Fatalf("non-latest fold was accepted: %v", err)
	}
	input.FoldIndex, input.VariantArtifactDigest = 2, "changed-after-approval"
	if _, err := buildFinalHoldoutEvaluation(dataset, experiment, development, input, "request", samples, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "artifact digest") {
		t.Fatalf("changed variant artifact was accepted: %v", err)
	}
}
