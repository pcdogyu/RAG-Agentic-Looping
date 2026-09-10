package evaluation

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	FinalHoldoutEvaluationContractVersion = "one-time-final-holdout-evaluation-v1"
	FinalHoldoutSelectionDecision         = "preapproved_single_variant_evaluated_once_no_model_selection"
)

type FinalHoldoutEvaluationInput struct {
	DatasetID             string `json:"dataset_id"`
	ExperimentID          string `json:"experiment_id"`
	DevelopmentReportID   string `json:"development_report_id"`
	FoldIndex             int    `json:"fold_index"`
	VariantName           string `json:"variant_name"`
	VariantArtifactDigest string `json:"variant_artifact_digest"`
	ApprovedBy            string `json:"approved_by"`
	ApprovalReason        string `json:"approval_reason"`
	IdempotencyKey        string `json:"-"`
}

type FinalHoldoutEvaluationReport struct {
	ID                                 string         `json:"id"`
	ContractVersion                    string         `json:"contract_version"`
	DatasetID                          string         `json:"dataset_id"`
	DatasetManifestDigest              string         `json:"dataset_manifest_digest"`
	HoldoutReservationID               string         `json:"holdout_reservation_id"`
	ExperimentID                       string         `json:"experiment_id"`
	DevelopmentReportID                string         `json:"development_report_id"`
	DevelopmentReportArtifactDigest    string         `json:"development_report_artifact_digest"`
	AssetClass                         string         `json:"asset_class"`
	Market                             string         `json:"market"`
	Objective                          string         `json:"objective"`
	HorizonSessions                    int            `json:"horizon_sessions"`
	FoldIndex                          int            `json:"fold_index"`
	VariantName                        string         `json:"variant_name"`
	VariantKind                        string         `json:"variant_kind"`
	VariantArtifactDigest              string         `json:"variant_artifact_digest"`
	Status                             string         `json:"status"`
	Reason                             string         `json:"reason,omitempty"`
	SealedSampleCount                  int            `json:"sealed_sample_count"`
	IndependentEventClusters           int            `json:"independent_event_clusters"`
	EvaluatedSampleCount               int            `json:"evaluated_sample_count"`
	UnavailableSampleCount             int            `json:"unavailable_sample_count"`
	Metrics                            VariantMetrics `json:"metrics"`
	PredictionEvidenceDigest           string         `json:"prediction_evidence_digest"`
	FinalHoldoutAccessed               bool           `json:"final_holdout_accessed"`
	SampleIDsShown                     bool           `json:"sample_ids_shown"`
	AutomaticModelSelection            bool           `json:"automatic_model_selection"`
	SelectionDecision                  string         `json:"selection_decision"`
	SelectionLockedBeforeHoldoutAccess bool           `json:"selection_locked_before_holdout_access"`
	ApprovedBy                         string         `json:"approved_by"`
	ApprovalReason                     string         `json:"approval_reason"`
	RequestDigest                      string         `json:"request_digest"`
	ArtifactDigest                     string         `json:"artifact_digest"`
	EvaluatedAt                        time.Time      `json:"evaluated_at"`
}

func buildFinalHoldoutEvaluation(dataset StoredDataset, experiment StoredExperiment, development StoredPerformanceReport,
	input FinalHoldoutEvaluationInput, requestDigest string, samples []ExperimentSample, evaluatedAt time.Time) (FinalHoldoutEvaluationReport, error) {
	variant, err := validateFinalHoldoutSelection(dataset, experiment, development, input)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, err
	}
	if len(samples) != dataset.Manifest.FinalHoldout.IncludedCount || len(samples) == 0 {
		return FinalHoldoutEvaluationReport{}, fmt.Errorf("sealed final holdout sample count does not match the immutable manifest")
	}
	clusters := map[string]bool{}
	evaluable, scores, probabilities := []ExperimentSample{}, []float64{}, []float64{}
	calibrated := variant.Calibrator != nil
	for _, sample := range samples {
		cluster := strings.TrimSpace(sample.EventCluster)
		if cluster == "" {
			cluster = "id:" + strings.TrimSpace(sample.ID)
		}
		clusters[cluster] = true
		score, available := finalHoldoutScore(variant, sample)
		if !available {
			continue
		}
		evaluable, scores = append(evaluable, sample), append(scores, score)
		if calibrated {
			applied := variant.Calibrator.Apply(variant.Model.Version, score, dataset.AssetClass, dataset.Market, dataset.HorizonSessions, "", sample.SignalAt)
			if applied.Status != "calibrated" || applied.Probability == nil {
				calibrated = false
				probabilities = nil
			} else if probabilities != nil {
				probabilities = append(probabilities, *applied.Probability)
			}
		}
	}
	if !calibrated {
		probabilities = nil
	}
	metrics, detailed := evaluateVariant(evaluable, scores, probabilities, dataset.Objective)
	report := FinalHoldoutEvaluationReport{
		ContractVersion: FinalHoldoutEvaluationContractVersion, DatasetID: dataset.Manifest.ID, DatasetManifestDigest: dataset.Manifest.ManifestDigest,
		HoldoutReservationID: dataset.Manifest.Config.HoldoutReservationID, ExperimentID: experiment.Experiment.ID,
		DevelopmentReportID: development.Report.ID, DevelopmentReportArtifactDigest: development.Report.ArtifactDigest,
		AssetClass: dataset.AssetClass, Market: dataset.Market, Objective: dataset.Objective, HorizonSessions: dataset.HorizonSessions,
		FoldIndex: input.FoldIndex, VariantName: variant.Name, VariantKind: variant.Kind, VariantArtifactDigest: variant.ArtifactDigest,
		Status: "evaluated", SealedSampleCount: len(samples), IndependentEventClusters: len(clusters), EvaluatedSampleCount: len(evaluable),
		UnavailableSampleCount: len(samples) - len(evaluable), Metrics: metrics, PredictionEvidenceDigest: digestValue(detailed),
		FinalHoldoutAccessed: true, SampleIDsShown: false, AutomaticModelSelection: false,
		SelectionDecision: FinalHoldoutSelectionDecision, SelectionLockedBeforeHoldoutAccess: true,
		ApprovedBy: input.ApprovedBy, ApprovalReason: input.ApprovalReason, RequestDigest: requestDigest, EvaluatedAt: evaluatedAt.UTC(),
	}
	if len(evaluable) == 0 {
		report.Status, report.Reason = "unavailable", "preapproved_variant_could_not_score_sealed_samples"
	} else if variant.Calibrator != nil && !calibrated {
		report.Status, report.Reason = "evaluated_uncalibrated", "frozen_calibrator_rejected_one_or_more_final_samples"
	} else if variant.Calibrator == nil {
		report.Status, report.Reason = "evaluated_uncalibrated", "preapproved_variant_has_no_frozen_calibrator"
	}
	copy := report
	copy.ID, copy.ArtifactDigest = "", ""
	report.ArtifactDigest = digestValue(copy)
	report.ID = "final-holdout-" + report.ArtifactDigest[:32]
	return report, nil
}

func validateFinalHoldoutSelection(dataset StoredDataset, experiment StoredExperiment, development StoredPerformanceReport,
	input FinalHoldoutEvaluationInput) (ExperimentVariant, error) {
	if dataset.Manifest.ID == "" || experiment.Experiment.ID == "" || development.Report.ID == "" {
		return ExperimentVariant{}, fmt.Errorf("persisted dataset, experiment and development report are required")
	}
	if input.DatasetID != dataset.Manifest.ID || input.ExperimentID != experiment.Experiment.ID || input.DevelopmentReportID != development.Report.ID ||
		experiment.Experiment.DatasetID != dataset.Manifest.ID || development.Report.DatasetID != dataset.Manifest.ID || development.Report.ExperimentID != experiment.Experiment.ID {
		return ExperimentVariant{}, fmt.Errorf("dataset, experiment and development report must match")
	}
	if dataset.Manifest.ContractVersion != WalkForwardDatasetContractVersion || experiment.Experiment.ContractVersion != DevelopmentExperimentContractVersion ||
		development.Report.ContractVersion != LayeredPerformanceReportContractVersion || dataset.Manifest.ManifestDigest == "" ||
		dataset.Manifest.Config.HoldoutReservationID == "" || development.Report.ArtifactDigest == "" {
		return ExperimentVariant{}, fmt.Errorf("supported immutable dataset, experiment and development report artifacts are required")
	}
	if experiment.Experiment.AssetClass != dataset.AssetClass || experiment.Experiment.Market != dataset.Market || experiment.Experiment.Objective != dataset.Objective ||
		experiment.Experiment.HorizonSessions != dataset.HorizonSessions || development.Report.AssetClass != dataset.AssetClass ||
		development.Report.Market != dataset.Market || development.Report.Objective != dataset.Objective || development.Report.HorizonSessions != dataset.HorizonSessions {
		return ExperimentVariant{}, fmt.Errorf("dataset, experiment and development report scopes must match")
	}
	if development.Report.FinalHoldoutAccessed || development.Report.SelectionDecision != "no_automatic_model_selection" {
		return ExperimentVariant{}, fmt.Errorf("a development-only report with no automatic model selection is required")
	}
	if len(experiment.Experiment.Folds) == 0 {
		return ExperimentVariant{}, fmt.Errorf("development experiment folds are required")
	}
	latest := experiment.Experiment.Folds[len(experiment.Experiment.Folds)-1]
	for _, fold := range experiment.Experiment.Folds {
		if fold.Index > latest.Index {
			latest = fold
		}
	}
	if input.FoldIndex != latest.Index {
		return ExperimentVariant{}, fmt.Errorf("fold_index must select the latest development fold")
	}
	variant, ok := finalHoldoutVariant(latest.Variants, input.VariantName)
	if !ok || variant.ArtifactDigest != input.VariantArtifactDigest {
		return ExperimentVariant{}, fmt.Errorf("the preapproved variant and artifact digest do not match the frozen experiment")
	}
	if variant.Status != "evaluated" && variant.Status != "evaluated_uncalibrated" {
		return ExperimentVariant{}, fmt.Errorf("the preapproved variant is not evaluable")
	}
	if variant.Model == nil && !((variant.Kind == "simple_rule" || variant.Kind == "llm_direction_score") && len(variant.FeatureNames) == 1) {
		return ExperimentVariant{}, fmt.Errorf("the preapproved variant has no replayable frozen scoring artifact")
	}
	if variant.Model != nil && (variant.Model.Version == "" || variant.Model.Objective != dataset.Objective || variant.Model.HorizonSessions != dataset.HorizonSessions) {
		return ExperimentVariant{}, fmt.Errorf("the frozen model artifact does not match the final holdout scope")
	}
	if variant.Calibrator != nil && variant.Model == nil {
		return ExperimentVariant{}, fmt.Errorf("the frozen calibrator has no matching model artifact")
	}
	if variant.Calibrator != nil && (variant.Calibrator.SourceModelVersion != variant.Model.Version ||
		!strings.EqualFold(variant.Calibrator.Scope.Market, dataset.Market) || variant.Calibrator.Scope.HorizonSessions != dataset.HorizonSessions) {
		return ExperimentVariant{}, fmt.Errorf("the frozen calibrator does not match the selected model and final holdout scope")
	}
	return variant, nil
}

func finalHoldoutVariant(values []ExperimentVariant, name string) (ExperimentVariant, bool) {
	name = strings.TrimSpace(name)
	for _, value := range values {
		if value.Name == name {
			return value, true
		}
	}
	return ExperimentVariant{}, false
}

func finalHoldoutScore(variant ExperimentVariant, sample ExperimentSample) (float64, bool) {
	if variant.Model != nil {
		value, ok := modelScore(*variant.Model, sample)
		return value, ok && !math.IsNaN(value) && !math.IsInf(value, 0)
	}
	if (variant.Kind == "simple_rule" || variant.Kind == "llm_direction_score") && len(variant.FeatureNames) == 1 {
		value := sample.Values[variant.FeatureNames[0]]
		if value != nil && !math.IsNaN(*value) && !math.IsInf(*value, 0) {
			return *value, true
		}
	}
	return 0, false
}
