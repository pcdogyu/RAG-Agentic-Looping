package evaluation

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/calibration"
)

const LayeredPerformanceReportContractVersion = "layered-performance-report-v1"

type ResearchPerformanceLayer struct {
	Status                    string                             `json:"status"`
	CandidatePredictions      int                                `json:"candidate_predictions"`
	LinkedResearchRuns        int                                `json:"linked_research_runs"`
	Completed                 int                                `json:"completed"`
	InsufficientEvidence      int                                `json:"insufficient_evidence"`
	TechnicalFailures         int                                `json:"technical_failures"`
	MissingResearch           int                                `json:"missing_research"`
	Coverage                  *float64                           `json:"coverage,omitempty"`
	RefusalRate               *float64                           `json:"refusal_rate,omitempty"`
	MeanLatencySeconds        *float64                           `json:"mean_latency_seconds,omitempty"`
	ReviewedPolicyEvaluations int                                `json:"reviewed_policy_evaluations"`
	AcceptedPolicyEvaluations int                                `json:"accepted_policy_evaluations"`
	CompositeReviewAccuracy   *float64                           `json:"composite_review_accuracy,omitempty"`
	DimensionMetrics          map[string]ResearchDimensionMetric `json:"dimension_metrics"`
	MissingReasons            map[string]int                     `json:"missing_reasons"`
}

type ResearchPerformanceSummary struct {
	CandidatePredictions      int
	LinkedResearchRuns        int
	Completed                 int
	InsufficientEvidence      int
	TechnicalFailures         int
	MissingResearch           int
	MeanLatencySeconds        *float64
	ReviewedPolicyEvaluations int
	AcceptedPolicyEvaluations int
	FactReviewed              int
	FactCorrect               int
	RelationshipReviewed      int
	RelationshipCorrect       int
	CitationReviewed          int
	CitationCorrect           int
	RefusalReviewed           int
	RefusalAppropriate        int
}

type ResearchDimensionMetric struct {
	Status         string   `json:"status"`
	Reason         string   `json:"reason,omitempty"`
	Reviewed       int      `json:"reviewed"`
	Correct        int      `json:"correct"`
	Accuracy       *float64 `json:"accuracy,omitempty"`
	AccuracyLow95  *float64 `json:"accuracy_low_95,omitempty"`
	AccuracyHigh95 *float64 `json:"accuracy_high_95,omitempty"`
}

type DirectionReturnBucket struct {
	Direction        string   `json:"direction"`
	Count            int      `json:"count"`
	MeanRawReturn    *float64 `json:"mean_raw_return,omitempty"`
	MeanExcessReturn *float64 `json:"mean_excess_return,omitempty"`
}

type SignalVariantPerformance struct {
	Name               string                  `json:"name"`
	Status             string                  `json:"status"`
	Reason             string                  `json:"reason,omitempty"`
	SampleCount        int                     `json:"sample_count"`
	Accuracy           *float64                `json:"accuracy,omitempty"`
	AccuracyLow95      *float64                `json:"accuracy_low_95,omitempty"`
	AccuracyHigh95     *float64                `json:"accuracy_high_95,omitempty"`
	PositiveRate       *float64                `json:"positive_rate,omitempty"`
	PredictedPositive  *float64                `json:"predicted_positive_rate,omitempty"`
	RankIC             *float64                `json:"rank_ic,omitempty"`
	Probability        *calibration.Metrics    `json:"probability,omitempty"`
	DirectionReturns   []DirectionReturnBucket `json:"direction_returns"`
	SmallSampleWarning bool                    `json:"small_sample_warning"`
	ArtifactDigest     string                  `json:"artifact_digest"`
}

type SignalFoldPerformance struct {
	FoldIndex int                        `json:"fold_index"`
	Variants  []SignalVariantPerformance `json:"variants"`
}

type SignalPerformanceLayer struct {
	Status              string                  `json:"status"`
	ExperimentID        string                  `json:"experiment_id"`
	SampleUnit          string                  `json:"sample_unit"`
	CorrelationHandling string                  `json:"correlation_handling"`
	PooledReport        bool                    `json:"pooled_report"`
	Folds               []SignalFoldPerformance `json:"folds"`
}

type ExecutionPerformanceSummary struct {
	TestPredictions  int
	Executable       int
	MeanRawReturn    *float64
	MeanExcessReturn *float64
	MeanNetReturn    *float64
}

type ExecutionPerformanceLayer struct {
	Status             string   `json:"status"`
	Reason             string   `json:"reason,omitempty"`
	TestPredictions    int      `json:"test_predictions"`
	ExecutableSamples  int      `json:"executable_samples"`
	MeanRawReturn      *float64 `json:"mean_raw_return,omitempty"`
	MeanExcessReturn   *float64 `json:"mean_excess_return,omitempty"`
	MeanNetReturn      *float64 `json:"mean_net_return,omitempty"`
	TurnoverStatus     string   `json:"turnover_status"`
	StrategyDrawdown   *float64 `json:"strategy_drawdown,omitempty"`
	DrawdownStatus     string   `json:"drawdown_status"`
	ExposureStatus     string   `json:"exposure_status"`
	ResearchResultOnly bool     `json:"research_result_only"`
}

type PerformanceDataQuality struct {
	CandidatePredictions       int            `json:"candidate_predictions"`
	DevelopmentAssignments     int            `json:"development_assignments"`
	ExcludedAssignments        int            `json:"excluded_assignments"`
	RejectedAssignments        int            `json:"rejected_assignments"`
	PendingAssignments         int            `json:"pending_assignments"`
	MissingAssignments         int            `json:"missing_assignments"`
	ExclusionCount             map[string]int `json:"exclusion_count"`
	FinalHoldoutIncluded       int            `json:"final_holdout_included"`
	FinalHoldoutExcluded       int            `json:"final_holdout_excluded"`
	FinalHoldoutSampleIDsShown bool           `json:"final_holdout_sample_ids_shown"`
}

type LayeredPerformanceReport struct {
	ID                   string                    `json:"id"`
	ContractVersion      string                    `json:"contract_version"`
	DatasetID            string                    `json:"dataset_id"`
	ExperimentID         string                    `json:"experiment_id"`
	AssetClass           string                    `json:"asset_class"`
	Market               string                    `json:"market"`
	Objective            string                    `json:"objective"`
	HorizonSessions      int                       `json:"horizon_sessions"`
	Research             ResearchPerformanceLayer  `json:"research"`
	Signal               SignalPerformanceLayer    `json:"signal"`
	Execution            ExecutionPerformanceLayer `json:"execution"`
	DataQuality          PerformanceDataQuality    `json:"data_quality"`
	FinalHoldoutAccessed bool                      `json:"final_holdout_accessed"`
	SelectionDecision    string                    `json:"selection_decision"`
	TraceLocations       map[string]string         `json:"trace_locations"`
	ArtifactDigest       string                    `json:"artifact_digest"`
}

func BuildLayeredPerformanceReport(dataset StoredDataset, experiment StoredExperiment, research ResearchPerformanceSummary, execution ExecutionPerformanceSummary) (LayeredPerformanceReport, error) {
	if dataset.Manifest.ID == "" || experiment.Experiment.ID == "" || experiment.Experiment.DatasetID != dataset.Manifest.ID {
		return LayeredPerformanceReport{}, fmt.Errorf("matching persisted dataset and experiment are required")
	}
	if dataset.Manifest.ContractVersion != WalkForwardDatasetContractVersion || experiment.Experiment.ContractVersion != DevelopmentExperimentContractVersion {
		return LayeredPerformanceReport{}, fmt.Errorf("unsupported dataset or experiment contract version")
	}
	if experiment.Experiment.AssetClass != dataset.AssetClass || experiment.Experiment.Market != dataset.Market ||
		experiment.Experiment.Objective != dataset.Objective || experiment.Experiment.HorizonSessions != dataset.HorizonSessions {
		return LayeredPerformanceReport{}, fmt.Errorf("dataset and experiment scope must match")
	}
	if len(experiment.Experiment.Folds) == 0 {
		return LayeredPerformanceReport{}, fmt.Errorf("development experiment folds are required")
	}
	if experiment.Experiment.FinalHoldoutAccessed {
		return LayeredPerformanceReport{}, fmt.Errorf("development performance report cannot use the final holdout")
	}
	report := LayeredPerformanceReport{ContractVersion: LayeredPerformanceReportContractVersion, DatasetID: dataset.Manifest.ID,
		ExperimentID: experiment.Experiment.ID, AssetClass: dataset.AssetClass, Market: dataset.Market, Objective: dataset.Objective,
		HorizonSessions: dataset.HorizonSessions, FinalHoldoutAccessed: false, SelectionDecision: "no_automatic_model_selection",
		TraceLocations: map[string]string{"dataset_manifest": "/go/evaluation-datasets/" + dataset.Manifest.ID,
			"experiment_samples_and_versions": "/go/evaluation-experiments/" + experiment.Experiment.ID,
			"exclusions":                      "/go/evaluation-datasets/" + dataset.Manifest.ID}}
	report.Research = buildResearchLayer(research)
	report.Signal = buildSignalLayer(experiment.Experiment)
	report.Execution = buildExecutionLayer(execution)
	assignments, excluded := 0, 0
	for _, fold := range dataset.Manifest.Folds {
		assignments += len(fold.Members)
		excluded += len(fold.Excluded)
	}
	pending, missing := categorizedExclusions(dataset.Manifest.ExclusionCount)
	report.DataQuality = PerformanceDataQuality{CandidatePredictions: dataset.Manifest.CandidateCount, DevelopmentAssignments: assignments,
		ExcludedAssignments: excluded, RejectedAssignments: excluded, PendingAssignments: pending, MissingAssignments: missing,
		ExclusionCount: stableReasonCounts(dataset.Manifest.ExclusionCount), FinalHoldoutIncluded: dataset.Manifest.FinalHoldout.IncludedCount,
		FinalHoldoutExcluded: dataset.Manifest.FinalHoldout.ExcludedCount, FinalHoldoutSampleIDsShown: false}
	report.ArtifactDigest = digestValue(struct {
		Contract, Dataset, Experiment string
		Research                      ResearchPerformanceLayer
		Signal                        SignalPerformanceLayer
		Execution                     ExecutionPerformanceLayer
		DataQuality                   PerformanceDataQuality
		Selection                     string
		Trace                         map[string]string
	}{report.ContractVersion, report.DatasetID, report.ExperimentID, report.Research, report.Signal, report.Execution, report.DataQuality, report.SelectionDecision, report.TraceLocations})
	report.ID = "performance-" + report.ArtifactDigest[:32]
	return report, nil
}

func buildResearchLayer(summary ResearchPerformanceSummary) ResearchPerformanceLayer {
	result := ResearchPerformanceLayer{Status: "unavailable", CandidatePredictions: summary.CandidatePredictions,
		LinkedResearchRuns: summary.LinkedResearchRuns, Completed: summary.Completed, InsufficientEvidence: summary.InsufficientEvidence,
		TechnicalFailures: summary.TechnicalFailures, MissingResearch: summary.MissingResearch, MeanLatencySeconds: summary.MeanLatencySeconds,
		ReviewedPolicyEvaluations: summary.ReviewedPolicyEvaluations, AcceptedPolicyEvaluations: summary.AcceptedPolicyEvaluations,
		DimensionMetrics: map[string]ResearchDimensionMetric{}, MissingReasons: map[string]int{}}
	if summary.CandidatePredictions > 0 {
		coverage := float64(summary.LinkedResearchRuns) / float64(summary.CandidatePredictions)
		refusal := float64(summary.InsufficientEvidence) / float64(summary.CandidatePredictions)
		result.Coverage, result.RefusalRate = &coverage, &refusal
	}
	if summary.ReviewedPolicyEvaluations > 0 {
		accuracy := float64(summary.AcceptedPolicyEvaluations) / float64(summary.ReviewedPolicyEvaluations)
		result.CompositeReviewAccuracy = &accuracy
	}
	result.DimensionMetrics["fact_accuracy"] = researchDimension(summary.FactCorrect, summary.FactReviewed)
	result.DimensionMetrics["relationship_accuracy"] = researchDimension(summary.RelationshipCorrect, summary.RelationshipReviewed)
	result.DimensionMetrics["citation_support_accuracy"] = researchDimension(summary.CitationCorrect, summary.CitationReviewed)
	result.DimensionMetrics["refusal_appropriateness"] = researchDimension(summary.RefusalAppropriate, summary.RefusalReviewed)
	if summary.MissingResearch > 0 {
		result.MissingReasons["research_run_unavailable"] = summary.MissingResearch
	}
	if summary.TechnicalFailures > 0 {
		result.MissingReasons["technical_failure"] = summary.TechnicalFailures
	}
	if summary.InsufficientEvidence > 0 {
		result.MissingReasons["insufficient_evidence"] = summary.InsufficientEvidence
	}
	if summary.LinkedResearchRuns > 0 {
		result.Status = "partially_observed"
	}
	if summary.LinkedResearchRuns > 0 && summary.ReviewedPolicyEvaluations > 0 {
		result.Status = "composite_review_available"
	}
	return result
}

func researchDimension(correct, reviewed int) ResearchDimensionMetric {
	result := ResearchDimensionMetric{Status: "unavailable", Reason: "dimension_specific_human_labels_unavailable", Correct: correct, Reviewed: reviewed}
	if reviewed < 1 {
		return result
	}
	accuracy := float64(correct) / float64(reviewed)
	low, high := performanceWilson(correct, reviewed)
	result.Status, result.Reason, result.Accuracy, result.AccuracyLow95, result.AccuracyHigh95 = "available", "", &accuracy, &low, &high
	return result
}

func buildSignalLayer(experiment DevelopmentExperiment) SignalPerformanceLayer {
	result := SignalPerformanceLayer{Status: "available", ExperimentID: experiment.ID,
		SampleUnit: "prediction_run_grouped_by_connected_event_cluster", CorrelationHandling: "event_cluster_isolated_and_reported_per_time_fold_without_pooled_significance",
		PooledReport: false, Folds: []SignalFoldPerformance{}}
	for _, fold := range experiment.Folds {
		item := SignalFoldPerformance{FoldIndex: fold.Index, Variants: []SignalVariantPerformance{}}
		for _, variant := range fold.Variants {
			performance := SignalVariantPerformance{Name: variant.Name, Status: variant.Status, Reason: variant.Reason,
				SampleCount: variant.Metrics.SampleCount, Accuracy: variant.Metrics.Accuracy, PositiveRate: variant.Metrics.PositiveRate,
				PredictedPositive: variant.Metrics.PredictedPositive, RankIC: variant.Metrics.RankIC, Probability: variant.Metrics.Probability,
				DirectionReturns: directionReturnBuckets(variant.Predictions), SmallSampleWarning: variant.Metrics.SampleCount < 30,
				ArtifactDigest: variant.ArtifactDigest}
			if variant.Metrics.Accuracy != nil && variant.Metrics.SampleCount > 0 {
				low, high := performanceWilson(variant.Metrics.Correct, variant.Metrics.SampleCount)
				performance.AccuracyLow95, performance.AccuracyHigh95 = &low, &high
			}
			item.Variants = append(item.Variants, performance)
		}
		result.Folds = append(result.Folds, item)
	}
	return result
}

func directionReturnBuckets(predictions []VariantPrediction) []DirectionReturnBucket {
	type accumulator struct {
		count       int
		raw, excess []float64
	}
	values := map[string]*accumulator{"non_positive": {}, "positive": {}}
	for _, prediction := range predictions {
		key := "non_positive"
		if prediction.RawScore > 0 {
			key = "positive"
		}
		values[key].count++
		if prediction.RawReturn != nil {
			values[key].raw = append(values[key].raw, *prediction.RawReturn)
		}
		if prediction.ExcessReturn != nil {
			values[key].excess = append(values[key].excess, *prediction.ExcessReturn)
		}
	}
	result := []DirectionReturnBucket{}
	for _, key := range []string{"non_positive", "positive"} {
		result = append(result, DirectionReturnBucket{Direction: key, Count: values[key].count, MeanRawReturn: meanPointer(values[key].raw), MeanExcessReturn: meanPointer(values[key].excess)})
	}
	return result
}

func buildExecutionLayer(summary ExecutionPerformanceSummary) ExecutionPerformanceLayer {
	result := ExecutionPerformanceLayer{Status: "unavailable", Reason: "strategy_and_cost_model_unavailable", TestPredictions: summary.TestPredictions,
		ExecutableSamples: summary.Executable, MeanRawReturn: summary.MeanRawReturn, MeanExcessReturn: summary.MeanExcessReturn,
		TurnoverStatus: "unavailable_without_portfolio_weights", DrawdownStatus: "unavailable_without_strategy_equity_curve",
		ExposureStatus: "unavailable_without_portfolio_positions", ResearchResultOnly: true}
	if summary.Executable > 0 && summary.MeanNetReturn != nil {
		result.Status, result.Reason, result.MeanNetReturn, result.ResearchResultOnly = "trade_level_net_returns_available", "portfolio_metrics_not_configured", summary.MeanNetReturn, false
	}
	return result
}

func performanceWilson(positive, total int) (float64, float64) {
	if total < 1 {
		return 0, 0
	}
	z := 1.959963984540054
	n, p := float64(total), float64(positive)/float64(total)
	denominator := 1 + z*z/n
	center := (p + z*z/(2*n)) / denominator
	margin := z * math.Sqrt((p*(1-p)+z*z/(4*n))/n) / denominator
	return math.Max(0, center-margin), math.Min(1, center+margin)
}

func stableReasonCounts(values map[string]int) map[string]int {
	result := map[string]int{}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result[key] = values[key]
	}
	return result
}

func categorizedExclusions(values map[string]int) (pending, missing int) {
	for reason, count := range values {
		lower := strings.ToLower(reason)
		if strings.Contains(lower, "pending") || strings.Contains(lower, "not_mature") {
			pending += count
		}
		if strings.Contains(lower, "missing") || strings.Contains(lower, "unavailable") {
			missing += count
		}
	}
	return pending, missing
}
