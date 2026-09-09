package evaluation

import "testing"

func TestLayeredPerformanceReportSeparatesLayersAndUncertainty(t *testing.T) {
	rawPositive, excessPositive := .08, .03
	rawNegative, excessNegative := -.04, -.02
	accuracy := .5
	dataset := StoredDataset{
		AssetClass: "equity", Market: "US", Objective: "excess_up", HorizonSessions: 5,
		Manifest: DatasetManifest{ID: "dataset-report", ContractVersion: WalkForwardDatasetContractVersion,
			CandidateCount: 6, ManifestDigest: "manifest", Folds: []Fold{{Index: 0}},
			FinalHoldout: FinalHoldoutDescriptor{IncludedCount: 2, ExcludedCount: 1}, ExclusionCount: map[string]int{"missing_feature": 1}},
	}
	experiment := StoredExperiment{Experiment: DevelopmentExperiment{
		ID: "experiment-report", ContractVersion: DevelopmentExperimentContractVersion, DatasetID: dataset.Manifest.ID,
		AssetClass: dataset.AssetClass, Market: dataset.Market, Objective: dataset.Objective, HorizonSessions: dataset.HorizonSessions,
		Folds: []ExperimentFold{{Index: 0, Variants: []ExperimentVariant{{Name: "simple_rule", Status: "evaluated", ArtifactDigest: "variant",
			Metrics: VariantMetrics{SampleCount: 2, Correct: 1, Accuracy: &accuracy}, Predictions: []VariantPrediction{
				{RawScore: 1, RawReturn: &rawPositive, ExcessReturn: &excessPositive},
				{RawScore: -1, RawReturn: &rawNegative, ExcessReturn: &excessNegative},
			}}}}},
	}}
	latency := 4.0
	report, err := BuildLayeredPerformanceReport(dataset, experiment,
		ResearchPerformanceSummary{CandidatePredictions: 6, LinkedResearchRuns: 4, Completed: 2, InsufficientEvidence: 1,
			TechnicalFailures: 1, MissingResearch: 2, MeanLatencySeconds: &latency, ReviewedPolicyEvaluations: 2, AcceptedPolicyEvaluations: 1},
		ExecutionPerformanceSummary{TestPredictions: 2, MeanRawReturn: &rawPositive})
	if err != nil {
		t.Fatal(err)
	}
	if report.FinalHoldoutAccessed || report.SelectionDecision != "no_automatic_model_selection" || report.ArtifactDigest == "" {
		t.Fatalf("unsafe report contract: %#v", report)
	}
	if report.Research.Coverage == nil || *report.Research.Coverage != 4.0/6.0 || report.Research.DimensionMetrics["fact_accuracy"].Status != "unavailable" {
		t.Fatalf("research coverage or explicit missing dimensions are absent: %#v", report.Research)
	}
	variant := report.Signal.Folds[0].Variants[0]
	if !variant.SmallSampleWarning || variant.AccuracyLow95 == nil || variant.AccuracyHigh95 == nil || len(variant.DirectionReturns) != 2 {
		t.Fatalf("signal uncertainty or grouped returns are absent: %#v", variant)
	}
	if variant.DirectionReturns[0].Count != 1 || variant.DirectionReturns[1].Count != 1 {
		t.Fatalf("direction buckets do not disclose every prediction: %#v", variant.DirectionReturns)
	}
	if report.Execution.Status != "unavailable" || report.Execution.DrawdownStatus != "unavailable_without_strategy_equity_curve" || !report.Execution.ResearchResultOnly {
		t.Fatalf("asset returns were misrepresented as strategy performance: %#v", report.Execution)
	}
}

func TestLayeredPerformanceReportRejectsScopeMismatchAndHoldoutUse(t *testing.T) {
	dataset := StoredDataset{AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		Manifest: DatasetManifest{ID: "dataset-report", ContractVersion: WalkForwardDatasetContractVersion, Folds: []Fold{{Index: 0}}}}
	experiment := StoredExperiment{Experiment: DevelopmentExperiment{ID: "experiment-report", ContractVersion: DevelopmentExperimentContractVersion,
		DatasetID: dataset.Manifest.ID, AssetClass: "equity", Market: "CN", Objective: dataset.Objective, HorizonSessions: 1,
		Folds: []ExperimentFold{{Index: 0}}}}
	if _, err := BuildLayeredPerformanceReport(dataset, experiment, ResearchPerformanceSummary{}, ExecutionPerformanceSummary{}); err == nil {
		t.Fatal("scope mismatch was accepted")
	}
	experiment.Experiment.Market = dataset.Market
	experiment.Experiment.FinalHoldoutAccessed = true
	if _, err := BuildLayeredPerformanceReport(dataset, experiment, ResearchPerformanceSummary{}, ExecutionPerformanceSummary{}); err == nil {
		t.Fatal("final holdout access was accepted")
	}
}
