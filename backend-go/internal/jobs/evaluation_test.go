package jobs

import (
	"path/filepath"
	"testing"
)

func evaluationRepositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestFrozenEvidenceEvaluationMatchesBaseline(t *testing.T) {
	root := evaluationRepositoryRoot(t)
	result, err := RunOfflineEvaluation(root, "fixed-evidence", "", "")
	if err != nil {
		t.Fatal(err)
	}
	baseline := map[string]any{}
	if err := readEvaluationJSON(filepath.Join(root, "evals", "baseline.json"), &baseline); err != nil {
		t.Fatal(err)
	}
	comparison := compareEvaluationMetrics(baseline, result, 0)
	if !EvaluationPassed(result) || !EvaluationPassed(comparison) {
		t.Fatalf("Go evaluator diverged from the frozen baseline: result=%v comparison=%v", result, comparison)
	}
	if got := numberValue(result["composite_score"]); got != 0.9875 {
		t.Fatalf("composite_score=%v want 0.9875", got)
	}
}

func TestChronologicalHoldoutAndProbabilitySuitesHaveAccurateSemantics(t *testing.T) {
	root := evaluationRepositoryRoot(t)
	walkForward, err := RunOfflineEvaluation(root, "chronological_holdout", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !EvaluationPassed(walkForward) || int(numberValue(walkForward["held_out_samples"])) != 4 {
		t.Fatalf("unexpected chronological holdout result: %v", walkForward)
	}
	if stringValue(walkForward["evaluation_type"]) != "chronological_holdout" || stringValue(walkForward["prediction_evaluation"]) != "skipped" {
		t.Fatalf("holdout incorrectly presented itself as a predictive walk-forward suite: %v", walkForward)
	}
	probability, err := RunOfflineEvaluation(root, "probability-calibration", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !EvaluationPassed(probability) || stringValue(probability["status"]) != "skipped" || stringValue(probability["reason"]) != "uncalibrated_predictions" {
		t.Fatalf("unexpected probability result: %v", probability)
	}
	_, hasBrier := probability["brier_score"]
	_, hasECE := probability["expected_calibration_error"]
	if hasBrier || hasECE {
		t.Fatalf("uncalibrated probability evaluation must not publish calibration metrics: %v", probability)
	}
}

func TestOfflineEvaluationRejectsDeprecatedWalkForwardAlias(t *testing.T) {
	_, err := RunOfflineEvaluation(evaluationRepositoryRoot(t), "walk-forward", "", "")
	if err == nil {
		t.Fatal("deprecated walk-forward alias was accepted")
	}
}

func TestFrozenResearchQualitySuitePasses(t *testing.T) {
	result, err := RunOfflineEvaluation(evaluationRepositoryRoot(t), "research-quality", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !EvaluationPassed(result) || int(numberValue(result["samples"])) != 7 || numberValue(result["quality_gate_accuracy"]) != 1 {
		t.Fatalf("unexpected research quality result: %v", result)
	}
}

func TestCompareEvaluationMetricsTreatsCalibrationErrorsAsLowerIsBetter(t *testing.T) {
	baseline := map[string]any{"dataset": "baseline", "brier_score": .2, "mapping_precision": .9, "passed": true}
	candidate := map[string]any{"dataset": "candidate", "brier_score": .15, "mapping_precision": .92, "passed": true}
	if result := compareEvaluationMetrics(baseline, candidate, 0); !EvaluationPassed(result) {
		t.Fatalf("improved candidate was rejected: %v", result)
	}
	candidate["brier_score"] = .21
	if result := compareEvaluationMetrics(baseline, candidate, 0); EvaluationPassed(result) {
		t.Fatalf("regressed candidate was accepted: %v", result)
	}
}
