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
	if got := numberValue(result["composite_score"]); got != 0.954242 {
		t.Fatalf("composite_score=%v want 0.954242", got)
	}
}

func TestFrozenWalkForwardAndProbabilitySuitesPass(t *testing.T) {
	root := evaluationRepositoryRoot(t)
	walkForward, err := RunOfflineEvaluation(root, "walk-forward", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !EvaluationPassed(walkForward) || int(numberValue(walkForward["held_out_samples"])) != 4 {
		t.Fatalf("unexpected walk-forward result: %v", walkForward)
	}
	probability, err := RunOfflineEvaluation(root, "probability-calibration", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !EvaluationPassed(probability) || numberValue(probability["brier_score"]) != 0.113333 {
		t.Fatalf("unexpected probability result: %v", probability)
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
