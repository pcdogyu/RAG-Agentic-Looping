from __future__ import annotations

from pathlib import Path

from backend.app.evaluation import (
    _meets_minimums,
    compare_model_metrics,
    fixed_evidence,
    probability_calibration,
    walk_forward,
)

ROOT = Path(__file__).resolve().parents[2]


def test_frozen_pipeline_evaluates_real_mapping_and_clustering_stages():
    result = fixed_evidence(ROOT)

    assert result["dataset"] == "frozen-pipeline-v2"
    assert result["samples"] >= 10
    assert result["mapping_precision"] == 1.0
    assert result["mapping_recall"] == 1.0
    assert result["cluster_precision"] == 1.0
    assert result["cluster_recall"] == 1.0
    assert result["passed"] is True


def test_walk_forward_replays_the_actual_stages_on_held_out_cases():
    result = walk_forward(ROOT)

    assert result["chronological_split"] is True
    assert result["train_samples"] > result["held_out_samples"] > 0
    assert result["held_out_metrics"]["samples"] == result["held_out_samples"]
    assert result["held_out_metrics"]["mapping_precision"] == 1.0
    assert result["passed"] is True


def test_probability_calibration_scores_brier_skill_and_ece():
    result = probability_calibration(ROOT)

    assert result["samples"] == 10
    assert result["brier_score"] == 0.113333
    assert result["brier_skill"] > 0
    assert result["expected_calibration_error"] == 0
    assert result["top_label_accuracy"] == 0.8
    assert result["score_probability_consistency"] == 1.0
    assert result["passed"] is True


def test_boolean_is_not_accepted_as_a_numeric_metric():
    assert _meets_minimums({"accuracy": True}, {"accuracy": 0.9}) is False


def test_model_comparison_allows_noise_but_rejects_material_regression():
    baseline = {
        "dataset": "champion",
        "mapping_precision": 0.99,
        "mapping_recall": 0.98,
        "composite_score": 0.95,
        "passed": True,
    }
    acceptable = {
        "dataset": "challenger",
        "mapping_precision": 0.985,
        "mapping_recall": 0.99,
        "composite_score": 0.96,
        "passed": True,
    }
    regressed = {
        **acceptable,
        "mapping_precision": 0.95,
    }

    accepted = compare_model_metrics(baseline, acceptable)
    rejected = compare_model_metrics(baseline, regressed)

    assert accepted["passed"] is True
    assert accepted["regressions"] == []
    assert rejected["passed"] is False
    assert rejected["regressions"] == ["mapping_precision"]


def test_model_comparison_knows_that_brier_and_ece_are_lower_is_better():
    baseline = {
        "brier_score": 0.10,
        "expected_calibration_error": 0.04,
    }

    improved = compare_model_metrics(
        baseline,
        {"brier_score": 0.08, "expected_calibration_error": 0.03},
    )
    regressed = compare_model_metrics(
        baseline,
        {"brier_score": 0.13, "expected_calibration_error": 0.06},
    )

    assert improved["passed"] is True
    assert regressed["passed"] is False
    assert regressed["regressions"] == ["brier_score", "expected_calibration_error"]


def test_model_comparison_rejects_a_candidate_that_failed_its_stage_gate():
    result = compare_model_metrics(
        {"dataset": "champion", "composite_score": 0.9},
        {"dataset": "challenger", "composite_score": 0.95, "passed": False},
    )

    assert result["passed"] is False
