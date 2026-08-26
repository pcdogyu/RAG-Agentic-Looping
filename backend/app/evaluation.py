from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite
from pathlib import Path
from typing import Any

from backend.app.config import Settings
from backend.app.domain import AssetRef, NewsEvent, NewsItem, SourceQuality
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.directional_scoring import (
    calibrate_probabilities,
    deterministic_direction_score,
    gated_score,
    probabilities_for_score,
)
from backend.app.services.events import EventService


class OfflineLlm:
    def generate_json(self, **kwargs):
        raise RuntimeError("offline evaluation intentionally disables the LLM")


def _load_dataset(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads((root / "evals" / "golden_events.json").read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return (
            {
                "version": 1,
                "dataset": "golden-events-v1",
                "assets": [],
                "minimums": {},
            },
            payload,
        )
    if not isinstance(payload, dict) or not isinstance(payload.get("cases"), list):
        raise ValueError("evals/golden_events.json must contain a list or a cases object")
    return payload, payload["cases"]


def _offline_registry(metadata: dict[str, Any]) -> ProviderRegistry:
    assets = [AssetRef.model_validate(item) for item in metadata.get("assets", [])]
    registry = ProviderRegistry(
        Settings(fmp_access_token="", fmp_mcp_url="", akshare_asset_master_enabled=False),
        assets=assets,
    )
    # A frozen evaluation must never gain or lose candidates because a live provider
    # or network endpoint changed. ProviderRegistry's real deterministic matching path
    # still runs against the frozen asset universe.
    registry.fmp.resolve_assets = lambda _query: []
    registry.crypto.resolve_assets = lambda _query: []
    registry._source_enabled = lambda _name, default=True: default
    return registry


def _news_item(record: dict[str, Any]) -> NewsItem:
    as_of = datetime.fromisoformat(record["as_of"].replace("Z", "+00:00"))
    digest = sha256(str(record.get("id") or record["title"]).encode()).hexdigest()
    return NewsItem(
        source="held-out fixture",
        source_quality=SourceQuality.PROFESSIONAL,
        title=record["title"],
        summary=record.get("summary", ""),
        symbols=record.get("symbols", []),
        url=f"https://example.invalid/{digest}",
        published_at=as_of,
        observed_at=as_of,
        as_of=as_of,
        content_hash=digest,
    )


def _safe_ratio(numerator: int, denominator: int, *, empty: float = 1.0) -> float:
    return numerator / denominator if denominator else empty


def _stage_metrics(
    records: Iterable[dict[str, Any]],
    *,
    metadata: dict[str, Any],
) -> dict[str, float | int]:
    records = list(records)
    service = EventService(_offline_registry(metadata), llm=OfflineLlm())
    true_positive = false_positive = false_negative = 0
    exact_asset_hits = event_type_hits = temporal_hits = 0
    evaluated: list[tuple[dict[str, Any], NewsEvent]] = []

    for record in records:
        item = _news_item(record)
        event = service.extract(item)
        expected = set(record["expected_assets"])
        actual = {candidate.asset.asset_id for candidate in event.candidates}
        true_positive += len(expected & actual)
        false_positive += len(actual - expected)
        false_negative += len(expected - actual)
        exact_asset_hits += int(actual == expected)
        event_type_hits += int(event.event_type.value == record["expected_event_type"])
        temporal_hits += int(
            event.published_at <= event.as_of
            and event.observed_at <= event.as_of
            and item.as_of == event.as_of
        )
        evaluated.append((record, event))

    cluster_true_positive = cluster_false_positive = cluster_false_negative = 0
    for index, (left_record, left_event) in enumerate(evaluated):
        for right_record, right_event in evaluated[index + 1 :]:
            left_cluster = left_record.get("cluster_id")
            expected_same = bool(left_cluster) and left_cluster == right_record.get("cluster_id")
            predicted_same = service._same_story(left_event, right_event)
            if expected_same and predicted_same:
                cluster_true_positive += 1
            elif predicted_same:
                cluster_false_positive += 1
            elif expected_same:
                cluster_false_negative += 1

    precision = _safe_ratio(true_positive, true_positive + false_positive)
    recall = _safe_ratio(true_positive, true_positive + false_negative)
    exact_asset_accuracy = _safe_ratio(exact_asset_hits, len(records), empty=0.0)
    event_accuracy = _safe_ratio(event_type_hits, len(records), empty=0.0)
    temporal_integrity = _safe_ratio(temporal_hits, len(records), empty=0.0)
    cluster_precision = _safe_ratio(
        cluster_true_positive,
        cluster_true_positive + cluster_false_positive,
    )
    cluster_recall = _safe_ratio(
        cluster_true_positive,
        cluster_true_positive + cluster_false_negative,
    )
    cluster_f1 = (
        2 * cluster_precision * cluster_recall / (cluster_precision + cluster_recall)
        if cluster_precision + cluster_recall
        else 0.0
    )
    composite = (
        0.25 * precision
        + 0.20 * recall
        + 0.15 * exact_asset_accuracy
        + 0.15 * event_accuracy
        + 0.15 * cluster_f1
        + 0.10 * temporal_integrity
    )
    return {
        "samples": len(records),
        "mapping_precision": round(precision, 6),
        "mapping_recall": round(recall, 6),
        "exact_asset_accuracy": round(exact_asset_accuracy, 6),
        "event_type_accuracy": round(event_accuracy, 6),
        "cluster_precision": round(cluster_precision, 6),
        "cluster_recall": round(cluster_recall, 6),
        "temporal_integrity": round(temporal_integrity, 6),
        "composite_score": round(composite, 6),
    }


def _meets_minimums(metrics: dict[str, Any], minimums: dict[str, Any]) -> bool:
    return all(
        isinstance(metrics.get(name), int | float)
        and not isinstance(metrics.get(name), bool)
        and float(metrics[name]) >= float(minimum)
        for name, minimum in minimums.items()
    )


def _probability_records(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(
        (root / "evals" / "golden_predictions.json").read_text(encoding="utf-8")
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("cases"), list):
        raise ValueError("evals/golden_predictions.json must contain a cases object")
    return payload, payload["cases"]


def probability_calibration(root: Path) -> dict[str, float | int | str | bool]:
    metadata, records = _probability_records(root)
    labels = ("bull", "base", "bear")
    actual_counts = {label: 0 for label in labels}
    forecasts: list[tuple[tuple[float, float, float], int, float, bool]] = []
    score_consistency_hits = 0

    for record in records:
        model_probabilities = tuple(
            float(record.get(f"model_{label}_probability", record[f"{label}_probability"]))
            for label in labels
        )
        evidence_strength = float(record.get("evidence_strength", 1))
        mapping_confidence = float(record.get("mapping_confidence", 1))
        calibrated = calibrate_probabilities(
            *model_probabilities,
            reliability=evidence_strength * mapping_confidence,
        )
        direction = deterministic_direction_score(
            bull_probability=calibrated[0],
            base_probability=calibrated[1],
            bear_probability=calibrated[2],
            event_direction=record.get("event_direction"),
            event_relevance=float(record.get("event_relevance", 0)),
            factor_signal=record.get("factor_signal"),
            factor_reliability=float(record.get("factor_reliability", 0)),
        )
        program_score = gated_score(
            direction.raw_score,
            evidence_strength,
            mapping_confidence,
        )
        probabilities = probabilities_for_score(
            program_score,
            base_probability=calibrated[1],
        )
        if (
            any(not isfinite(value) or value < 0 or value > 1 for value in probabilities)
            or abs(sum(probabilities) - 1) > 0.001
        ):
            raise ValueError(f"invalid probability vector in frozen case {record.get('id')}")
        try:
            actual_index = labels.index(record["actual"])
        except ValueError as exc:
            raise ValueError(f"invalid actual label in frozen case {record.get('id')}") from exc
        actual_counts[labels[actual_index]] += 1
        predicted_index = max(range(3), key=probabilities.__getitem__)
        confidence = probabilities[predicted_index]
        forecasts.append(
            (probabilities, actual_index, confidence, predicted_index == actual_index)
        )
        implied_score = round(100 * (probabilities[0] - probabilities[2]))
        score_consistency_hits += int(
            abs(int(record["score"]) - program_score) <= 5
            and abs(program_score - implied_score) <= 1
        )

    samples = len(forecasts)
    if not samples:
        raise ValueError("frozen probability evaluation requires at least one case")
    brier_score = sum(
        sum(
            (forecast - float(index == actual_index)) ** 2
            for index, forecast in enumerate(probabilities)
        )
        / 3
        for probabilities, actual_index, _, _ in forecasts
    ) / samples
    class_frequencies = tuple(actual_counts[label] / samples for label in labels)
    reference_brier = sum(
        sum(
            (frequency - float(index == actual_index)) ** 2
            for index, frequency in enumerate(class_frequencies)
        )
        / 3
        for _, actual_index, _, _ in forecasts
    ) / samples
    brier_skill = 1 - brier_score / reference_brier if reference_brier > 0 else 0.0

    bins: dict[int, list[tuple[float, bool]]] = {}
    for _, _, confidence, correct in forecasts:
        bucket = min(9, int(confidence * 10))
        bins.setdefault(bucket, []).append((confidence, correct))
    expected_calibration_error = sum(
        len(items)
        / samples
        * abs(
            sum(float(correct) for _, correct in items) / len(items)
            - sum(confidence for confidence, _ in items) / len(items)
        )
        for items in bins.values()
    )
    top_label_accuracy = sum(int(correct) for _, _, _, correct in forecasts) / samples
    score_probability_consistency = score_consistency_hits / samples
    result: dict[str, float | int | str | bool] = {
        "version": int(metadata.get("version", 1)),
        "dataset": str(metadata.get("dataset", "golden-predictions")),
        "samples": samples,
        "brier_score": round(brier_score, 6),
        "reference_brier_score": round(reference_brier, 6),
        "brier_skill": round(brier_skill, 6),
        "expected_calibration_error": round(expected_calibration_error, 6),
        "top_label_accuracy": round(top_label_accuracy, 6),
        "score_probability_consistency": round(score_probability_consistency, 6),
    }
    result["passed"] = _meets_minimums(result, metadata.get("minimums", {})) and all(
        float(result[name]) <= float(maximum)
        for name, maximum in metadata.get("maximums", {}).items()
    )
    return result


def fixed_evidence(root: Path) -> dict[str, float | int | str | bool]:
    metadata, records = _load_dataset(root)
    metrics = _stage_metrics(records, metadata=metadata)
    stage_composite = float(metrics.pop("composite_score"))
    calibration = probability_calibration(root)
    calibration_composite = (
        max(0.0, float(calibration["brier_skill"]))
        + (1 - float(calibration["expected_calibration_error"]))
        + float(calibration["top_label_accuracy"])
        + float(calibration["score_probability_consistency"])
    ) / 4
    result: dict[str, float | int | str | bool] = {
        "version": int(metadata.get("version", 1)),
        "dataset": str(metadata.get("dataset", "golden-events")),
        **metrics,
        "stage_composite_score": round(stage_composite, 6),
        "calibration_samples": int(calibration["samples"]),
        "brier_score": float(calibration["brier_score"]),
        "reference_brier_score": float(calibration["reference_brier_score"]),
        "brier_skill": float(calibration["brier_skill"]),
        "expected_calibration_error": float(calibration["expected_calibration_error"]),
        "top_label_accuracy": float(calibration["top_label_accuracy"]),
        "score_probability_consistency": float(
            calibration["score_probability_consistency"]
        ),
        "composite_score": round(0.8 * stage_composite + 0.2 * calibration_composite, 6),
    }
    result["passed"] = _meets_minimums(result, metadata.get("minimums", {})) and bool(
        calibration["passed"]
    )
    return result


def walk_forward(root: Path) -> dict[str, Any]:
    metadata, records = _load_dataset(root)
    ordered = sorted(
        records,
        key=lambda item: datetime.fromisoformat(item["as_of"].replace("Z", "+00:00")).astimezone(
            UTC
        ),
    )
    split = max(1, int(len(ordered) * 0.7)) if ordered else 0
    train = ordered[:split]
    held_out = ordered[split:]
    train_times = [
        datetime.fromisoformat(item["as_of"].replace("Z", "+00:00")).astimezone(UTC)
        for item in train
    ]
    held_out_times = [
        datetime.fromisoformat(item["as_of"].replace("Z", "+00:00")).astimezone(UTC)
        for item in held_out
    ]
    no_overlap = not train_times or not held_out_times or max(train_times) < min(held_out_times)
    train_metrics = _stage_metrics(train, metadata=metadata) if train else {"samples": 0}
    held_out_metrics = _stage_metrics(held_out, metadata=metadata) if held_out else {"samples": 0}
    held_out_passed = bool(held_out) and _meets_minimums(
        held_out_metrics, metadata.get("walk_forward_minimums", metadata.get("minimums", {}))
    )
    return {
        "version": int(metadata.get("version", 1)),
        "dataset": str(metadata.get("dataset", "golden-events")),
        "train_samples": len(train),
        "held_out_samples": len(held_out),
        "chronological_split": no_overlap,
        "train_metrics": train_metrics,
        "held_out_metrics": held_out_metrics,
        "passed": no_overlap and held_out_passed,
    }


def compare_model_metrics(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    tolerance: float = 0.01,
) -> dict[str, Any]:
    ignored = {"version", "samples", "passed"}
    lower_is_better = {"brier_score", "expected_calibration_error"}
    common = sorted(
        key
        for key, value in baseline.items()
        if key not in ignored
        and isinstance(value, int | float)
        and not isinstance(value, bool)
        and isinstance(candidate.get(key), int | float)
        and not isinstance(candidate.get(key), bool)
    )
    raw_deltas = {key: float(candidate[key]) - float(baseline[key]) for key in common}
    deltas = {key: round(delta, 6) for key, delta in raw_deltas.items()}
    regressions = [
        key
        for key in common
        if (
            raw_deltas[key] > tolerance
            if key in lower_is_better
            else raw_deltas[key] < -tolerance
        )
    ]
    return {
        "baseline_dataset": baseline.get("dataset", "unknown"),
        "candidate_dataset": candidate.get("dataset", "unknown"),
        "tolerance": tolerance,
        "metrics_compared": len(common),
        "deltas": deltas,
        "regressions": regressions,
        "passed": bool(common) and not regressions and bool(candidate.get("passed", True)),
    }


def compare_models(root: Path, baseline_path: Path, candidate_path: Path) -> dict[str, Any]:
    baseline = json.loads((root / baseline_path).read_text(encoding="utf-8"))
    candidate = json.loads((root / candidate_path).read_text(encoding="utf-8"))
    return compare_model_metrics(baseline, candidate)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "suite",
        choices=[
            "fixed-evidence",
            "walk-forward",
            "probability-calibration",
            "compare-models",
        ],
    )
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--baseline", type=Path, default=Path("evals/baseline.json"))
    parser.add_argument("--candidate", type=Path, default=Path("evals/candidate.json"))
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    if arguments.suite == "fixed-evidence":
        result = fixed_evidence(root)
        (root / "evals" / "candidate.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    elif arguments.suite == "walk-forward":
        result = walk_forward(root)
    elif arguments.suite == "probability-calibration":
        result = probability_calibration(root)
    else:
        result = compare_models(root, arguments.baseline, arguments.candidate)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("passed", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
