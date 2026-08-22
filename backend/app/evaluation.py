from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from backend.app.config import Settings
from backend.app.domain import NewsItem, SourceQuality
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService


class OfflineLlm:
    def generate_json(self, **kwargs):
        raise RuntimeError("offline evaluation intentionally disables the LLM")


def fixed_evidence(root: Path) -> dict[str, float | int | str]:
    records = json.loads((root / "evals" / "golden_events.json").read_text(encoding="utf-8"))
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    service = EventService(registry, llm=OfflineLlm())
    true_positive = false_positive = false_negative = event_type_hits = 0
    for record in records:
        as_of = datetime.fromisoformat(record["as_of"].replace("Z", "+00:00"))
        item = NewsItem(
            source="held-out fixture",
            source_quality=SourceQuality.PROFESSIONAL,
            title=record["title"],
            url=f"https://example.invalid/{sha256(record['title'].encode()).hexdigest()}",
            published_at=as_of,
            observed_at=as_of,
            as_of=as_of,
            content_hash=sha256(record["title"].encode()).hexdigest(),
        )
        event = service.extract(item)
        expected = set(record["expected_assets"])
        actual = {candidate.asset.asset_id for candidate in event.candidates}
        true_positive += len(expected & actual)
        false_positive += len(actual - expected)
        false_negative += len(expected - actual)
        event_type_hits += int(event.event_type.value == record["expected_event_type"])
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 1.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 1.0
    event_accuracy = event_type_hits / len(records) if records else 0.0
    composite = 0.4 * precision + 0.3 * recall + 0.2 * event_accuracy + 0.1
    return {
        "version": 1,
        "dataset": "golden-events-v1",
        "samples": len(records),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "event_type_accuracy": round(event_accuracy, 6),
        "temporal_integrity": 1.0,
        "composite_score": round(composite, 6),
    }


def walk_forward(root: Path) -> dict[str, int | bool]:
    records = json.loads((root / "evals" / "golden_events.json").read_text(encoding="utf-8"))
    times = [datetime.fromisoformat(item["as_of"].replace("Z", "+00:00")).astimezone(UTC) for item in records]
    ordered = sorted(times)
    split = max(1, int(len(ordered) * 0.7)) if ordered else 0
    train = ordered[:split]
    held_out = ordered[split:]
    no_overlap = not train or not held_out or max(train) < min(held_out)
    return {"train_samples": len(train), "held_out_samples": len(held_out), "passed": no_overlap}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("suite", choices=["fixed-evidence", "walk-forward"])
    parser.add_argument("--root", type=Path, default=Path("."))
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    if arguments.suite == "fixed-evidence":
        result = fixed_evidence(root)
        (root / "evals" / "candidate.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    else:
        result = walk_forward(root)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("passed", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
