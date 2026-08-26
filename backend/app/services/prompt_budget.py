from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

QUALITY_PRIORITY = {
    "official": 0,
    "primary": 1,
    "professional": 2,
    "aggregator": 3,
    "social": 4,
}
EVIDENCE_FIELDS = (
    "id",
    "claim",
    "source_name",
    "source_url",
    "source_quality",
    "published_at",
    "observed_at",
    "as_of",
    "excerpt",
    "independent_group",
    "numeric_value",
    "numeric_unit",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return value


def compact_json_records(records: list[Any], char_limit: int) -> str:
    """Keep complete JSON records inside a deterministic character budget."""

    selected: list[Any] = []
    current_size = 2
    for record in records:
        value = _json_value(record)
        encoded = json.dumps(value, ensure_ascii=False)
        added_size = len(encoded) + (1 if selected else 0)
        if current_size + added_size > char_limit:
            if selected:
                break
            continue
        selected.append(value)
        current_size += added_size
        if current_size >= char_limit:
            break
    return json.dumps(selected, ensure_ascii=False)


def compact_evidence(records: list[Any], char_limit: int) -> str:
    values: list[dict[str, Any]] = []
    for record in records:
        raw = _json_value(record)
        if not isinstance(raw, dict):
            continue
        values.append({key: raw.get(key) for key in EVIDENCE_FIELDS if raw.get(key) is not None})
    values.sort(
        key=lambda item: (
            QUALITY_PRIORITY.get(str(item.get("source_quality", "aggregator")), 9),
            0 if item.get("numeric_value") is not None else 1,
            str(item.get("published_at", "")),
        )
    )
    return compact_json_records(values, char_limit)
