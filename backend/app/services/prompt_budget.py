from __future__ import annotations

import json
import threading
from typing import Any

from pydantic import BaseModel

from backend.app.config import Settings

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
    "source_quality",
    "published_at",
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


def compact_evidence(
    records: list[Any],
    char_limit: int,
    *,
    max_per_group: int | None = None,
) -> str:
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
    if max_per_group is not None:
        selected: list[dict[str, Any]] = []
        group_counts: dict[str, int] = {}
        for index, item in enumerate(values):
            group = str(
                item.get("independent_group")
                or item.get("id")
                or f"ungrouped:{index}"
            )
            if group_counts.get(group, 0) >= max_per_group:
                continue
            selected.append(item)
            group_counts[group] = group_counts.get(group, 0) + 1
        values = selected
    return compact_json_records(values, char_limit)


class QwenPromptBudget:
    """Count complete Qwen chat inputs and deterministically trim variable prompt text."""

    _cache: dict[tuple[str, str], Any] = {}
    _guard = threading.Lock()

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _tokenizer(self):
        key = (self.settings.ollama_7b_tokenizer, self.settings.ollama_7b_tokenizer_revision)
        with self._guard:
            tokenizer = self._cache.get(key)
            if tokenizer is None:
                from transformers import AutoTokenizer

                tokenizer = AutoTokenizer.from_pretrained(
                    key[0],
                    revision=key[1],
                )
                self._cache[key] = tokenizer
        return tokenizer

    def count(self, messages: list[dict[str, str]], schema_payload: dict[str, Any]) -> int:
        tokenizer = self._tokenizer()
        chat_tokens = tokenizer.apply_chat_template(
            messages,
            tokenize=True,
            add_generation_prompt=True,
        )
        schema_tokens = tokenizer.encode(
            json.dumps(schema_payload, ensure_ascii=False, separators=(",", ":")),
            add_special_tokens=False,
        )
        # Ollama adds a small format wrapper around the JSON schema.
        return len(chat_tokens) + len(schema_tokens) + 32

    def fit(
        self,
        *,
        system: str,
        prompt: str,
        schema_payload: dict[str, Any],
        max_tokens: int,
    ) -> tuple[list[dict[str, str]], int]:
        suffix = "\n\n只返回符合请求中 format JSON Schema 的 JSON。"

        def messages_for(value: str) -> list[dict[str, str]]:
            return [
                {"role": "system", "content": system},
                {"role": "user", "content": f"{value}{suffix}"},
            ]

        messages = messages_for(prompt)
        count = self.count(messages, schema_payload)
        if count <= max_tokens:
            return messages, count

        tokenizer = self._tokenizer()
        variable_tokens = tokenizer.encode(prompt, add_special_tokens=False)
        fixed_count = self.count(messages_for(""), schema_payload)
        available = max_tokens - fixed_count
        if available < 64:
            raise ValueError(
                f"fixed system prompt and schema require {fixed_count} tokens, limit is {max_tokens}"
            )
        # Keep the task identity and final constraints while dropping the middle,
        # where retrieval context and lower-priority evidence are placed.
        head = max(1, int(available * 0.8))
        tail = max(0, available - head)
        selected = variable_tokens[:head]
        if tail:
            selected += variable_tokens[-tail:]
        fitted_prompt = tokenizer.decode(selected, skip_special_tokens=True)
        messages = messages_for(fitted_prompt)
        count = self.count(messages, schema_payload)
        while count > max_tokens and len(selected) > 64:
            selected = selected[: max(1, len(selected) - (count - max_tokens) - 8)]
            fitted_prompt = tokenizer.decode(selected, skip_special_tokens=True)
            messages = messages_for(fitted_prompt)
            count = self.count(messages, schema_payload)
        if count > max_tokens:
            raise ValueError(f"unable to fit Qwen prompt into {max_tokens} tokens")
        return messages, count
