import pytest

from backend.app.config import Settings
from backend.app.services.prompt_budget import QwenPromptBudget, compact_evidence


class CharacterTokenizer:
    def apply_chat_template(self, messages, *, tokenize, add_generation_prompt):
        assert tokenize is True
        assert add_generation_prompt is True
        return list("".join(message["content"] for message in messages))

    def encode(self, value, *, add_special_tokens):
        assert add_special_tokens is False
        return list(value)

    def decode(self, values, *, skip_special_tokens):
        assert skip_special_tokens is True
        return "".join(values)


def test_qwen_budget_counts_schema_and_trims_variable_middle(monkeypatch):
    budget = QwenPromptBudget(Settings(_env_file=None))
    monkeypatch.setattr(budget, "_tokenizer", lambda: CharacterTokenizer())
    prompt = "对象：600000\n" + "低质量上下文" * 500 + "\n只能引用给定证据。"

    messages, count = budget.fit(
        system="研究系统提示",
        prompt=prompt,
        schema_payload={"type": "object", "properties": {"score": {"type": "number"}}},
        max_tokens=500,
    )

    assert count <= 500
    assert messages[1]["content"].startswith("对象：600000")
    assert "只返回符合请求中 format JSON Schema" in messages[1]["content"]


def test_qwen_budget_rejects_fixed_prompt_over_limit(monkeypatch):
    budget = QwenPromptBudget(Settings(_env_file=None))
    monkeypatch.setattr(budget, "_tokenizer", lambda: CharacterTokenizer())

    with pytest.raises(ValueError, match="fixed system prompt and schema"):
        budget.fit(
            system="固定" * 300,
            prompt="变量",
            schema_payload={"type": "object"},
            max_tokens=100,
        )


def test_compact_evidence_prioritizes_official_numeric_records():
    records = [
        {"id": "social", "source_quality": "social", "claim": "传闻"},
        {
            "id": "official",
            "source_quality": "official",
            "claim": "归母净利润增长",
            "numeric_value": 73.72,
            "numeric_unit": "%",
        },
    ]

    compacted = compact_evidence(records, 170)

    assert "official" in compacted
    assert "73.72" in compacted
