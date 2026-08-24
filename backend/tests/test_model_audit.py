import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel
from sqlalchemy import select

from backend.app.config import Settings
from backend.app.db import ModelCallAuditRow, SessionLocal
from backend.app.domain import AnalysisStep, EventType, NewsEvent, NewsItem, SourceQuality, utc_now
from backend.app.llm import LlmGateway
from backend.app.main import app
from backend.app.model_audit import (
    backfill_legacy_model_audits,
    cleanup_model_audits,
    detect_language,
    persist_model_audit,
)
from backend.app.storage import save_event, save_news


class AuditOutput(BaseModel):
    answer: str


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, payload):
        self.payload = payload
        self.last_request = None

    def post(self, *args, **kwargs):
        self.last_request = {"args": args, "kwargs": kwargs}
        return FakeResponse(self.payload)


def test_gateway_records_exact_redacted_input_output(monkeypatch):
    settings = Settings(
        ollama_base_url="http://ollama.invalid",
        ollama_num_threads=8,
        ollama_max_output_tokens=768,
        ollama_keep_alive="5m",
        cloud_llm_api_key="cloud-secret-value",
    )
    gateway = LlmGateway(settings)
    gateway.client = FakeClient(
        {
            "message": {"content": json.dumps({"answer": "完成"}, ensure_ascii=False)},
            "prompt_eval_count": 41,
            "eval_count": 7,
            "total_duration": 123,
        }
    )
    monkeypatch.setattr(gateway.gpu, "acquire", lambda *args, **kwargs: _null_context())

    result = gateway.generate_json(
        model="qwen2.5:3b",
        system="Authorization: Bearer cloud-secret-value",
        prompt="api_key=cloud-secret-value 请处理中文",
        schema=AuditOutput,
        operation="event_extraction",
        entity_type="news_item",
        entity_id="news-1",
    )

    assert result == {"answer": "完成"}
    request = gateway.client.last_request["kwargs"]["json"]
    assert request["keep_alive"] == "5m"
    assert request["options"]["num_thread"] == 8
    assert request["options"]["num_predict"] == 768
    assert request["format"] == AuditOutput.model_json_schema()
    assert '"properties"' not in request["messages"][-1]["content"]
    with SessionLocal() as db:
        row = db.scalar(select(ModelCallAuditRow))
    assert row is not None
    assert row.status == "completed"
    assert row.operation == "event_extraction"
    assert row.prompt_tokens == 41
    assert row.completion_tokens == 7
    assert row.fidelity == "exact"
    serialized = json.dumps(row.messages, ensure_ascii=False)
    assert "cloud-secret-value" not in serialized
    assert "[REDACTED]" in serialized
    assert "JSON Schema" in row.messages[-1]["content"]
    assert row.raw_response == '{"answer": "完成"}'


class _null_context:
    def __enter__(self):
        return None

    def __exit__(self, *args):
        return False


def test_gateway_records_both_failed_attempts(monkeypatch):
    settings = Settings(ollama_base_url="http://ollama.invalid")
    gateway = LlmGateway(settings)

    class FailingClient:
        def post(self, *args, **kwargs):
            request = httpx.Request("POST", "http://ollama.invalid/api/chat")
            raise httpx.ConnectError("Bearer retry-secret-token", request=request)

    gateway.client = FailingClient()
    monkeypatch.setattr("backend.app.llm.sleep", lambda _: None)
    monkeypatch.setattr(gateway.gpu, "acquire", lambda *args, **kwargs: _null_context())

    with pytest.raises(httpx.ConnectError):
        gateway.generate_json(model="qwen2.5:7b", system="system", prompt="prompt")

    with SessionLocal() as db:
        rows = list(db.scalars(select(ModelCallAuditRow).order_by(ModelCallAuditRow.attempt)))
    assert [row.attempt for row in rows] == [1, 2]
    assert {row.logical_call_id for row in rows} == {rows[0].logical_call_id}
    assert {row.status for row in rows} == {"failed"}
    assert all("retry-secret-token" not in (row.error or "") for row in rows)


def test_legacy_backfill_is_idempotent_and_labeled(db):
    observed = datetime(2026, 8, 22, 9, 0, tzinfo=UTC)
    item = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="示例公司发布业绩",
        summary="Revenue increased.",
        url="https://example.com/news",
        language="zh",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"audit-backfill").hexdigest(),
    )
    save_news(db, item)
    event = NewsEvent(
        news_item_ids=[item.id],
        headline=item.title,
        event_type=EventType.EARNINGS,
        entities=["示例公司"],
        direct_impact="收入增长",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        analysis_steps=[
            AnalysisStep(
                phase="event_extraction",
                executor="ollama",
                model="qwen2.5:3b",
                summary="已提取",
                occurred_at=observed,
            )
        ],
    )
    save_event(db, event)

    assert backfill_legacy_model_audits(db) == 1
    assert backfill_legacy_model_audits(db) == 0
    row = db.scalar(select(ModelCallAuditRow))
    assert row is not None
    assert row.fidelity == "reconstructed"
    assert "并非原始 prompt" in row.messages[-1]["content"]
    assert row.parsed_response["headline"] == item.title


def test_cleanup_and_language_detection(db):
    persist_model_audit(
        logical_call_id=uuid4(),
        provider="ollama",
        model="qwen2.5:3b",
        operation="event_extraction",
        attempt=1,
        status="completed",
        started_at=utc_now() - timedelta(days=100),
        completed_at=utc_now() - timedelta(days=100),
        messages=[{"role": "user", "content": "中文 and English mixed"}],
        schema_payload={},
        raw_response="完成",
    )
    assert detect_language("中文 and English mixed") == "mixed"
    assert cleanup_model_audits(db, 90) == 1
    assert db.scalar(select(ModelCallAuditRow)) is None


def test_model_log_api_filters_paginates_and_returns_detail(db):
    for index in range(3):
        persist_model_audit(
            logical_call_id=uuid4(),
            provider="ollama",
            model="qwen2.5:7b" if index < 2 else "qwen2.5:3b",
            operation="report_drafting",
            attempt=1,
            status="completed",
            started_at=utc_now() + timedelta(seconds=index),
            completed_at=utc_now() + timedelta(seconds=index, milliseconds=12),
            messages=[{"role": "user", "content": f"输入 {index}"}],
            schema_payload={"type": "object"},
            raw_response=f'{{"index": {index}}}',
            parsed_response={"index": index},
            prompt_tokens=10,
            completion_tokens=2,
        )

    with TestClient(app) as client:
        usage = client.get("/api/v1/model-usage", params={"model": "qwen2.5:7b"})
        first_page = client.get("/api/v1/model-logs", params={"limit": 1})
        cursor = first_page.json()["next_cursor"]
        second_page = client.get("/api/v1/model-logs", params={"limit": 1, "cursor": cursor})
        detail_id = first_page.json()["items"][0]["id"]
        detail = client.get(f"/api/v1/model-logs/{detail_id}")
        missing = client.get(f"/api/v1/model-logs/{uuid4()}")
        bad_cursor = client.get("/api/v1/model-logs", params={"cursor": "%%%"})

    assert usage.status_code == 200
    assert usage.json()["calls"] == 2
    assert usage.json()["success_rate"] == 1
    assert first_page.status_code == 200
    assert second_page.status_code == 200
    assert first_page.json()["items"][0]["id"] != second_page.json()["items"][0]["id"]
    assert detail.status_code == 200
    assert detail.json()["messages"][0]["content"].startswith("输入")
    assert detail.json()["parsed_response"]["index"] in {0, 1, 2}
    assert missing.status_code == 404
    assert bad_cursor.status_code == 400
