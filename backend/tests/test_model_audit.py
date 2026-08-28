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
from backend.app.llm import GpuSemaphore, LlmError, LlmGateway, serialize_keep_alive
from backend.app.main import app
from backend.app.model_audit import (
    backfill_legacy_model_audits,
    cleanup_model_audits,
    detect_language,
    persist_model_audit,
)
from backend.app.services.asset_mapping import AssetMappingOutput
from backend.app.services.model_instances import model_instance_affinity
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


@pytest.mark.parametrize(
    ("configured", "serialized"),
    [("-1", -1), ("0", 0), ("3600", 3600), ("5m", "5m"), ("24h", "24h")],
)
def test_ollama_keep_alive_serialization(configured, serialized):
    assert serialize_keep_alive(configured) == serialized


def test_gateway_records_exact_redacted_input_output(monkeypatch):
    settings = Settings(
        _env_file=None,
        ollama_base_url="http://ollama.invalid",
        ollama_context_length=4096,
        ollama_num_threads=8,
        ollama_max_output_tokens=768,
        ollama_keep_alive="5m",
        cloud_llm_api_key="cloud-secret-value",  # pragma: allowlist secret
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
    assert request["options"]["num_ctx"] == 4096
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


@pytest.mark.parametrize(
    ("lane", "expected_threads", "expected_capacity"),
    [
        ("extract", 8, 1),
        ("assist", 10, 1),
        ("research", 16, 2),
        ("code", 6, 1),
    ],
)
def test_gateway_uses_model_specific_threads_and_capacity(
    lane, expected_threads, expected_capacity
):
    settings = Settings(
        ollama_num_threads=4,
        ollama_extract_num_threads=8,
        ollama_assist_num_threads=10,
        ollama_research_num_threads=16,
        ollama_code_num_threads=6,
        ollama_extract_max_concurrency=1,
        ollama_assist_max_concurrency=1,
        ollama_research_max_concurrency=2,
        ollama_code_max_concurrency=1,
    )
    gateway = LlmGateway(settings)

    model = getattr(settings, f"ollama_{lane}_model")
    assert gateway.num_threads_for(model, lane=lane) == expected_threads
    assert gateway.gpu.capacity_for(model, lane=lane) == expected_capacity


def test_model_queue_status_counts_waiters_and_running_slots():
    class FakeRedis:
        def zremrangebyscore(self, *_args):
            return 0

        def zcard(self, _key):
            return 2

        def exists(self, key):
            return key.endswith(":0")

    semaphore = GpuSemaphore(Settings(ollama_assist_max_concurrency=2))
    semaphore._redis = FakeRedis()

    assert semaphore.queue_status("qwen2.5:7b") == {
        "lane": "assist",
        "capacity": 2,
        "queued": 2,
        "running": 1,
        "available": 1,
        "observable": True,
    }


def test_gateway_queue_status_exposes_instance_topology():
    settings = Settings(
        ollama_assist_base_url="http://assist.invalid",
        ollama_research_base_urls=("http://research-0.invalid,http://research-1.invalid"),
        ollama_assist_max_concurrency=1,
        ollama_research_max_concurrency=2,
    )
    gateway = LlmGateway(settings)
    gateway.gpu._redis = None

    class HealthyClient:
        def get(self, *_args, **_kwargs):
            return FakeResponse({"models": [{"name": "qwen2.5:7b"}]})

    gateway.client = HealthyClient()

    extract = gateway.queue_status(settings.ollama_extract_model, lane="extract")
    assist = gateway.queue_status(settings.ollama_assist_model, lane="assist")
    research = gateway.queue_status(settings.ollama_research_model, lane="research")
    code = gateway.queue_status(settings.ollama_code_model, lane="code")

    assert (extract["instance_count"], extract["per_instance_concurrency"]) == (1, 1)
    assert (assist["instance_count"], assist["per_instance_concurrency"]) == (1, 1)
    assert (research["instance_count"], research["per_instance_concurrency"]) == (2, 1)
    assert (code["instance_count"], code["per_instance_concurrency"]) == (1, 1)


def test_model_semaphore_enforces_independent_local_capacities():
    settings = Settings(
        ollama_extract_max_concurrency=1,
        ollama_research_max_concurrency=2,
    )
    semaphore = GpuSemaphore(settings)
    semaphore._redis = None

    with semaphore.acquire(settings.ollama_extract_model, timeout=0.01):
        with pytest.raises(LlmError, match="local extract"):
            with semaphore.acquire(settings.ollama_extract_model, timeout=0.01):
                pass
        with semaphore.acquire(settings.ollama_research_model, timeout=0.01, lane="research"):
            with semaphore.acquire(settings.ollama_research_model, timeout=0.01, lane="research"):
                with pytest.raises(LlmError, match="local research"):
                    with semaphore.acquire(
                        settings.ollama_research_model, timeout=0.01, lane="research"
                    ):
                        pass


def test_model_semaphore_renews_short_redis_lease(monkeypatch):
    import time as time_module

    class FakeLock:
        def __init__(self):
            self.extensions = 0
            self.released = False

        def acquire(self, blocking=False):
            return True

        def extend(self, _seconds, replace_ttl=False):
            assert replace_ttl is True
            self.extensions += 1

        def owned(self):
            return not self.released

        def release(self):
            self.released = True

    lock = FakeLock()

    class FakeRedis:
        def zadd(self, *_args):
            return 1

        def zrem(self, *_args):
            return 1

        def lock(self, _key, *, timeout, blocking_timeout, thread_local):
            assert timeout == 120
            assert blocking_timeout == 0
            assert thread_local is False
            return lock

    semaphore = GpuSemaphore(Settings(ollama_research_max_concurrency=1))
    semaphore._redis = FakeRedis()
    monkeypatch.setattr("backend.app.llm.INFERENCE_LOCK_HEARTBEAT_SECONDS", 0.01)

    with semaphore.acquire(semaphore.settings.ollama_research_model, timeout=0.1, lane="research"):
        time_module.sleep(0.035)

    assert lock.extensions >= 2
    assert lock.released is True


class _null_context:
    def __enter__(self):
        return None

    def __exit__(self, *args):
        return False


def test_gateway_does_not_retry_transport_failures(monkeypatch):
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
        gateway.generate_json(model="qwen2.5:7b", lane="assist", system="system", prompt="prompt")

    with SessionLocal() as db:
        rows = list(db.scalars(select(ModelCallAuditRow).order_by(ModelCallAuditRow.attempt)))
    assert [row.attempt for row in rows] == [1]
    assert {row.logical_call_id for row in rows} == {rows[0].logical_call_id}
    assert {row.status for row in rows} == {"failed"}
    assert all("retry-secret-token" not in (row.error or "") for row in rows)


def test_gateway_retries_invalid_json_once(monkeypatch):
    settings = Settings(ollama_base_url="http://ollama.invalid")
    gateway = LlmGateway(settings)

    class InvalidThenValidClient:
        def __init__(self):
            self.calls = 0

        def post(self, *args, **kwargs):
            self.calls += 1
            content = "not-json" if self.calls == 1 else '{"answer":"ok"}'
            return FakeResponse({"message": {"content": content}})

    client = InvalidThenValidClient()
    gateway.client = client
    monkeypatch.setattr("backend.app.llm.sleep", lambda _: None)
    monkeypatch.setattr(gateway.gpu, "acquire", lambda *args, **kwargs: _null_context())

    assert gateway.generate_json(
        model="qwen2.5:7b",
        lane="assist",
        system="system",
        prompt="prompt",
        schema=AuditOutput,
    ) == {"answer": "ok"}
    assert client.calls == 2


def test_gateway_retries_mapping_output_without_candidate_reason(monkeypatch):
    settings = Settings(ollama_base_url="http://ollama.invalid")
    gateway = LlmGateway(settings)

    class EmptyThenExplainedClient:
        def __init__(self):
            self.calls = 0

        def post(self, *args, **kwargs):
            self.calls += 1
            content = (
                "{}"
                if self.calls == 1
                else '{"candidates":[],"no_asset_reason":"没有明确标的"}'
            )
            return FakeResponse({"message": {"content": content}})

    client = EmptyThenExplainedClient()
    gateway.client = client
    monkeypatch.setattr("backend.app.llm.sleep", lambda _: None)
    monkeypatch.setattr(gateway.gpu, "acquire", lambda *args, **kwargs: _null_context())

    assert gateway.generate_json(
        model="qwen2.5:7b",
        lane="assist",
        system="system",
        prompt="prompt",
        schema=AssetMappingOutput,
    )["no_asset_reason"] == "没有明确标的"
    assert client.calls == 2


def test_research_pool_routes_one_slot_per_endpoint(monkeypatch):
    settings = Settings(
        ollama_base_url="http://main.invalid",
        ollama_research_base_urls="http://research-0.invalid,http://research-1.invalid",
        ollama_research_max_output_tokens=1024,
        ollama_research_timeout_seconds=900,
    )
    gateway = LlmGateway(settings)
    gateway.gpu._redis = None

    class PoolClient:
        def __init__(self):
            self.urls = []

        def get(self, *args, **kwargs):
            return FakeResponse({"models": [{"name": settings.ollama_research_model}]})

        def post(self, url, **kwargs):
            self.urls.append((url, kwargs))
            return FakeResponse({"message": {"content": '{"answer":"ok"}'}})

    client = PoolClient()
    gateway.client = client
    monkeypatch.setattr("backend.app.llm.persist_model_audit", lambda **kwargs: None)

    for _ in range(2):
        assert gateway.generate_json(
            model=settings.ollama_research_model,
            lane="research",
            system="system",
            prompt="prompt",
            schema=AuditOutput,
        ) == {"answer": "ok"}

    assert [item[0] for item in client.urls] == [
        "http://research-0.invalid/api/chat",
        "http://research-1.invalid/api/chat",
    ]
    assert all(item[1]["json"]["options"]["num_predict"] == 1024 for item in client.urls)
    assert all(item[1]["timeout"] == 900 for item in client.urls)


def test_same_7b_model_keeps_assist_and_research_lanes_isolated(monkeypatch):
    settings = Settings(
        ollama_assist_model="qwen2.5:7b",
        ollama_research_model="qwen2.5:7b",
        ollama_assist_base_url="http://assist.invalid",
        ollama_research_base_urls="http://research-0.invalid,http://research-1.invalid",
        ollama_assist_num_threads=8,
        ollama_research_num_threads=8,
        ollama_assist_max_output_tokens=8192,
        ollama_research_max_output_tokens=8192,
    )
    gateway = LlmGateway(settings)
    gateway.gpu._redis = None

    class LaneClient:
        def __init__(self):
            self.requests = []

        def get(self, *_args, **_kwargs):
            return FakeResponse({"models": [{"name": "qwen2.5:7b"}]})

        def post(self, url, **kwargs):
            self.requests.append((url, kwargs))
            return FakeResponse(
                {
                    "message": {"content": '{"answer":"ok"}'},
                    "prompt_eval_count": 2000,
                }
            )

    gateway.client = LaneClient()
    monkeypatch.setattr("backend.app.llm.persist_model_audit", lambda **_kwargs: None)
    prompt_budgets = []

    def fit_prompt(**kwargs):
        prompt_budgets.append(kwargs["max_tokens"])
        return ([{"role": "user", "content": "bounded"}], 2000)

    monkeypatch.setattr(gateway.prompt_budget, "fit", fit_prompt)

    for lane in ("assist", "research", "research"):
        assert gateway.generate_json(
            model="qwen2.5:7b",
            lane=lane,
            system="system",
            prompt="prompt",
            schema=AuditOutput,
        ) == {"answer": "ok"}

    assert gateway.generate_json(
        model="qwen2.5:7b",
        lane="assist",
        system="system",
        prompt="mapping prompt",
        schema=AuditOutput,
        max_input_tokens=2048,
        context_length=8192,
        max_output_tokens=1024,
    ) == {"answer": "ok"}

    assert [request[0] for request in gateway.client.requests] == [
        "http://assist.invalid/api/chat",
        "http://research-0.invalid/api/chat",
        "http://research-1.invalid/api/chat",
        "http://assist.invalid/api/chat",
    ]
    for _, request in gateway.client.requests[:3]:
        assert request["json"]["options"] == {
            "temperature": 0.1,
            "num_ctx": 16384,
            "num_predict": 8192,
            "num_thread": 8,
        }
    assert gateway.client.requests[-1][1]["json"]["options"] == {
        "temperature": 0.1,
        "num_ctx": 8192,
        "num_predict": 1024,
        "num_thread": 8,
    }
    assert prompt_budgets == [
        settings.ollama_7b_max_input_tokens,
        settings.ollama_research_max_input_tokens,
        settings.ollama_research_max_input_tokens,
        2048,
    ]


def test_bound_research_task_keeps_instance_affinity(monkeypatch):
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://affinity-0.invalid,http://affinity-1.invalid",
        ollama_research_max_concurrency=2,
    )
    gateway = LlmGateway(settings)
    gateway.gpu._redis = None

    class Client:
        def __init__(self):
            self.urls = []

        def get(self, *_args, **_kwargs):
            return FakeResponse({"models": [{"name": settings.ollama_research_model}]})

        def post(self, url, **_kwargs):
            self.urls.append(url)
            return FakeResponse({"message": {"content": '{"answer":"ok"}'}})

    gateway.client = Client()
    monkeypatch.setattr("backend.app.llm.persist_model_audit", lambda **_kwargs: None)

    with model_instance_affinity("research", "research-1", task_id="task-1"):
        for _ in range(2):
            assert gateway.generate_json(
                model=settings.ollama_research_model,
                lane="research",
                system="system",
                prompt="prompt",
                schema=AuditOutput,
            ) == {"answer": "ok"}

    assert gateway.client.urls == [
        "http://affinity-1.invalid/api/chat",
        "http://affinity-1.invalid/api/chat",
    ]


def test_transport_failure_moves_bound_task_to_other_healthy_instance(monkeypatch):
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://failover-0.invalid,http://failover-1.invalid",
        ollama_research_max_concurrency=2,
    )
    gateway = LlmGateway(settings)
    gateway.gpu._redis = None

    class Client:
        def __init__(self):
            self.urls = []

        def get(self, *_args, **_kwargs):
            return FakeResponse({"models": [{"name": settings.ollama_research_model}]})

        def post(self, url, **_kwargs):
            self.urls.append(url)
            if "failover-0" in url:
                request = httpx.Request("POST", url)
                raise httpx.ConnectError("instance offline", request=request)
            return FakeResponse({"message": {"content": '{"answer":"ok"}'}})

    gateway.client = Client()
    monkeypatch.setattr("backend.app.llm.persist_model_audit", lambda **_kwargs: None)
    monkeypatch.setattr("backend.app.llm.sleep", lambda _seconds: None)

    with model_instance_affinity("research", "research-0", task_id="task-2") as affinity:
        assert gateway.generate_json(
            model=settings.ollama_research_model,
            lane="research",
            system="system",
            prompt="prompt",
            schema=AuditOutput,
        ) == {"answer": "ok"}
        assert affinity.instance_id == "research-1"

    assert gateway.client.urls == [
        "http://failover-0.invalid/api/chat",
        "http://failover-1.invalid/api/chat",
    ]


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
