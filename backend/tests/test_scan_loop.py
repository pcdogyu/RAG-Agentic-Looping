import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from time import sleep
from types import SimpleNamespace

import pytest

from backend.app import worker
from backend.app.domain import (
    AnalysisStep,
    CandidateAsset,
    EventReport,
    EventResearchRun,
    Evidence,
    NewsEvent,
    NewsItem,
    Rating,
    RunStatus,
    SourceQuality,
    TargetImpact,
    TargetType,
    TradeStatus,
)
from backend.app.main import _analysis_logs
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.asset_mapping import AssetMappingResult
from backend.app.services.source_filter import (
    SourceFilterConfig,
    filter_news_items,
    save_source_filter,
)
from backend.app.storage import get_event, get_news, list_runs, save_event, save_news, save_run


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.expirations = []

    def get(self, key):
        return self.data.get(key)

    def ping(self):
        return True

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return False
        self.data[key] = value.encode() if isinstance(value, str) else value
        return True

    def delete(self, key):
        self.data.pop(key, None)

    def expire(self, key, seconds):
        self.expirations.append((key, seconds))
        return key in self.data


def test_scan_gate_lease_is_renewed_only_by_its_owner():
    redis = FakeRedis()
    redis.set(worker.SCAN_GATE_KEY, "scan-task")

    assert worker._renew_scan_gate(redis, "scan-task") is True
    assert redis.expirations == [(worker.SCAN_GATE_KEY, worker.SCAN_GATE_TTL_SECONDS)]
    assert worker._renew_scan_gate(redis, "other-task") is False
    assert len(redis.expirations) == 1


def test_scan_visibility_and_gate_cover_long_running_tasks():
    expected = worker.SCAN_VISIBILITY_TIMEOUT_SECONDS

    assert expected == 12 * 60 * 60
    assert worker.SCAN_GATE_TTL_SECONDS >= expected
    assert worker.celery_app.conf.broker_transport_options["visibility_timeout"] == expected
    assert worker.celery_app.conf.result_backend_transport_options["visibility_timeout"] == expected
    assert worker.celery_app.conf.visibility_timeout == expected
    assert worker.NEWS_EXTRACTION_QUEUE_TTL_SECONDS == 12 * 60 * 60
    assert worker.celery_app.conf.task_routes["market_loop.extract_news_item"] == {
        "queue": "extract"
    }
    assert worker.celery_app.conf.task_routes["market_loop.retry_news_item"] == {"queue": "extract"}
    assert worker.celery_app.conf.task_routes["market_loop.research_asset"] == {"queue": "research"}
    assert worker.celery_app.conf.task_default_priority == 5
    assert worker.celery_app.conf.task_queue_max_priority == 9
    assert worker.celery_app.conf.broker_transport_options["priority_steps"] == list(range(10))


def test_instance_task_delays_when_every_instance_is_offline(monkeypatch):
    selected = SimpleNamespace(id="extract-0")
    retry_options = {}
    called = False

    class FakeTask:
        request = SimpleNamespace(
            id="offline-task",
            delivery_info={"routing_key": "extract.extract-0"},
        )

        def retry(self, **kwargs):
            retry_options.update(kwargs)
            return worker.Retry()

    def business_task(_self, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(worker, "select_model_instance", lambda *_args, **_kwargs: selected)
    monkeypatch.setattr(worker, "instance_health", lambda *_args, **_kwargs: (False, False))
    monkeypatch.setattr(worker, "update_instance_assignment", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker, "update_model_task", lambda *_args, **_kwargs: None)
    wrapped = worker.model_instance_task("extract")(business_task)

    with pytest.raises(worker.Retry):
        wrapped(FakeTask(), model_instance_id="extract-0")

    assert called is False
    assert retry_options["queue"] == "extract.extract-0"
    assert retry_options["countdown"] == 30
    assert retry_options["max_retries"] == 1_000_000


def test_instance_task_refreshes_running_model_task_lease(monkeypatch):
    selected = SimpleNamespace(id="extract-0")
    touches = []
    statuses = []

    class FakeTask:
        request = SimpleNamespace(
            id="heartbeat-task",
            delivery_info={"routing_key": "extract.extract-0"},
        )

    def business_task(_self, **_kwargs):
        sleep(0.04)
        return "completed"

    monkeypatch.setattr(worker.settings, "model_task_heartbeat_seconds", 0.01)
    monkeypatch.setattr(worker, "select_model_instance", lambda *_args, **_kwargs: selected)
    monkeypatch.setattr(worker, "instance_health", lambda *_args, **_kwargs: (True, True))
    monkeypatch.setattr(worker, "update_instance_assignment", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker,
        "update_model_task",
        lambda *_args, **kwargs: statuses.append(kwargs["status"]),
    )
    monkeypatch.setattr(
        worker,
        "touch_model_task",
        lambda *_args, **_kwargs: touches.append(True) or True,
    )
    wrapped = worker.model_instance_task("extract")(business_task)

    assert wrapped(FakeTask(), model_instance_id="extract-0") == "completed"
    assert len(touches) >= 2
    assert statuses == ["running", "completed"]


def test_instance_task_marks_tracked_model_task_failed(monkeypatch):
    selected = SimpleNamespace(id="extract-0")
    updates = []

    class FakeTask:
        request = SimpleNamespace(
            id="failed-task",
            delivery_info={"routing_key": "extract.extract-0"},
        )

    def business_task(_self, **_kwargs):
        raise RuntimeError("inference failed")

    monkeypatch.setattr(worker, "select_model_instance", lambda *_args, **_kwargs: selected)
    monkeypatch.setattr(worker, "instance_health", lambda *_args, **_kwargs: (True, True))
    monkeypatch.setattr(worker, "update_instance_assignment", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker,
        "update_model_task",
        lambda *_args, **kwargs: updates.append(kwargs),
    )
    wrapped = worker.model_instance_task("extract")(business_task)

    with pytest.raises(RuntimeError, match="inference failed"):
        wrapped(FakeTask(), model_instance_id="extract-0")

    assert [item["status"] for item in updates] == ["running", "failed"]
    assert updates[-1]["error"] == "RuntimeError: inference failed"


@pytest.mark.parametrize(
    ("priority", "retries", "expected"),
    [(5, 0, True), (0, 0, False), (5, 1, False)],
)
def test_research_instance_task_only_rebalances_ordinary_first_attempts(
    monkeypatch, priority, retries, expected
):
    selected = SimpleNamespace(id="research-0")
    selection = {}

    class FakeTask:
        request = SimpleNamespace(
            id="research-task",
            retries=retries,
            delivery_info={
                "routing_key": "research.research-0",
                "priority": priority,
            },
        )

    def select(*_args, **kwargs):
        selection.update(kwargs)
        return selected

    monkeypatch.setattr(worker, "select_model_instance", select)
    monkeypatch.setattr(worker, "instance_health", lambda *_args, **_kwargs: (True, True))
    monkeypatch.setattr(worker, "update_instance_assignment", lambda *_args, **_kwargs: None)
    wrapped = worker.model_instance_task("research")(
        lambda _self, **_kwargs: "completed"
    )

    assert wrapped(FakeTask(), model_instance_id="research-0") == "completed"
    assert selection["rebalance_preferred"] is expected


def test_periodic_evolution_dispatch_uses_an_instance_queue(monkeypatch):
    selected = SimpleNamespace(id="code-1")
    recorded = {}
    published = {}

    monkeypatch.setattr(worker, "select_model_instance", lambda *_args, **_kwargs: selected)
    monkeypatch.setattr(
        worker,
        "record_model_task",
        lambda _lane, **kwargs: recorded.update(kwargs),
    )
    monkeypatch.setattr(
        worker.evolve_from_outcomes,
        "apply_async",
        lambda **kwargs: published.update(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )

    result = worker.dispatch_evolve_from_outcomes.run()

    assert recorded["instance_id"] == "code-1"
    assert published["queue"] == "evolution.code-1"
    assert published["kwargs"] == {"model_instance_id": "code-1"}
    assert result == {"task_id": published["task_id"], "instance_id": "code-1"}


def test_market_factor_refresh_advances_only_when_a_larger_window_matures():
    assert worker._due_market_factor_refresh_session(age_days=1.9) is None
    assert worker._due_market_factor_refresh_session(age_days=2) == 1
    assert worker._due_market_factor_refresh_session(age_days=8, completed_session=1) == 5
    assert worker._due_market_factor_refresh_session(age_days=30, completed_session=5) == 20
    assert worker._due_market_factor_refresh_session(age_days=40, completed_session=20) is None


def test_news_extraction_registry_sorts_active_items_and_keeps_failed(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC).isoformat()
    entries = [
        {
            "task_id": f"task-{status}",
            "news_id": f"00000000-0000-0000-0000-00000000000{index}",
            "title": status,
            "source": "test",
            "published_at": now,
            "status": status,
            "attempt": 1,
            "queued_at": now,
            "updated_at": now,
            "error": "failed" if status == "failed" else None,
        }
        for index, status in enumerate(
            ["completed", "queued", "failed", "retrying", "running"], start=1
        )
    ]
    worker._initialize_news_extraction_queue(
        redis,
        "scan-1",
        entries,
        {"discovered": 5, "accepted": 5, "filtered": 0},
    )

    payload = worker.get_news_extraction_queue(limit=3)

    assert payload["model"] == "qwen2.5:3b"
    assert payload["counts"] == {
        "queued": 1,
        "running": 1,
        "retrying": 1,
        "completed": 1,
        "failed": 1,
    }
    assert [item["status"] for item in payload["items"]] == [
        "running",
        "retrying",
        "queued",
    ]
    assert payload["truncated"] is True


def test_clear_news_extraction_queue_cancels_active_and_failed_items():
    redis = FakeRedis()
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC).isoformat()
    entries = [
        {
            "task_id": f"task-{status}",
            "news_id": f"news-{status}",
            "title": status,
            "status": status,
            "queued_at": now,
            "updated_at": now,
        }
        for status in ("queued", "running", "completed", "failed")
    ]
    worker._initialize_news_extraction_queue(redis, "scan-1", entries, {})
    redis.set(worker.SCAN_GATE_KEY, "scan-1")

    result = worker.clear_news_extraction_queue(redis)
    payload = worker._read_news_extraction_queue(redis)

    assert result == {
        "cancelled": 3,
        "celery_task_ids": ["task-queued", "task-running"],
    }
    assert payload["state"] == "cancelled"
    assert [item["status"] for item in payload["items"]] == [
        "cancelled",
        "cancelled",
        "completed",
        "cancelled",
    ]
    assert redis.get(worker.SCAN_GATE_KEY) is None


def test_clear_news_extraction_instance_releases_active_scan_gate():
    redis = FakeRedis()
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC).isoformat()
    entries = [
        {
            "task_id": "task-completed",
            "news_id": "news-completed",
            "title": "completed",
            "instance_id": "extract-1",
            "status": "completed",
            "queued_at": now,
            "updated_at": now,
        },
        {
            "task_id": "task-running",
            "news_id": "news-running",
            "title": "running",
            "instance_id": "extract-0",
            "status": "running",
            "queued_at": now,
            "updated_at": now,
        },
    ]
    worker._initialize_news_extraction_queue(redis, "scan-1", entries, {})
    worker._update_scan_status(
        redis,
        state="running",
        task_id="scan-1",
        phase="extracting",
        current=1,
        total=2,
    )
    redis.set(worker.SCAN_GATE_KEY, "scan-1")

    result = worker.clear_news_extraction_queue(redis, instance_id="extract-0")
    payload = worker._read_news_extraction_queue(redis)
    scan_status = worker._read_scan_status(redis)

    assert result == {
        "cancelled": 1,
        "celery_task_ids": ["task-running"],
    }
    assert payload["state"] == "cancelled"
    assert [item["status"] for item in payload["items"]] == [
        "completed",
        "cancelled",
    ]
    assert scan_status["state"] == "cancelled"
    assert scan_status["current"] == 2
    assert scan_status["total"] == 2
    assert redis.get(worker.SCAN_GATE_KEY) is None


def test_clear_empty_extraction_instance_keeps_active_scan_gate():
    redis = FakeRedis()
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC).isoformat()
    entries = [
        {
            "task_id": "task-running",
            "news_id": "news-running",
            "title": "running",
            "instance_id": "extract-1",
            "status": "running",
            "queued_at": now,
            "updated_at": now,
        }
    ]
    worker._initialize_news_extraction_queue(redis, "scan-1", entries, {})
    redis.set(worker.SCAN_GATE_KEY, "scan-1")

    result = worker.clear_news_extraction_queue(redis, instance_id="extract-0")
    payload = worker._read_news_extraction_queue(redis)

    assert result == {"cancelled": 0, "celery_task_ids": []}
    assert payload["state"] == "running"
    assert payload["items"][0]["status"] == "running"
    assert redis.get(worker.SCAN_GATE_KEY) == b"scan-1"


def test_news_extraction_timing_accumulates_attempts_without_retry_wait(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    current = {"now": datetime(2026, 8, 26, 8, 0, tzinfo=UTC)}
    monkeypatch.setattr(worker, "utc_now", lambda: current["now"])
    queued_at = current["now"]
    worker._initialize_news_extraction_queue(
        redis,
        "scan-1",
        [
            {
                "task_id": "extract-timed",
                "news_id": "00000000-0000-0000-0000-000000000001",
                "title": "计时新闻",
                "source": "test",
                "published_at": queued_at.isoformat(),
                "status": "queued",
                "attempt": 0,
                "queued_at": queued_at.isoformat(),
                "updated_at": queued_at.isoformat(),
                "error": None,
            }
        ],
        {"discovered": 1, "accepted": 1, "filtered": 0},
    )

    current["now"] = queued_at + timedelta(seconds=10)
    worker._update_news_extraction_item(
        redis, "scan-1", "00000000-0000-0000-0000-000000000001", "running", attempt=1
    )
    current["now"] = queued_at + timedelta(seconds=30)
    worker._update_news_extraction_item(
        redis,
        "scan-1",
        "00000000-0000-0000-0000-000000000001",
        "retrying",
        attempt=1,
        error="temporary",
    )
    current["now"] = queued_at + timedelta(seconds=50)
    worker._update_news_extraction_item(
        redis, "scan-1", "00000000-0000-0000-0000-000000000001", "running", attempt=2
    )
    current["now"] = queued_at + timedelta(seconds=80)
    worker._update_news_extraction_item(
        redis, "scan-1", "00000000-0000-0000-0000-000000000001", "completed", attempt=2
    )

    stored = worker._read_news_extraction_queue(redis)["items"][0]
    assert stored["queue_duration_ms"] == 10000
    assert stored["execution_duration_ms"] == 50000
    assert stored["completed_at"] == current["now"].isoformat()
    payload = worker.get_news_extraction_queue()
    assert payload["items"] == []
    assert payload["average_queue_duration_ms"] == 10000
    assert payload["average_execution_duration_ms"] == 50000
    assert payload["queue_duration_sample_count"] == 1
    assert payload["execution_duration_sample_count"] == 1


def test_news_extraction_average_execution_uses_recent_four_hours(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    monkeypatch.setattr(worker, "utc_now", lambda: now)
    entries = [
        {
            "task_id": "recent",
            "news_id": "news-recent",
            "title": "最近完成",
            "status": "completed",
            "queued_at": (now - timedelta(minutes=3)).isoformat(),
            "started_at": (now - timedelta(minutes=2)).isoformat(),
            "completed_at": (now - timedelta(minutes=1)).isoformat(),
            "updated_at": (now - timedelta(minutes=1)).isoformat(),
            "execution_duration_ms": 60_000,
        },
        {
            "task_id": "old",
            "news_id": "news-old",
            "title": "较早完成",
            "status": "completed",
            "queued_at": (now - timedelta(hours=6)).isoformat(),
            "started_at": (now - timedelta(hours=5, minutes=30)).isoformat(),
            "completed_at": (now - timedelta(hours=5)).isoformat(),
            "updated_at": (now - timedelta(hours=5)).isoformat(),
            "execution_duration_ms": 30 * 60_000,
        },
    ]
    worker._initialize_news_extraction_queue(redis, "scan-1", entries, {})

    payload = worker.get_news_extraction_queue()

    assert payload["average_execution_duration_ms"] == 60_000
    assert payload["execution_duration_sample_count"] == 1


def test_news_extraction_workflow_routes_header_and_callback_to_extract(monkeypatch):
    redis = FakeRedis()
    captured = {}
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    news = NewsItem(
        source="test",
        title="逐条抽取新闻",
        url="https://example.com/extract-one",
        published_at=now,
        content_hash=sha256(b"extract-one").hexdigest(),
    )

    class ChordStub:
        def __init__(self, header, callback):
            captured["header"] = header
            captured["callback"] = callback

    monkeypatch.setattr(worker, "chord", ChordStub)

    task_ids, workflow = worker._build_news_extraction_workflow(
        redis,
        "scan-1",
        [news],
        {"discovered": 1, "accepted": 1, "filtered": 0},
    )

    assert len(task_ids) == 1
    assert isinstance(workflow, ChordStub)
    assert captured["header"][0].options["queue"] == "extract.extract-0"
    assert captured["header"][0].kwargs["model_instance_id"] == "extract-0"
    assert captured["callback"].options["queue"] == "extract"
    assert worker._read_news_extraction_queue(redis)["items"][0]["status"] == "queued"


def test_background_extraction_queue_only_receives_hard_gate_matches(db, monkeypatch):
    redis = FakeRedis()
    captured = {}
    observed = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    items = [
        NewsItem(
            source="test",
            title=title,
            url=f"https://example.com/{suffix}",
            published_at=observed,
            content_hash=sha256(suffix.encode()).hexdigest(),
        )
        for title, suffix in [
            ("Apple earnings", "hard-gate-allowed"),
            ("Microsoft earnings", "hard-gate-whitelist-miss"),
            ("Apple weather warning", "hard-gate-blacklist-veto"),
        ]
    ]
    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["Apple"], blacklist_keywords=["weather"]),
    )

    class ChordStub:
        def __init__(self, header, callback):
            captured["header"] = header
            captured["callback"] = callback

    monkeypatch.setattr(worker, "chord", ChordStub)
    accepted, filtered = filter_news_items(db, items)
    pending = worker._persist_news_for_extraction(db, accepted)
    task_ids, _workflow = worker._build_news_extraction_workflow(
        redis,
        "scan-hard-gate",
        pending,
        {"discovered": 3, "accepted": len(accepted), "filtered": filtered},
    )

    assert [item.title for item in accepted] == ["Apple earnings"]
    assert filtered == 2
    assert len(task_ids) == 1
    assert len(captured["header"]) == 1
    assert worker._read_news_extraction_queue(redis)["items"][0]["title"] == "Apple earnings"
    assert get_news(db, items[0].id) is not None
    assert get_news(db, items[1].id) is None
    assert get_news(db, items[2].id) is None


def test_single_news_extraction_dispatches_downstream_immediately(db, monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    news = NewsItem(
        source="test",
        title="单篇新闻抽取",
        url="https://example.com/single-extraction",
        published_at=now,
        content_hash=sha256(b"single-extraction").hexdigest(),
    )
    save_news(db, news)
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="other",
        direct_impact="测试",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
    )

    class EventServiceStub:
        def __init__(self, _registry):
            pass

        def ingest(self, _db, items):
            assert [item.id for item in items] == [news.id]
            return [event]

    monkeypatch.setattr(worker, "EventService", EventServiceStub)
    monkeypatch.setattr(worker.settings, "auto_research", True)
    dispatched = []
    monkeypatch.setattr(
        worker,
        "enqueue_asset_mapping",
        lambda _db, queued_event, **_kwargs: dispatched.append(queued_event.id)
        or "mapping-task",
    )
    redis.set(worker.SCAN_GATE_KEY, "scan-1")
    worker._initialize_news_extraction_queue(
        redis,
        "scan-1",
        [
            {
                "task_id": "extract-1",
                "news_id": str(news.id),
                "title": news.title,
                "source": news.source,
                "published_at": now.isoformat(),
                "status": "queued",
                "attempt": 0,
                "queued_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "error": None,
            }
        ],
        {"discovered": 1, "accepted": 1, "filtered": 0},
    )

    result = worker.extract_news_item.run("scan-1", str(news.id))

    assert result["status"] == "completed"
    assert result["event_ids"] == [str(event.id)]
    assert result["research_queued"] == 0
    assert result["asset_mapping_queued"] == 1
    assert result["downstream_dispatched"] is True
    assert dispatched == [event.id]
    payload = worker._read_news_extraction_queue(redis)
    assert payload["items"][0]["status"] == "completed"
    assert payload["items"][0]["attempt"] == 1


def test_realtime_dispatch_routes_mapped_event_to_research(monkeypatch):
    monkeypatch.setattr(worker.settings, "auto_research", True)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="已映射事件实时进入研究",
        event_type="other",
        direct_impact="测试",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[0],
                relationship="direct",
                relevance=1,
                rationale="test",
            )
        ],
    )
    dispatched = []
    monkeypatch.setattr(
        worker,
        "enqueue_event_report",
        lambda _db, queued_event: dispatched.append(queued_event.id)
        or ("research-task", object()),
    )

    result = worker._dispatch_extracted_events(object(), [event], tolerate_errors=True)

    assert result == (1, 0, True)
    assert dispatched == [event.id]


def test_terminal_news_extraction_failure_returns_structured_result(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(worker.extract_news_item, "max_retries", 0)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC).isoformat()
    news_id = "00000000-0000-0000-0000-000000000099"
    redis.set(worker.SCAN_GATE_KEY, "scan-1")
    worker._initialize_news_extraction_queue(
        redis,
        "scan-1",
        [
            {
                "task_id": "extract-failed",
                "news_id": news_id,
                "title": "不存在的新闻",
                "source": "test",
                "published_at": now,
                "status": "queued",
                "attempt": 0,
                "queued_at": now,
                "updated_at": now,
                "error": None,
            }
        ],
        {"discovered": 1, "accepted": 1, "filtered": 0},
    )

    result = worker.extract_news_item.run("scan-1", news_id)

    assert result["status"] == "failed"
    assert result["event_ids"] == []
    assert "ValueError" in result["error"]
    payload = worker._read_news_extraction_queue(redis)
    assert payload["state"] == "completed_with_errors"
    assert payload["items"][0]["status"] == "failed"


def test_failed_extraction_does_not_block_batch_finalization(db, monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(worker.notifier, "send", lambda *_args, **_kwargs: None)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="已成功抽取的事件",
        event_type="other",
        direct_impact="测试",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[0],
                relationship="direct",
                relevance=1,
                rationale="test",
            )
        ],
    )
    save_event(db, event)
    redis.set(worker.SCAN_GATE_KEY, "scan-1")
    entries = [
        {
            "task_id": f"extract-{index}",
            "news_id": f"00000000-0000-0000-0000-00000000000{index}",
            "title": f"news-{index}",
            "source": "test",
            "published_at": now.isoformat(),
            "status": status,
            "attempt": 1,
            "queued_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "error": "failed" if status == "failed" else None,
        }
        for index, status in enumerate(["completed", "failed"], start=1)
    ]
    worker._initialize_news_extraction_queue(
        redis,
        "scan-1",
        entries,
        {"discovered": 3, "accepted": 2, "filtered": 1},
    )
    queued = []
    monkeypatch.setattr(
        worker,
        "enqueue_event_report",
        lambda _db, queued_event: queued.append(queued_event.id) or ("task", object()),
    )

    result = worker.finalize_news_extraction.run(
        [
            {
                "status": "completed",
                "event_ids": [str(event.id)],
                "research_queued": 1,
                "asset_mapping_queued": 0,
                "downstream_dispatched": True,
            },
            {"status": "failed", "event_ids": [], "error": "RuntimeError"},
        ],
        "scan-1",
    )

    assert result["status"] == "completed_with_errors"
    assert result["extraction_completed"] == 1
    assert result["extraction_failed"] == 1
    assert result["research_queued"] == 1
    assert queued == []
    assert redis.get(worker.SCAN_GATE_KEY) is None


def test_batch_finalizer_backfills_legacy_extraction_results(db, monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(worker.notifier, "send", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker.settings, "auto_research", True)
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="滚动升级前抽取的事件",
        event_type="other",
        direct_impact="测试",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[0],
                relationship="direct",
                relevance=1,
                rationale="test",
            )
        ],
    )
    save_event(db, event)
    redis.set(worker.SCAN_GATE_KEY, "scan-legacy")
    worker._initialize_news_extraction_queue(
        redis,
        "scan-legacy",
        [
            {
                "task_id": "extract-legacy",
                "news_id": "00000000-0000-0000-0000-000000000001",
                "title": "legacy",
                "source": "test",
                "published_at": now.isoformat(),
                "status": "completed",
                "attempt": 1,
                "queued_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "error": None,
            }
        ],
        {"discovered": 1, "accepted": 1, "filtered": 0},
    )
    queued = []
    monkeypatch.setattr(
        worker,
        "enqueue_event_report",
        lambda _db, queued_event: queued.append(queued_event.id) or ("task", object()),
    )

    result = worker.finalize_news_extraction.run(
        [{"status": "completed", "event_ids": [str(event.id)]}],
        "scan-legacy",
    )

    assert result["research_queued"] == 1
    assert result["asset_mapping_queued"] == 0
    assert queued == [event.id]
    assert redis.get(worker.SCAN_GATE_KEY) is None


def test_scan_gate_claim_never_replaces_another_task():
    redis = FakeRedis()
    redis.set(worker.SCAN_GATE_KEY, "active-task")

    assert worker._claim_scan_gate(redis, "active-task") is True
    assert worker._claim_scan_gate(redis, "stale-task") is False
    assert redis.get(worker.SCAN_GATE_KEY) == b"active-task"


def test_stale_scan_cannot_overwrite_or_clear_current_status():
    redis = FakeRedis()
    redis.set(worker.SCAN_GATE_KEY, "current-task")
    worker._update_scan_status(
        redis,
        state="running",
        task_id="current-task",
        phase="extracting",
        current=3,
        total=8,
    )

    result = worker._complete_scan(
        redis,
        "stale-task",
        {"status": "completed", "discovered": 8, "events": 8},
    )

    assert result["state"] == "running"
    assert result["task_id"] == "current-task"
    assert redis.get(worker.SCAN_GATE_KEY) == b"current-task"


def test_stale_scan_stops_at_the_next_safe_checkpoint():
    redis = FakeRedis()
    redis.set(worker.SCAN_GATE_KEY, "current-task")

    with pytest.raises(worker.ScanLeaseLost):
        worker._wait_if_scan_paused(
            redis,
            "stale-task",
            phase="extracting",
            current=2,
            total=8,
        )


def test_scan_queue_is_idempotent_and_completion_anchors_countdown(monkeypatch):
    redis = FakeRedis()
    queued_ids = []
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(
        worker.scan_news,
        "apply_async",
        lambda **kwargs: queued_ids.append(kwargs["task_id"]),
    )

    task_id, state = worker.enqueue_scan()
    repeated_id, repeated_state = worker.enqueue_scan()

    assert state == "queued"
    assert repeated_state == "already_queued"
    assert repeated_id == task_id
    assert queued_ids == [task_id]
    assert worker._read_scan_status(redis)["state"] == "queued"

    completed_at = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    status = worker._complete_scan(
        redis,
        task_id,
        {"status": "completed", "discovered": 4, "events": 3},
        completed_at,
    )
    next_scan = datetime.fromisoformat(status["next_scan_at"])
    assert next_scan - completed_at == timedelta(minutes=20)
    assert status["state"] == "idle"
    assert redis.get(worker.SCAN_GATE_KEY) is None


def test_scan_loop_waits_until_due_and_bootstraps_without_state(monkeypatch):
    redis = FakeRedis()
    calls = []
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(
        worker, "enqueue_scan", lambda: (calls.append("queued") or "task-1", "queued")
    )

    result = worker.ensure_scan_loop.run()
    assert result["status"] == "queued"
    assert calls == ["queued"]

    calls.clear()
    worker._update_scan_status(
        redis,
        state="idle",
        next_scan_at=(worker.utc_now() + timedelta(minutes=5)).isoformat(),
    )
    result = worker.ensure_scan_loop.run()
    assert result["status"] == "waiting"
    assert calls == []


def test_active_scan_can_be_paused_and_resumed(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    redis.set(worker.SCAN_GATE_KEY, "scan-task")
    worker._update_scan_status(
        redis,
        state="running",
        task_id="scan-task",
        phase="extracting",
        current=2,
        total=5,
    )

    paused = worker.request_scan_pause()

    assert paused["state"] == "paused"
    assert paused["phase"] == "paused"
    assert paused["paused_from_phase"] == "extracting"
    assert redis.get(worker.SCAN_PAUSE_KEY) == b"scan-task"

    resumed = worker.resume_scan()

    assert resumed["state"] == "running"
    assert resumed["phase"] == "extracting"
    assert resumed["paused_from_phase"] is None
    assert redis.get(worker.SCAN_PAUSE_KEY) is None


def test_pause_requires_an_active_scan(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)

    with pytest.raises(RuntimeError, match="no active scan"):
        worker.request_scan_pause()


def test_worker_resumes_from_same_safe_checkpoint(monkeypatch):
    redis = FakeRedis()
    redis.set(worker.SCAN_GATE_KEY, "scan-task")
    redis.set(worker.SCAN_PAUSE_KEY, "scan-task")
    monkeypatch.setattr(worker, "sleep", lambda _: redis.delete(worker.SCAN_PAUSE_KEY))

    worker._wait_if_scan_paused(
        redis,
        "scan-task",
        phase="extracting",
        current=3,
        total=8,
    )

    status = worker._read_scan_status(redis)
    assert status["state"] == "running"
    assert status["phase"] == "extracting"
    assert status["current"] == 3
    assert status["total"] == 8


def test_each_event_queues_only_its_primary_asset_and_unmapped_is_auditable(db, monkeypatch):
    monkeypatch.setattr(
        worker.research_asset,
        "apply_async",
        lambda **kwargs: SimpleNamespace(id="research-task"),
    )
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Example Wire",
        source_quality=SourceQuality.PROFESSIONAL,
        title="A low-priority but mapped market event",
        summary="The event remains eligible for automatic research.",
        url="https://example.com/event",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"primary-asset-event").hexdigest(),
    )
    save_news(db, news)
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="other",
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        priority=0.1,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[0], relationship="direct", relevance=0.95, rationale="primary"
            ),
            CandidateAsset(
                asset=SEED_ASSETS[1], relationship="related", relevance=0.8, rationale="secondary"
            ),
        ],
        analysis_steps=[
            AnalysisStep(
                phase="event_extraction",
                executor="ollama",
                model="qwen2.5:3b",
                summary="event extracted",
                occurred_at=observed,
            )
        ],
    )
    save_event(db, event)

    queued = worker.enqueue_event_research(db, event)
    assert queued is not None
    assert list_runs(db)[0].asset.asset_id == SEED_ASSETS[0].asset_id
    assert list_runs(db)[0].historical_replay is False

    unmapped = event.model_copy(update={"id": None, "headline": "Unmapped event", "candidates": []})
    # Let Pydantic allocate a valid new ID rather than persisting a null key.
    unmapped = NewsEvent(**{**unmapped.model_dump(exclude={"id"}), "news_item_ids": [news.id]})
    save_event(db, unmapped)
    assert worker.enqueue_event_research(db, unmapped) is None

    logs = _analysis_logs(db, 10)
    unmapped_log = next(item for item in logs if item["event_id"] == str(unmapped.id))
    assert unmapped_log["status"] == "unmapped"
    assert unmapped_log["news"][0]["url"] == news.url
    assert "qwen2.5:3b" in unmapped_log["models"]
    assert "prompt" not in json.dumps(unmapped_log).lower()


def test_7b_fallback_queues_at_most_three_distinct_assets_idempotently(db, monkeypatch):
    queued_assets = []

    def apply_async(*, args, **kwargs):
        queued_assets.append(args[0])
        return SimpleNamespace(id=f"research-{len(queued_assets)}")

    monkeypatch.setattr(worker.research_asset, "apply_async", apply_async)
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    candidates = [
        CandidateAsset(
            asset=asset,
            relationship="entity",
            relevance=0.95 - index * 0.05,
            rationale="verified by master data",
        )
        for index, asset in enumerate(
            [
                *SEED_ASSETS,
            ][:4]
        )
    ]
    event = NewsEvent(
        news_item_ids=[],
        headline="Several explicitly named assets",
        event_type="other",
        direct_impact="The article names four tradable assets.",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        candidates=candidates,
    )

    first = worker.enqueue_event_researches(db, event, 3)
    repeated = worker.enqueue_event_researches(db, event, 3)

    assert len(first) == 3
    assert repeated == []
    assert queued_assets == [item.asset.asset_id for item in candidates[:3]]
    assert len(list_runs(db)) == 3


def test_distinct_events_for_same_asset_share_one_queued_research(db, monkeypatch):
    queued_tasks = []
    monkeypatch.setattr(
        worker.research_asset,
        "apply_async",
        lambda **kwargs: queued_tasks.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    candidate = CandidateAsset(
        asset=SEED_ASSETS[0],
        relationship="direct",
        relevance=1,
        rationale="verified by master data",
    )
    first = NewsEvent(
        news_item_ids=[],
        headline="Apple publishes first update",
        event_type="other",
        direct_impact="First update",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        candidates=[candidate],
    )
    second = NewsEvent(
        news_item_ids=[],
        headline="Apple publishes second update",
        event_type="other",
        direct_impact="Second update",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed + timedelta(hours=1),
        observed_at=observed + timedelta(hours=1),
        as_of=observed + timedelta(hours=1),
        candidates=[candidate],
    )

    worker.enqueue_event_researches(db, first, 1)
    worker.enqueue_event_researches(db, second, 1)

    runs = sorted(list_runs(db), key=lambda item: item.created_at)
    assert len(queued_tasks) == 1
    assert len(runs) == 2
    assert runs[0].status is RunStatus.QUEUED
    assert runs[0].trigger_event_ids == [first.id, second.id]
    assert runs[1].status is RunStatus.COALESCED
    assert runs[1].coalesced_into_run_id == runs[0].id


def test_insufficient_run_with_new_cluster_evidence_respects_asset_cooldown(
    db, monkeypatch
):
    queued_tasks = []
    monkeypatch.setattr(
        worker.research_asset,
        "apply_async",
        lambda **kwargs: queued_tasks.append(kwargs) or SimpleNamespace(id="research-task"),
    )
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    first_news = NewsItem(
        source="Wire A",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Apple reports Services revenue growth",
        url="https://a.example/apple",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"requeue-a").hexdigest(),
    )
    second_news = first_news.model_copy(
        update={
            "id": None,
            "source": "Wire B",
            "url": "https://b.example/apple",
            "content_hash": sha256(b"requeue-b").hexdigest(),
        }
    )
    second_news = NewsItem(**second_news.model_dump(exclude={"id"}))
    save_news(db, first_news)
    save_news(db, second_news)
    event = NewsEvent(
        news_item_ids=[first_news.id],
        headline=first_news.title,
        event_type="earnings",
        entities=["Apple"],
        direct_impact="Services revenue grew.",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[0],
                relationship="direct",
                relevance=1,
                rationale="AAPL is explicit",
            )
        ],
    )
    first_task = worker.enqueue_event_research(db, event)
    assert first_task is not None
    first_run = list_runs(db)[0]
    first_run.status = RunStatus.INSUFFICIENT_EVIDENCE
    first_run.completed_at = worker.utc_now()
    first_run.evidence = [
        Evidence(
            run_id=first_run.id,
            claim=first_news.title,
            source_name=first_news.source,
            source_url=first_news.url,
            source_quality=first_news.source_quality,
            published_at=observed,
            observed_at=observed,
            as_of=observed,
            excerpt=first_news.title,
            independent_group="origin:a.example",
        )
    ]
    save_run(db, first_run)
    event.news_item_ids.append(second_news.id)
    save_event(db, event)

    second_task = worker.enqueue_event_research(db, event)

    assert second_task is None
    assert len(queued_tasks) == 1
    assert all(task["queue"] == "research.research-0" for task in queued_tasks)
    assert all(task["kwargs"]["model_instance_id"] == "research-0" for task in queued_tasks)
    assert len(list_runs(db)) == 1


def test_unmapped_event_queues_only_one_visible_7b_mapping_task(db, monkeypatch):
    queued = []
    monkeypatch.setattr(
        worker.resolve_event_assets,
        "apply_async",
        lambda **kwargs: queued.append(kwargs) or SimpleNamespace(id="mapping-task"),
    )
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="Unmapped macro event",
        event_type="macro",
        direct_impact="No security is named.",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )

    first = worker.enqueue_asset_mapping(db, event)
    repeated = worker.enqueue_asset_mapping(db, event)
    forced = worker.enqueue_asset_mapping(db, event, force=True, priority=0)

    assert first == "mapping-task"
    assert repeated is None
    assert forced == "mapping-task"
    assert len(queued) == 2
    assert queued[0]["queue"] == "mapping.assist-0"
    assert queued[0]["kwargs"]["model_instance_id"] == "assist-0"
    assert queued[1]["priority"] == 0
    assert queued[1]["kwargs"]["force_mapping"] is True
    assert event.analysis_steps[-1].phase == "asset_mapping_queue"
    assert event.analysis_steps[-1].status == "queued"
    assert event.analysis_steps[-1].model == "qwen2.5:7b"


def test_stale_mapping_recovery_is_idempotent_and_requeues_once(db, monkeypatch):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="Abandoned mapping event",
        event_type="other",
        direct_impact="Mapping still required.",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    save_event(db, event)
    stale_task = SimpleNamespace(
        task_id="stale-mapping",
        entity_id=str(event.id),
        status="running",
        updated_at=worker.utc_now() - timedelta(minutes=10),
    )
    active = {"value": True}
    queued = []
    revoked = []
    redis = FakeRedis()

    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(
        worker,
        "stale_model_task_records",
        lambda *_args, **_kwargs: [stale_task] if active["value"] else [],
    )
    monkeypatch.setattr(
        worker,
        "list_model_task_records",
        lambda *_args, **_kwargs: [stale_task] if active["value"] else [],
    )
    monkeypatch.setattr(
        worker,
        "enqueue_asset_mapping",
        lambda *_args, **kwargs: queued.append(kwargs) or "replacement-task",
    )

    def cancel(_lane, task_id, **_kwargs):
        if task_id == "stale-mapping" and active["value"]:
            active["value"] = False
            return True
        return task_id == "replacement-task"

    monkeypatch.setattr(worker, "cancel_model_task", cancel)
    monkeypatch.setattr(worker, "_revoke_model_task", revoked.append)

    first = worker.reconcile_asset_mapping_leases.run()
    second = worker.reconcile_asset_mapping_leases.run()

    assert first["requeued"] == 1
    assert second["stale"] == 0
    assert len(queued) == 1
    assert queued[0]["force"] is True
    assert revoked == ["stale-mapping"]


def test_stale_mapping_recovery_cancels_terminal_event_without_requeue(db, monkeypatch):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="Already unmapped event",
        event_type="other",
        direct_impact="No asset exists.",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        analysis_steps=[
            AnalysisStep(
                phase="asset_mapping",
                status="unmapped",
                executor="ollama+provider-registry",
                summary="No verified security.",
            )
        ],
    )
    save_event(db, event)
    stale_task = SimpleNamespace(
        task_id="terminal-stale",
        entity_id=str(event.id),
        status="running",
        updated_at=worker.utc_now() - timedelta(minutes=10),
    )
    redis = FakeRedis()
    revoked = []

    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(worker, "stale_model_task_records", lambda *_args, **_kwargs: [stale_task])
    monkeypatch.setattr(worker, "list_model_task_records", lambda *_args, **_kwargs: [stale_task])
    monkeypatch.setattr(worker, "cancel_model_task", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(worker, "_revoke_model_task", revoked.append)
    monkeypatch.setattr(
        worker,
        "enqueue_asset_mapping",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("terminal event was requeued")
        ),
    )

    result = worker.reconcile_asset_mapping_leases.run()

    assert result["cancelled"] == 1
    assert result["requeued"] == 0
    assert revoked == ["terminal-stale"]


def test_stale_mapping_recovery_does_nothing_when_redis_is_unavailable(monkeypatch):
    monkeypatch.setattr(
        worker,
        "_redis_client",
        lambda: (_ for _ in ()).throw(ConnectionError("redis unavailable")),
    )

    result = worker.reconcile_asset_mapping_leases.run()

    assert result["status"] == "redis_unavailable"
    assert result["requeued"] == 0


def test_cancelled_mapping_message_redelivery_stops_before_database_work(monkeypatch):
    selected = SimpleNamespace(id="assist-0")
    monkeypatch.setattr(worker, "select_model_instance", lambda *_args, **_kwargs: selected)
    monkeypatch.setattr(worker, "update_instance_assignment", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker, "update_model_task", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker, "model_task_is_cancelled", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        worker,
        "init_db",
        lambda: (_ for _ in ()).throw(AssertionError("cancelled task reached database")),
    )

    result = worker.resolve_event_assets.run(
        "00000000-0000-0000-0000-000000000001",
        model_instance_id="assist-0",
    )

    assert result == {
        "status": "cancelled",
        "event_id": "00000000-0000-0000-0000-000000000001",
    }


def test_standalone_news_retry_is_recorded_and_sent_with_requested_priority(
    monkeypatch,
):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Example",
        title="Retry this durable news item",
        url="https://example.com/retry-news",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"retry-news").hexdigest(),
    )
    recorded = []
    queued = []
    monkeypatch.setattr(
        worker,
        "record_model_task",
        lambda lane, **kwargs: recorded.append((lane, kwargs)),
    )
    monkeypatch.setattr(
        worker.retry_news_item,
        "apply_async",
        lambda **kwargs: queued.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )

    task_id = worker.enqueue_news_extraction_retry(news, priority=0)

    assert recorded[0][0] == "extract"
    assert recorded[0][1]["entity_id"] == str(news.id)
    assert recorded[0][1]["source"] == "manual"
    assert queued == [
        {
            "args": [str(news.id)],
            "kwargs": {"model_instance_id": "extract-0"},
            "queue": "extract.extract-0",
            "task_id": task_id,
            "priority": 0,
        }
    ]


def test_standalone_news_rescan_forces_downstream_7b_mapping(db, monkeypatch):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Example",
        title="Force this news through mapping",
        url="https://example.com/force-news-mapping",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"force-news-mapping").hexdigest(),
    )
    save_news(db, news)
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="other",
        direct_impact=news.title,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[0],
                relationship="direct",
                relevance=0.95,
                rationale="deterministic mapping",
            )
        ],
    )
    captured = []
    monkeypatch.setattr(worker, "init_db", lambda: None)
    monkeypatch.setattr(worker.settings, "auto_research", False)
    monkeypatch.setattr(worker, "model_task_is_cancelled", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(worker, "update_model_task", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker.EventService,
        "ingest",
        lambda *_args, **_kwargs: [event],
    )
    monkeypatch.setattr(
        worker,
        "enqueue_asset_mapping",
        lambda _db, queued_event, **kwargs: (
            captured.append((queued_event.id, kwargs)) or "mapping-task"
        ),
    )

    result = worker.retry_news_item.run(
        str(news.id),
        force_asset_mapping=True,
    )

    assert result["status"] == "completed"
    assert result["asset_mapping_queued"] == 1
    assert captured == [(event.id, {"force": True})]


def test_forced_7b_mapping_replaces_candidates_then_queues_target_event_report(
    db, monkeypatch
):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Four companies named in one filing",
        summary="The source explicitly names four companies.",
        url="https://example.com/four-companies",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"four-companies").hexdigest(),
    )
    save_news(db, news)
    candidates = [
        CandidateAsset(
            asset=asset,
            relationship="entity",
            relevance=0.95 - index * 0.05,
            rationale="verified",
        )
        for index, asset in enumerate(SEED_ASSETS[:4])
    ]
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="other",
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        candidates=[
            CandidateAsset(
                asset=SEED_ASSETS[-1],
                relationship="direct",
                relevance=0.7,
                rationale="preexisting deterministic mapping",
            )
        ],
    )
    save_event(db, event)
    monkeypatch.setattr(
        worker.AssetMappingService,
        "map_event",
        lambda *args, **kwargs: AssetMappingResult(
            candidates=candidates,
            proposed_count=4,
        ),
    )
    queued_reports = []
    monkeypatch.setattr(
        worker,
        "enqueue_event_report",
        lambda _db, mapped_event: (
            queued_reports.append(mapped_event.id)
            or ("event-report-task", EventResearchRun(event_id=mapped_event.id))
        ),
    )

    result = worker.resolve_event_assets.run(str(event.id), force_mapping=True)

    db.expire_all()
    persisted = get_event(db, event.id)
    assert result["verified_assets"] == 4
    assert result["status"] == "event_research_queued"
    assert result["task_id"] == "event-report-task"
    assert persisted is not None
    assert len(persisted.candidates) == 4
    assert queued_reports == [event.id]


def test_target_research_queues_only_impacts_that_pass_v2_gate(db, monkeypatch):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="目标门槛测试",
        event_type="other",
        direct_impact="测试",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    tradeable = TargetImpact(
        target_type=TargetType.TRADABLE_ASSET,
        target_name=SEED_ASSETS[0].name,
        asset=SEED_ASSETS[0],
        direction=1,
        score=0.3,
        rating=Rating.BULLISH,
        confidence=0.7,
        trade_status=TradeStatus.TRADEABLE,
        execution_supported=True,
    )
    blocked = tradeable.model_copy(
        update={
            "target_name": SEED_ASSETS[1].name,
            "asset": SEED_ASSETS[1],
            "score": 0.1,
            "rating": Rating.WATCH,
            "trade_status": TradeStatus.UNTRADEABLE,
        }
    )
    captured = []
    monkeypatch.setattr(worker, "get_run_for_event_asset", lambda *_args: None)
    monkeypatch.setattr(
        worker,
        "enqueue_research",
        lambda _db, asset, queued_event, **_kwargs: (
            captured.append((asset.asset_id, queued_event.id))
            or ("research-task", object())
        ),
    )

    queued = worker.enqueue_target_researches(
        db,
        event,
        EventReport(
            summary="逐目标",
            scoring_version="target-transmission-v2",
            impacts=[tradeable, blocked],
            trade_status=TradeStatus.TRADEABLE,
        ),
    )

    assert len(queued) == 1
    assert captured == [(SEED_ASSETS[0].asset_id, event.id)]
