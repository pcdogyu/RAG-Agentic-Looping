from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.app.domain import (
    EventResearchRun,
    EventType,
    NewsEvent,
    NewsItem,
    Rating,
    Recommendation,
    ResearchRun,
    RunStatus,
    SourceQuality,
    Thesis,
    utc_now,
)
from backend.app.main import app
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.research_queue import build_research_queue
from backend.app.storage import (
    list_recommendations,
    save_event,
    save_event_research_run,
    save_recommendation,
    save_run,
)


def recommendation_for(run: ResearchRun, *, score: int = 0) -> Recommendation:
    return Recommendation(
        run_id=run.id,
        asset=run.asset,
        score=score,
        rating=Rating.WATCH,
        confidence=0.45,
        bull_probability=0.2,
        base_probability=0.6,
        bear_probability=0.2,
        thesis=Thesis(summary="Existing execution result"),
        as_of=run.as_of,
        evidence_complete=False,
    )


def test_health_and_asset_endpoints():
    with TestClient(app) as client:
        health = client.get("/health")
        assets = client.get("/api/v1/assets")
        scan = client.get("/api/v1/scan/status")
        analysis_logs = client.get("/api/v1/analysis-logs")
        event_runs = client.get("/api/v1/event-research-runs")

    assert health.status_code == 200
    assert health.json()["database"] is True
    assert assets.status_code == 200
    assert any(item["asset_id"] == "crypto:coingecko:bitcoin" for item in assets.json())
    assert scan.status_code == 200
    assert scan.json()["interval_seconds"] == 1200
    assert "server_time" in scan.json()
    assert analysis_logs.status_code == 200
    assert analysis_logs.json() == []
    assert event_runs.status_code == 200
    assert event_runs.json() == []


def test_health_lists_dedicated_assist_and_research_instances(monkeypatch):
    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    monkeypatch.setattr(
        "backend.app.main.settings.ollama_assist_base_url", "http://assist.invalid"
    )
    monkeypatch.setattr(
        "backend.app.main.settings.ollama_research_base_urls",
        "http://research-0.invalid,http://research-1.invalid",
    )
    monkeypatch.setattr(
        "backend.app.main.httpx.get",
        lambda url, **_kwargs: Response(
            {"models": [{"name": "qwen2.5:7b"}]}
            if url.endswith("/api/tags")
            else {"models": [{"name": "qwen2.5:7b"}]}
        ),
    )

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    instances = {item["id"]: item for item in response.json()["ollama_instances"]}
    assert {
        "extract-0",
        "assist-0",
        "research-0",
        "research-1",
        "code-0",
    }.issubset(instances)
    assert all(
        instances[item]["model_available"]
        for item in ("assist-0", "research-0", "research-1")
    )
    assert not instances["extract-0"]["model_available"]
    assert not instances["code-0"]["model_available"]


def test_scan_pause_and_resume_endpoints(monkeypatch):
    status = {
        "state": "paused",
        "task_id": "scan-task",
        "phase": "paused",
        "paused_from_phase": "extracting",
        "current": 1,
        "total": 3,
        "server_time": "2026-08-22T12:00:00+00:00",
    }
    monkeypatch.setattr("backend.app.main.request_scan_pause", lambda: None)
    monkeypatch.setattr("backend.app.main.resume_scan", lambda: None)
    monkeypatch.setattr("backend.app.main.get_scan_status", lambda: status)

    with TestClient(app) as client:
        paused = client.post("/api/v1/scan/pause")
        status["state"] = "running"
        status["phase"] = "extracting"
        resumed = client.post("/api/v1/scan/resume")

    assert paused.status_code == 200
    assert paused.json()["scan"]["state"] == "paused"
    assert resumed.status_code == 200
    assert resumed.json()["scan"]["state"] == "running"


def test_synchronous_scan_applies_title_filter_before_event_ingest(monkeypatch):
    items = [
        NewsItem(
            source="test",
            title="今日天气预报",
            url="https://example.com/weather",
            published_at=utc_now(),
            content_hash=sha256(b"weather-api-scan").hexdigest(),
        ),
        NewsItem(
            source="test",
            title="上市公司发布业绩公告",
            url="https://example.com/earnings",
            published_at=utc_now(),
            content_hash=sha256(b"earnings-api-scan").hexdigest(),
        ),
    ]
    ingested: list[NewsItem] = []

    class RegistryStub:
        def all_assets(self):
            return []

        def discover_news(self, **_kwargs):
            return items

    class EventServiceStub:
        def __init__(self, _registry):
            pass

        def ingest(self, _db, accepted):
            ingested.extend(accepted)
            return []

    monkeypatch.setattr("backend.app.main._provider_registry", lambda _db: RegistryStub())
    monkeypatch.setattr("backend.app.main.EventService", EventServiceStub)

    with TestClient(app) as client:
        configured = client.put(
            "/api/v1/source-filter",
            json={
                "enabled": True,
                "whitelist_keywords": ["业绩"],
                "blacklist_keywords": ["天气"],
            },
        )
        response = client.post("/api/v1/scan", json={"background": False})

    assert configured.status_code == 200
    assert response.status_code == 200
    assert response.json() == {"news": 2, "accepted": 1, "filtered": 1, "events": 0}
    assert [item.title for item in ingested] == ["上市公司发布业绩公告"]


def test_failed_asset_research_can_be_requeued_with_latest_data(db, monkeypatch):
    original = ResearchRun(
        asset=SEED_ASSETS[1],
        status=RunStatus.FAILED,
        error="TimeoutError: model timed out",
    )
    save_run(db, original)
    captured = {}

    def fake_enqueue(_db, asset, event, **kwargs):
        captured.update({"asset": asset, "event": event, **kwargs})
        retry = ResearchRun(
            asset=asset,
            retry_of_run_id=kwargs["retry_of_run_id"],
            retry_attempt=kwargs["retry_attempt"],
        )
        return "retry-task", retry

    monkeypatch.setattr("backend.app.main.enqueue_research", fake_enqueue)

    with TestClient(app) as client:
        failed = client.get("/api/v1/failed-research-runs")
        response = client.post(f"/api/v1/research-runs/{original.id}/retry")

    assert failed.status_code == 200
    assert failed.json()[0]["id"] == str(original.id)
    assert failed.json()[0]["asset"]["symbol"] == "600519"
    assert response.status_code == 202
    assert response.json()["retry_of_run_id"] == str(original.id)
    assert response.json()["retry_attempt"] == 1
    assert captured["historical_replay"] is False
    assert captured["as_of"] > original.as_of


def test_failed_asset_research_is_hidden_when_result_already_exists(db):
    original = ResearchRun(
        asset=SEED_ASSETS[1],
        status=RunStatus.FAILED,
        error="IntegrityError: recommendation already exists",
    )
    save_run(db, original)
    save_recommendation(db, recommendation_for(original))

    with TestClient(app) as client:
        response = client.get("/api/v1/failed-research-runs")
        bulk = client.post("/api/v1/failed-research-runs/retry")

    assert response.status_code == 200
    assert response.json() == []
    assert bulk.json()["requested"] == 0


def test_failed_asset_research_is_hidden_while_retry_is_active(db):
    original = ResearchRun(
        asset=SEED_ASSETS[1],
        status=RunStatus.FAILED,
        error="TimeoutError: model timed out",
    )
    retry = ResearchRun(
        asset=original.asset,
        status=RunStatus.QUEUED,
        retry_of_run_id=original.id,
        retry_attempt=1,
    )
    save_run(db, original)
    save_run(db, retry)

    with TestClient(app) as client:
        response = client.get("/api/v1/failed-research-runs")
        bulk = client.post("/api/v1/failed-research-runs/retry")

    assert response.status_code == 200
    assert response.json() == []
    assert bulk.json()["requested"] == 0


def test_bulk_failed_research_retry_includes_every_kind_beyond_page_limit(
    db, monkeypatch
):
    originals = []
    for index in range(51):
        asset = SEED_ASSETS[0].model_copy(
            update={
                "asset_id": f"test:bulk:{index:02d}",
                "symbol": f"B{index:02d}",
                "name": f"Bulk asset {index:02d}",
            }
        )
        run = ResearchRun(
            asset=asset,
            status=RunStatus.FAILED,
            error="TimeoutError: model timed out",
        )
        save_run(db, run)
        originals.append(run)

    now = utc_now()
    event = NewsEvent(
        news_item_ids=[],
        headline="Bulk retry event",
        event_type=EventType.OTHER,
        direct_impact="Event report retry",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
    )
    save_event(db, event)
    event_run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.FAILED,
        error="TimeoutError: event report timed out",
    )
    save_event_research_run(db, event_run)
    queued_order = []

    def fake_enqueue_asset(_db, asset, queued_event, **kwargs):
        queued_order.append(("asset", kwargs["retry_of_run_id"]))
        retry = ResearchRun(
            asset=asset,
            event_id=queued_event.id if queued_event else None,
            status=RunStatus.QUEUED,
            retry_of_run_id=kwargs["retry_of_run_id"],
            retry_attempt=kwargs["retry_attempt"],
        )
        save_run(_db, retry)
        return f"asset-task-{retry.id}", retry

    def fake_enqueue_event(_db, queued_event, queued_run):
        queued_order.append(("event", queued_run.id))
        queued_run.status = RunStatus.QUEUED
        queued_run.retry_count += 1
        save_event_research_run(_db, queued_run)
        return f"event-task-{queued_run.id}", queued_run

    def fail_lineage_scan(*_args, **_kwargs):
        raise AssertionError("bulk retry must reuse the shared lineage snapshot")

    monkeypatch.setattr("backend.app.main.enqueue_research", fake_enqueue_asset)
    monkeypatch.setattr(
        "backend.app.main.enqueue_event_research_retry", fake_enqueue_event
    )
    monkeypatch.setattr("backend.app.main.list_retries_for_run", fail_lineage_scan)

    with TestClient(app) as client:
        visible = client.get("/api/v1/failed-research-runs?limit=50")
        response = client.post("/api/v1/failed-research-runs/retry")
        remaining = client.get("/api/v1/failed-research-runs?limit=200")
        repeated = client.post("/api/v1/failed-research-runs/retry")

    assert len(visible.json()) == 50
    assert response.status_code == 202
    assert response.json()["requested"] == 52
    assert response.json()["retried"] == 52
    assert response.json()["skipped"] == 0
    assert response.json()["failed"] == 0
    assert queued_order[0] == ("asset", originals[0].id)
    assert queued_order[-1] == ("event", event_run.id)
    assert remaining.json() == []
    assert repeated.json()["requested"] == 0
    assert repeated.json()["results"] == []


def test_bulk_failed_research_retry_continues_after_skips_and_failures(db, monkeypatch):
    runs = []
    for index in range(3):
        run = ResearchRun(
            asset=SEED_ASSETS[0].model_copy(
                update={
                    "asset_id": f"test:partial:{index}",
                    "symbol": f"P{index}",
                }
            ),
            status=RunStatus.FAILED,
            error="model timeout",
        )
        save_run(db, run)
        runs.append(run)

    calls = []

    def fake_retry(run_id, _db, _known_retries):
        calls.append(run_id)
        if run_id == runs[0].id:
            raise HTTPException(409, "retry already active")
        if run_id == runs[1].id:
            raise RuntimeError("broker unavailable")
        return {
            "task_id": "queued-task",
            "run_id": str(uuid4()),
            "status": "queued",
        }

    monkeypatch.setattr("backend.app.main._queue_asset_research_retry", fake_retry)

    with TestClient(app) as client:
        response = client.post("/api/v1/failed-research-runs/retry")

    assert response.status_code == 202
    assert calls == [item.id for item in runs]
    assert response.json()["requested"] == 3
    assert response.json()["retried"] == 1
    assert response.json()["skipped"] == 1
    assert response.json()["failed"] == 1
    assert [item["status"] for item in response.json()["results"]] == [
        "skipped",
        "failed",
        "queued",
    ]


def test_recommendation_save_is_idempotent_for_run(db):
    run = ResearchRun(asset=SEED_ASSETS[1])
    first = recommendation_for(run)
    replacement = recommendation_for(run, score=10)

    save_recommendation(db, first)
    save_recommendation(db, replacement)

    saved = list_recommendations(db)
    assert len(saved) == 1
    assert saved[0].id == first.id
    assert saved[0].score == 10
    assert replacement.id == first.id


def test_non_failed_research_cannot_be_requeued(db):
    run = ResearchRun(asset=SEED_ASSETS[1], status=RunStatus.COMPLETED)
    save_run(db, run)

    with TestClient(app) as client:
        response = client.post(f"/api/v1/research-runs/{run.id}/retry")

    assert response.status_code == 409


def test_research_queue_aggregates_active_runs_and_applies_status_priority(db):
    now = utc_now()
    shared_asset = SEED_ASSETS[1]
    running_asset = SEED_ASSETS[0].model_copy(
        update={"asset_id": "test:running", "symbol": "RUN", "name": "Running"}
    )
    waiting_asset = SEED_ASSETS[0].model_copy(
        update={"asset_id": "test:waiting", "symbol": "WAIT", "name": "Waiting"}
    )
    runs = [
        ResearchRun(
            asset=shared_asset,
            status=RunStatus.QUEUED,
            created_at=now - timedelta(minutes=20),
        ),
        ResearchRun(
            asset=shared_asset,
            status=RunStatus.VERIFYING,
            created_at=now - timedelta(minutes=10),
        ),
        ResearchRun(
            asset=running_asset,
            status=RunStatus.RUNNING,
            created_at=now - timedelta(minutes=15),
        ),
        ResearchRun(
            asset=waiting_asset,
            status=RunStatus.QUEUED,
            created_at=now - timedelta(minutes=30),
        ),
        ResearchRun(asset=SEED_ASSETS[2], status=RunStatus.COMPLETED),
    ]
    for run in runs:
        save_run(db, run)

    with TestClient(app) as client:
        response = client.get("/api/v1/research-queue?limit=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload["model"] == "qwen2.5:7b"
    assert payload["total_assets"] == 3
    assert payload["total_runs"] == 4
    assert payload["counts"] == {"queued": 2, "running": 1, "verifying": 1}
    assert payload["truncated"] is True
    assert [item["status"] for item in payload["items"]] == ["verifying", "running"]
    assert payload["items"][0]["asset_id"] == shared_asset.asset_id
    assert payload["items"][0]["task_count"] == 2


def test_news_extraction_queue_is_public_and_preserves_model_and_counts(monkeypatch):
    now = utc_now().isoformat()
    monkeypatch.setattr(
        "backend.app.main.get_news_extraction_queue",
        lambda limit: {
            "generated_at": now,
            "model": "qwen2.5:3b",
            "scan_task_id": "scan-1",
            "state": "running",
            "total_items": 3,
            "counts": {
                "queued": 1,
                "running": 1,
                "retrying": 0,
                "completed": 1,
                "failed": 0,
            },
            "truncated": False,
            "items": [
                {
                    "task_id": "extract-1",
                    "news_id": "00000000-0000-0000-0000-000000000001",
                    "title": "测试新闻",
                    "source": "金十",
                    "published_at": now,
                    "status": "running",
                    "attempt": 1,
                    "queued_at": now,
                    "updated_at": now,
                    "error": None,
                }
            ],
            "error": None,
        },
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/news-extraction-queue?limit=25")

    assert response.status_code == 200
    assert response.json()["model"] == "qwen2.5:3b"
    assert response.json()["counts"]["completed"] == 1
    assert response.json()["items"][0]["title"] == "测试新闻"


def test_additional_model_inference_queues_are_public(monkeypatch):
    def queue_status(model, *_args, lane=None, **_kwargs):
        if lane == "assist":
            return {
                "lane": "assist",
                "capacity": 1,
                "queued": 2,
                "running": 1,
                "available": 0,
                "observable": True,
            }
        return {
            "lane": "code",
            "capacity": 1,
            "queued": 0,
            "running": 0,
            "available": 1,
            "observable": True,
        }

    monkeypatch.setattr("backend.app.main.gateway.gpu.queue_status", queue_status)
    monkeypatch.setattr(
        "backend.app.main.gateway.num_threads_for", lambda _model, **_kwargs: 8
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/model-inference-queues")

    assert response.status_code == 200
    payload = response.json()
    assert [item["model"] for item in payload["items"]] == [
        "qwen2.5:7b",
        "qwen2.5-coder:7b",
    ]
    assert payload["items"][0]["state"] == "queued"
    assert payload["items"][0]["threads"] == 8
    assert payload["items"][0]["purpose"] == "股票映射"
    assert payload["items"][0]["binding"] == "新闻事件二次股票映射"
    assert payload["items"][0]["task_enabled"] is True
    assert payload["items"][1]["state"] == "idle"


def test_unified_model_queue_overview_is_public_and_ordered(monkeypatch):
    now = utc_now().isoformat()
    build_calls = 0

    def queue(queue_id, model):
        return {
            "id": queue_id,
            "model": model,
            "purpose": "test",
            "binding": "test binding",
            "enabled": queue_id != "code",
            "state": "disabled" if queue_id == "code" else "idle",
            "threads": 4,
            "capacity": 1,
            "available": 1,
            "instance_count": 1,
            "per_instance_concurrency": 1,
            "observable": True,
            "counts": {
                "queued": 0,
                "running": 0,
                "retrying": 0,
                "verifying": 0,
                "waiting_for_model": 0,
                "completed": 0,
                "failed": 0,
            },
            "metrics": {
                "average_queue_duration_ms": None,
                "average_execution_duration_ms": None,
                "longest_wait_ms": None,
                "estimated_clear_ms": None,
                "queue_duration_sample_count": 0,
                "execution_duration_sample_count": 0,
            },
            "total_tasks": 0,
            "truncated": False,
            "tasks": [],
            "error": None,
        }

    def build_overview(*_args, **_kwargs):
        nonlocal build_calls
        build_calls += 1
        return {
            "generated_at": now,
            "queues": [
                queue("extract", "qwen2.5:3b"),
                queue("research", "qwen2.5:7b"),
                queue("assist", "qwen2.5:7b"),
                queue("code", "qwen2.5-coder:7b"),
            ],
        }

    monkeypatch.setattr("backend.app.main.build_model_queue_overview", build_overview)

    with TestClient(app) as client:
        response = client.get("/api/v1/model-queue-overview?limit=500")
        cached_response = client.get("/api/v1/model-queue-overview?limit=1")

    assert response.status_code == 200
    assert cached_response.status_code == 200
    assert build_calls == 1
    payload = response.json()
    assert [item["id"] for item in payload["queues"]] == [
        "extract",
        "research",
        "assist",
        "code",
    ]
    assert payload["queues"][2]["model"] == "qwen2.5:7b"
    assert all(item["instance_count"] == 1 for item in payload["queues"])
    assert all(item["per_instance_concurrency"] == 1 for item in payload["queues"])
    assert payload["queues"][3]["enabled"] is False


def test_research_queue_orders_waiting_assets_oldest_first_and_can_be_empty(db):
    now = utc_now()
    newer = SEED_ASSETS[0].model_copy(
        update={"asset_id": "test:newer", "symbol": "NEW", "name": "Newer"}
    )
    older = SEED_ASSETS[0].model_copy(
        update={"asset_id": "test:older", "symbol": "OLD", "name": "Older"}
    )

    with TestClient(app) as client:
        empty = client.get("/api/v1/research-queue")
    assert empty.json()["items"] == []
    assert empty.json()["total_assets"] == 0

    save_run(
        db,
        ResearchRun(
            asset=newer,
            status=RunStatus.QUEUED,
            created_at=now - timedelta(minutes=5),
        ),
    )
    save_run(
        db,
        ResearchRun(
            asset=older,
            status=RunStatus.QUEUED,
            created_at=now - timedelta(minutes=10),
        ),
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/research-queue")

    assert [item["symbol"] for item in response.json()["items"]] == ["OLD", "NEW"]


def test_research_queue_reports_task_timing_and_uses_current_status_representative():
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    asset = SEED_ASSETS[0]
    older_verifying = ResearchRun(
        asset=asset,
        status=RunStatus.VERIFYING,
        created_at=now - timedelta(minutes=20),
        started_at=now - timedelta(minutes=15),
        updated_at=now - timedelta(minutes=5),
    )
    current_verifying = ResearchRun(
        asset=asset,
        status=RunStatus.VERIFYING,
        created_at=now - timedelta(minutes=12),
        started_at=now - timedelta(minutes=10),
        updated_at=now - timedelta(minutes=1),
    )
    waiting = ResearchRun(
        asset=SEED_ASSETS[1],
        status=RunStatus.QUEUED,
        created_at=now - timedelta(minutes=6),
        updated_at=now - timedelta(minutes=6),
    )
    legacy_running = ResearchRun(
        asset=SEED_ASSETS[2],
        status=RunStatus.RUNNING,
        created_at=now - timedelta(minutes=30),
        updated_at=now - timedelta(minutes=2),
    )

    payload = build_research_queue(
        [older_verifying, current_verifying, waiting, legacy_running],
        500,
        "qwen2.5:7b",
        generated_at=now,
    )

    shared = next(item for item in payload.items if item.asset_id == asset.asset_id)
    assert shared.task_count == 2
    assert shared.representative_queued_at == current_verifying.created_at
    assert shared.queue_duration_ms == 2 * 60 * 1000
    assert shared.execution_duration_ms == 10 * 60 * 1000
    assert payload.queue_duration_sample_count == 3
    assert payload.execution_duration_sample_count == 2
    assert payload.average_queue_duration_ms == (300000 + 120000 + 360000) // 3
    assert payload.average_execution_duration_ms == (900000 + 600000) // 2
    legacy = next(item for item in payload.items if item.asset_id == legacy_running.asset.asset_id)
    assert legacy.queue_duration_ms is None
    assert legacy.execution_duration_ms is None


def test_failed_event_research_can_be_requeued(db, monkeypatch):
    now = utc_now()
    event = NewsEvent(
        news_item_ids=[],
        headline="行业政策发生变化",
        event_type=EventType.REGULATION,
        direct_impact="行业层面影响",
        source_quality=SourceQuality.OFFICIAL,
        published_at=now,
        observed_at=now,
        as_of=now,
    )
    save_event(db, event)
    run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.FAILED,
        error="RuntimeError: report failed",
    )
    save_event_research_run(db, run)

    def fake_retry(_db, queued_event, queued_run):
        assert queued_event.id == event.id
        queued_run.status = RunStatus.QUEUED
        queued_run.retry_count += 1
        return "event-retry-task", queued_run

    monkeypatch.setattr("backend.app.main.enqueue_event_research_retry", fake_retry)

    with TestClient(app) as client:
        response = client.post(f"/api/v1/event-research-runs/{run.id}/retry")

    assert response.status_code == 202
    assert response.json()["run_id"] == str(run.id)
    assert response.json()["retry_count"] == 1
