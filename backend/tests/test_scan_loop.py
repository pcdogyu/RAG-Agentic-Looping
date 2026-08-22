import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import SimpleNamespace

from backend.app import worker
from backend.app.domain import AnalysisStep, CandidateAsset, NewsEvent, NewsItem, SourceQuality
from backend.app.main import _analysis_logs
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.asset_mapping import AssetMappingResult
from backend.app.storage import get_event, list_runs, save_event, save_news


class FakeRedis:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return False
        self.data[key] = value.encode() if isinstance(value, str) else value
        return True

    def delete(self, key):
        self.data.pop(key, None)

    def expire(self, key, seconds):
        return key in self.data


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
    assert next_scan - completed_at == timedelta(minutes=10)
    assert status["state"] == "idle"
    assert redis.get(worker.SCAN_GATE_KEY) is None


def test_scan_loop_waits_until_due_and_bootstraps_without_state(monkeypatch):
    redis = FakeRedis()
    calls = []
    monkeypatch.setattr(worker, "_redis_client", lambda: redis)
    monkeypatch.setattr(worker, "enqueue_scan", lambda: (calls.append("queued") or "task-1", "queued"))

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

    unmapped = event.model_copy(
        update={"id": None, "headline": "Unmapped event", "candidates": []}
    )
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

    assert first == "mapping-task"
    assert repeated is None
    assert len(queued) == 1
    assert event.analysis_steps[-1].phase == "asset_mapping_queue"
    assert event.analysis_steps[-1].status == "queued"


def test_7b_mapping_task_persists_candidates_and_queues_top_three(db, monkeypatch):
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
    queued_assets = []
    monkeypatch.setattr(
        worker.research_asset,
        "apply_async",
        lambda *, args, **kwargs: queued_assets.append(args[0])
        or SimpleNamespace(id=f"research-{len(queued_assets)}"),
    )

    result = worker.resolve_event_assets.run(str(event.id))

    db.expire_all()
    persisted = get_event(db, event.id)
    assert result["verified_assets"] == 4
    assert result["research_queued"] == 3
    assert persisted is not None
    assert len(persisted.candidates) == 4
    assert queued_assets == [item.asset.asset_id for item in candidates[:3]]
