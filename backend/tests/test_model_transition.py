from datetime import UTC, datetime

from sqlalchemy import select

from backend.app.db import EventResearchRunRow, ResearchRunRow, SessionLocal
from backend.app.domain import (
    AssetClass,
    AssetRef,
    EventResearchRun,
    EventType,
    Market,
    NewsEvent,
    ResearchRun,
    RunStatus,
    SourceQuality,
)
from backend.app.model_transition import transition_active_research
from backend.app.storage import save_event, save_event_research_run, save_run
from backend.app.worker import research_event


def test_active_model_transition_preserves_original_and_creates_7b_retry(db, monkeypatch):
    now = datetime(2026, 8, 27, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="公司发布公告",
        event_type=EventType.EARNINGS,
        direct_impact="利润增长",
        source_quality=SourceQuality.OFFICIAL,
        published_at=now,
        observed_at=now,
        as_of=now,
    )
    save_event(db, event)
    asset = AssetRef(
        asset_id="cn:600000",
        asset_class=AssetClass.EQUITY,
        market=Market.CN,
        symbol="600000",
        name="测试公司",
        exchange_or_provider="SSE",
        currency="CNY",
    )
    original = ResearchRun(
        event_id=event.id,
        asset=asset,
        status=RunStatus.RUNNING,
        celery_task_id="old-asset-task",
    )
    event_run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.VERIFYING,
        celery_task_id="old-event-task",
    )
    save_run(db, original)
    save_event_research_run(db, event_run)

    def fake_enqueue_research(session, asset, event, **kwargs):
        retry = ResearchRun(
            event_id=event.id,
            asset=asset,
            celery_task_id="new-asset-task",
            retry_of_run_id=kwargs["retry_of_run_id"],
            retry_attempt=kwargs["retry_attempt"],
        )
        save_run(session, retry)
        return "new-asset-task", retry

    def fake_enqueue_event(session, _event, run):
        run.status = RunStatus.QUEUED
        run.celery_task_id = "new-event-task"
        save_event_research_run(session, run)
        return "new-event-task", run

    monkeypatch.setattr(
        "backend.app.model_transition.enqueue_research", fake_enqueue_research
    )
    monkeypatch.setattr(
        "backend.app.model_transition.enqueue_event_research_retry", fake_enqueue_event
    )

    result = transition_active_research(apply=True)

    assert result["asset_runs"] == 1
    assert result["event_runs"] == 1
    assert {item["task_id"] for item in result["queued"]} == {
        "new-asset-task",
        "new-event-task",
    }
    with SessionLocal() as session:
        asset_rows = list(session.scalars(select(ResearchRunRow)).all())
        original_payload = next(row.payload for row in asset_rows if str(row.id) == str(original.id))
        retry_payload = next(row.payload for row in asset_rows if str(row.id) != str(original.id))
        stored_event = session.scalar(select(EventResearchRunRow))
    assert original_payload["status"] == "cancelled"
    assert retry_payload["retry_of_run_id"] == str(original.id)
    assert retry_payload["analysis_steps"][-1]["model"] == "qwen2.5:7b"
    assert stored_event.payload["status"] == "queued"
    assert stored_event.payload["celery_task_id"] == "new-event-task"

    stale = research_event.apply(
        args=[str(event.id), str(event_run.id)],
        task_id="old-event-task",
        throw=True,
    ).get()
    assert stale["superseded_task_id"] == "old-event-task"
