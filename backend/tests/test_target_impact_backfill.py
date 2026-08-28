from datetime import UTC, datetime
from hashlib import sha256
from types import SimpleNamespace

from backend.app.domain import (
    ActionStage,
    EventAction,
    EventReport,
    EventResearchRun,
    NewsEvent,
    NewsItem,
    RunStatus,
    SourceQuality,
)
from backend.app.services.macro_impacts import TARGET_SCORING_VERSION
from backend.app.storage import (
    get_event_research_for_event,
    save_event,
    save_event_research_run,
    save_news,
)
from backend.app.target_impact_backfill import reprocess_target_impacts


def test_target_v2_backfill_is_previewable_point_in_time_and_idempotent(db, monkeypatch):
    observed = datetime(2025, 8, 24, 12, tzinfo=UTC)
    news = NewsItem(
        source="Official",
        source_quality=SourceQuality.OFFICIAL,
        title="伊朗谴责美国新一轮制裁",
        summary="制裁范围尚未说明。",
        url="https://official.example/iran",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"target-v2-backfill").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="regulation",
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    legacy_report = EventReport(summary="旧版中性事件研报")
    legacy_run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.COMPLETED,
        as_of=observed,
        report=legacy_report,
    )
    save_news(db, news)
    save_event(db, event)
    save_event_research_run(db, legacy_run)

    dispatched = []
    monkeypatch.setattr(
        "backend.app.target_impact_backfill.select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(
        "backend.app.worker._record_research_dispatch",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "backend.app.worker._clear_research_dispatch",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "backend.app.worker.research_event.apply_async",
        lambda **kwargs: dispatched.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )

    preview = reprocess_target_impacts(apply=False, batch_size=10, max_active=10)
    assert preview["pending"] == 1
    assert preview["selected"] == 1
    assert preview["dry_run"] is True

    applied = reprocess_target_impacts(apply=True, batch_size=10, max_active=10)
    assert applied["queued"] == 1
    assert len(dispatched) == 1
    queued = get_event_research_for_event(db, event.id)
    assert queued is not None
    assert queued.historical_replay is True
    assert queued.as_of == observed
    assert queued.report is None
    assert queued.report_history == [legacy_report]

    repeated = reprocess_target_impacts(apply=True, batch_size=10, max_active=10)
    assert repeated["selected"] == 0
    assert repeated["queued"] == 0
    assert len(dispatched) == 1

    queued.status = RunStatus.COMPLETED
    queued.report = EventReport(
        summary="v2 逐目标报告",
        scoring_version=TARGET_SCORING_VERSION,
    )
    save_event_research_run(db, queued)
    complete = reprocess_target_impacts(apply=False, batch_size=10, max_active=10)
    assert complete["pending"] == 0
    assert complete["failed"] == 0
    assert complete["complete"] is True


def test_target_v2_backfill_queue_failure_can_be_retried(db, monkeypatch):
    observed = datetime(2025, 8, 24, 12, tzinfo=UTC)
    event = NewsEvent(
        news_item_ids=[],
        headline="伊朗谴责美国施压",
        event_type="regulation",
        actions=[
            EventAction(
                actor="伊朗",
                action_type="condemnation",
                action_stage=ActionStage.STATEMENT,
                action="谴责",
                strength=0.15,
            )
        ],
        direct_impact="外交表态",
        source_quality=SourceQuality.OFFICIAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    failed_run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.FAILED,
        as_of=observed,
        error="legacy failure",
    )
    save_event(db, event)
    save_event_research_run(db, failed_run)
    monkeypatch.setattr(
        "backend.app.target_impact_backfill.select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(
        "backend.app.worker._record_research_dispatch",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "backend.app.worker._clear_research_dispatch",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "backend.app.worker.research_event.apply_async",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("broker unavailable")),
    )

    first = reprocess_target_impacts(apply=True, batch_size=10, max_active=10)

    assert first["queue_failures"] == 1
    restored = get_event_research_for_event(db, event.id)
    assert restored is not None
    assert restored.status is RunStatus.FAILED
    assert restored.error == "legacy failure"

    dispatched = []
    monkeypatch.setattr(
        "backend.app.worker.research_event.apply_async",
        lambda **kwargs: dispatched.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )
    second = reprocess_target_impacts(apply=True, batch_size=10, max_active=10)

    assert second["queued"] == 1
    assert len(dispatched) == 1
