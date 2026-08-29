from datetime import UTC, datetime
from types import SimpleNamespace

from backend.app import worker
from backend.app.domain import (
    AnalysisStep,
    EventReport,
    EventResearchRun,
    EventType,
    NewsEvent,
    RunStatus,
    SourceQuality,
)
from backend.app.research_priority_migration import (
    MIGRATION_PHASE,
    migrate_queued_event_priorities,
)
from backend.app.storage import (
    get_event_research_run,
    save_event,
    save_event_research_run,
)


def _event(headline: str) -> NewsEvent:
    now = datetime(2026, 8, 28, tzinfo=UTC)
    return NewsEvent(
        news_item_ids=[],
        headline=headline,
        event_type=EventType.OTHER,
        direct_impact="待确认",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
    )


def test_event_priority_migration_preserves_run_and_is_idempotent(db, monkeypatch):
    event = _event("旧优先级事件")
    save_event(db, event)
    run = EventResearchRun(
        event_id=event.id,
        celery_task_id="old-task",
        model_instance_id="research-0",
        retry_count=2,
        analysis_steps=[
            AnalysisStep(phase="original", executor="test", summary="保留")
        ],
    )
    save_event_research_run(db, run)
    published = []
    monkeypatch.setattr(
        "backend.app.research_priority_migration.select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(
        "backend.app.research_priority_migration._record_research_dispatch",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "backend.app.research_priority_migration._clear_research_dispatch",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "backend.app.research_priority_migration.research_event.apply_async",
        lambda **kwargs: published.append(kwargs),
    )

    preview = migrate_queued_event_priorities()
    assert preview["dry_run"] is True
    assert preview["requested"] == 1
    assert preview["requeued"] == 0
    assert get_event_research_run(db, run.id).celery_task_id == "old-task"

    applied = migrate_queued_event_priorities(apply=True)
    stored = get_event_research_run(db, run.id)
    assert applied["requeued"] == 1
    assert applied["failed"] == 0
    assert stored.id == run.id
    assert stored.event_id == run.event_id
    assert stored.as_of == run.as_of
    assert stored.retry_count == 2
    assert stored.celery_task_id != "old-task"
    assert stored.analysis_steps[-1].phase == MIGRATION_PHASE
    assert published[0]["priority"] == 1

    repeated = migrate_queued_event_priorities(apply=True)
    assert repeated["requested"] == 1
    assert repeated["requeued"] == 0
    assert repeated["skipped"] == 1
    assert len(published) == 1


def test_event_priority_migration_continues_after_one_publish_failure(db, monkeypatch):
    runs = []
    for headline in ("事件一", "事件二"):
        event = _event(headline)
        save_event(db, event)
        run = EventResearchRun(event_id=event.id, celery_task_id=f"old-{headline}")
        save_event_research_run(db, run)
        runs.append(run)

    published = []
    monkeypatch.setattr(
        "backend.app.research_priority_migration.select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(
        "backend.app.research_priority_migration._record_research_dispatch",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "backend.app.research_priority_migration._clear_research_dispatch",
        lambda *_args, **_kwargs: None,
    )

    def publish(**kwargs):
        if not published:
            published.append("failed")
            raise RuntimeError("broker unavailable")
        published.append(kwargs)

    monkeypatch.setattr(
        "backend.app.research_priority_migration.research_event.apply_async", publish
    )

    result = migrate_queued_event_priorities(apply=True)

    assert result["failed"] == 1
    assert result["requeued"] == 1
    first = get_event_research_run(db, runs[0].id)
    second = get_event_research_run(db, runs[1].id)
    assert first.celery_task_id == runs[0].celery_task_id
    assert first.status is RunStatus.QUEUED
    assert second.celery_task_id != runs[1].celery_task_id


def test_event_retry_cannot_be_demoted_by_caller_priority(db, monkeypatch):
    event = _event("失败事件")
    save_event(db, event)
    run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.FAILED,
        retryable_reason="model_timeout",
    )
    save_event_research_run(db, run)
    published = []
    monkeypatch.setattr(
        worker,
        "select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(worker, "_record_research_dispatch", lambda *_args: True)
    monkeypatch.setattr(
        worker.research_event,
        "apply_async",
        lambda **kwargs: published.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )

    worker.enqueue_event_research_retry(db, event, run, priority=9)

    assert published[0]["priority"] == worker.EVENT_RESEARCH_PRIORITY == 1
    stored = get_event_research_run(db, run.id)
    assert stored.analysis_steps[-1].metrics["priority"] == 1


def test_event_refresh_archives_and_keeps_published_report(db, monkeypatch):
    event = _event("已发布事件")
    save_event(db, event)
    report = EventReport(
        summary="继续显示的旧研报",
        confidence=0.74,
        evidence_complete=True,
    )
    run = EventResearchRun(
        event_id=event.id,
        status=RunStatus.COMPLETED,
        report=report,
    )
    save_event_research_run(db, run)
    published = []
    monkeypatch.setattr(
        worker,
        "select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(worker, "_record_research_dispatch", lambda *_args: True)
    monkeypatch.setattr(
        worker.research_event,
        "apply_async",
        lambda **kwargs: published.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )

    task_id, queued = worker.enqueue_event_research_refresh(db, event, run)

    assert queued.status is RunStatus.QUEUED
    assert queued.report == report
    assert queued.report_history == [report]
    assert queued.analysis_steps[-1].phase == "forced_event_research_queue"
    assert published[0]["args"] == [str(event.id), str(run.id)]
    assert published[0]["task_id"] == task_id
    assert published[0]["priority"] == worker.EVENT_RESEARCH_PRIORITY
    stored = get_event_research_run(db, run.id)
    assert stored.report.summary == "继续显示的旧研报"
    assert stored.report_history[-1].summary == "继续显示的旧研报"
