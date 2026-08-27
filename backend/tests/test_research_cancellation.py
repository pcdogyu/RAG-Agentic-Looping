from uuid import uuid4

from backend.app import main
from backend.app.domain import EventResearchRun, ResearchRun, RunStatus
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.model_queue import ModelQueueTask
from backend.app.services.research_cancellation import cancel_research_tasks
from backend.app.storage import get_event_research_run, get_run, save_event_research_run, save_run


def test_cancel_asset_card_cancels_all_active_runs_and_late_save_cannot_restore(db):
    first = ResearchRun(
        asset=SEED_ASSETS[0], status=RunStatus.QUEUED, celery_task_id="queued-task"
    )
    second = ResearchRun(
        asset=SEED_ASSETS[0], status=RunStatus.RUNNING, celery_task_id="running-task"
    )
    untouched = ResearchRun(asset=SEED_ASSETS[1], status=RunStatus.QUEUED)
    for run in (first, second, untouched):
        save_run(db, run)
    stale_worker_copy = second.model_copy(deep=True)

    result = cancel_research_tasks(
        db,
        kind="asset_research",
        entity_id=first.asset.asset_id,
        task_id="synthetic-card-id",
    )
    stale_worker_copy.status = RunStatus.COMPLETED
    save_run(db, stale_worker_copy)

    db.expire_all()
    assert result.cancelled == 2
    assert result.counts_by_status == {"queued": 1, "running": 1}
    assert result.celery_task_ids == ["queued-task", "running-task"]
    assert get_run(db, first.id).status is RunStatus.CANCELLED
    assert get_run(db, second.id).status is RunStatus.CANCELLED
    assert get_run(db, untouched.id).status is RunStatus.QUEUED


def test_clear_cancels_asset_and_event_research_but_not_terminal_runs(db):
    active_asset = ResearchRun(asset=SEED_ASSETS[0], status=RunStatus.VERIFYING)
    completed_asset = ResearchRun(asset=SEED_ASSETS[1], status=RunStatus.COMPLETED)
    active_event = EventResearchRun(event_id=uuid4(), status=RunStatus.QUEUED)
    for run in (active_asset, completed_asset):
        save_run(db, run)
    save_event_research_run(db, active_event)

    result = cancel_research_tasks(db)

    assert result.cancelled == 2
    assert result.asset_runs == 1
    assert result.event_runs == 1
    assert get_run(db, active_asset.id).status is RunStatus.CANCELLED
    assert get_run(db, completed_asset.id).status is RunStatus.COMPLETED
    assert get_event_research_run(db, active_event.id).status is RunStatus.CANCELLED


def test_cancel_and_clear_endpoints_revoke_only_research_tasks(db, monkeypatch):
    first = ResearchRun(
        asset=SEED_ASSETS[0], status=RunStatus.QUEUED, celery_task_id="research-one"
    )
    second = ResearchRun(
        asset=SEED_ASSETS[1], status=RunStatus.QUEUED, celery_task_id="research-two"
    )
    save_run(db, first)
    save_run(db, second)
    revoked: list[str] = []
    monkeypatch.setattr(
        main,
        "_revoke_research_tasks",
        lambda task_ids: revoked.extend(task_ids) or len(task_ids),
    )
    monkeypatch.setattr(main, "_purge_research_queue", lambda: 1)
    monkeypatch.setattr(main, "_mark_model_queue_snapshot_stale", lambda: None)

    cancelled = main.cancel_research_task(
        main.ResearchCancellationRequest(
            task_id="asset-card", kind="asset_research", entity_id=first.asset.asset_id
        ),
        db,
    )
    cleared = main.clear_research_tasks(db)

    assert cancelled["cancelled"] == 1
    assert cleared["cancelled"] == 1
    assert cleared["purged"] == 1
    assert revoked == ["research-one", "research-two"]


def test_generic_clear_endpoint_uses_only_the_selected_model_queue(db, monkeypatch):
    purged: list[str] = []
    revoked: list[str] = []
    monkeypatch.setattr(
        main,
        "clear_news_extraction_queue",
        lambda: {"cancelled": 2, "celery_task_ids": ["extract-1", "extract-2"]},
    )
    monkeypatch.setattr(
        main, "_purge_model_queue", lambda queue: purged.append(queue) or 3
    )
    monkeypatch.setattr(
        main,
        "_revoke_model_tasks",
        lambda task_ids: revoked.extend(task_ids) or len(task_ids),
    )
    monkeypatch.setattr(main, "_mark_model_queue_snapshot_stale", lambda: None)

    result = main.clear_model_queue("extract", db)

    assert result == {
        "queue_id": "extract",
        "cancelled": 2,
        "purged": 3,
        "revoked": 2,
    }
    assert purged == ["extract"]
    assert revoked == ["extract-1", "extract-2"]


def test_cancel_asset_mapping_endpoint_cancels_and_revokes_one_task(monkeypatch):
    cancelled: list[tuple[str, str]] = []
    revoked: list[str] = []
    refreshed: list[bool] = []
    monkeypatch.setattr(
        main,
        "cancel_model_task",
        lambda lane, task_id: cancelled.append((lane, task_id)) or True,
    )
    monkeypatch.setattr(
        main,
        "_revoke_model_tasks",
        lambda task_ids: revoked.extend(task_ids) or len(task_ids),
    )
    monkeypatch.setattr(
        main,
        "_mark_model_queue_snapshot_stale",
        lambda: refreshed.append(True),
    )

    result = main.cancel_asset_mapping_task(
        main.ResearchCancellationRequest(
            task_id="mapping-task", kind="asset_mapping", entity_id="event-one"
        )
    )

    assert result == {
        "queue_id": "assist",
        "cancelled": 1,
        "revoked": 1,
    }
    assert cancelled == [("assist", "mapping-task")]
    assert revoked == ["mapping-task"]
    assert refreshed == [True]


def _failed_model_task(task_id: str = "failed-mapping") -> ModelQueueTask:
    now = main.utc_now()
    return ModelQueueTask(
        task_id=task_id,
        kind="asset_mapping",
        entity_id=f"event-{task_id}",
        title="映射失败新闻",
        status="failed",
        queued_at=now,
        completed_at=now,
        updated_at=now,
        error="模型响应暂时不可解析",
    )


def test_single_model_retry_uses_highest_priority(db, monkeypatch):
    task = _failed_model_task()
    queued: list[tuple[str, int]] = []
    monkeypatch.setattr(main, "_retryable_model_queue_tasks", lambda _queue: [task])
    monkeypatch.setattr(
        main,
        "_enqueue_model_task_retry",
        lambda _db, *, queue_id, task, priority: queued.append(
            (task.task_id, priority)
        )
        or "replacement-task",
    )
    monkeypatch.setattr(main, "_mark_model_queue_snapshot_stale", lambda: None)

    result = main.retry_model_queue_task(
        "assist",
        main.ModelTaskRetryRequest(
            task_id=task.task_id,
            kind=task.kind,
            entity_id=task.entity_id,
        ),
        db,
    )

    assert queued == [(task.task_id, main.MANUAL_RETRY_PRIORITY)]
    assert result["priority"] == "highest"
    assert result["task_ids"] == ["replacement-task"]


def test_bulk_model_retry_requeues_every_error_task_at_normal_priority(db, monkeypatch):
    tasks = [_failed_model_task("first"), _failed_model_task("second")]
    priorities: list[int] = []
    monkeypatch.setattr(main, "_retryable_model_queue_tasks", lambda _queue: tasks)
    monkeypatch.setattr(
        main,
        "_enqueue_model_task_retry",
        lambda _db, *, queue_id, task, priority: priorities.append(priority)
        or f"retry-{task.task_id}",
    )
    monkeypatch.setattr(main, "_mark_model_queue_snapshot_stale", lambda: None)

    result = main.retry_model_queue_tasks("assist", db)

    assert priorities == [main.BULK_RETRY_PRIORITY, main.BULK_RETRY_PRIORITY]
    assert result["requested"] == 2
    assert result["retried"] == 2
    assert result["skipped"] == 0
