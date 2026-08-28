from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

from backend.app import worker
from backend.app.config import Settings
from backend.app.domain import EventResearchRun, ResearchRun, RunStatus, utc_now
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.research_lifecycle import (
    compact_queued_research_runs,
    reconcile_stale_research_runs,
)
from backend.app.storage import (
    get_event_research_run,
    get_run,
    save_event_research_run,
    save_run,
)


class EmptyRedis:
    def scan_iter(self, _pattern):
        return []


class MemoryRedis(EmptyRedis):
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, **_kwargs):
        self.values[key] = value
        return True

    def delete(self, key):
        return int(self.values.pop(key, None) is not None)


def test_compacts_only_queued_runs_inside_rolling_window(db):
    start = utc_now() - timedelta(hours=30)
    event_ids = [uuid4(), uuid4(), uuid4()]
    runs = [
        ResearchRun(
            event_id=event_id,
            asset=SEED_ASSETS[0],
            created_at=start + offset,
            updated_at=start + offset,
        )
        for event_id, offset in zip(
            event_ids,
            (timedelta(), timedelta(hours=23), timedelta(hours=25)),
            strict=True,
        )
    ]
    for run in runs:
        save_run(db, run)

    settings = Settings(_env_file=None, research_coalesce_window_hours=24)
    preview = compact_queued_research_runs(db, settings, dry_run=True)
    assert preview == {"scanned": 3, "canonical": 2, "coalesced": 1}
    assert get_run(db, runs[1].id).status is RunStatus.QUEUED

    result = compact_queued_research_runs(db, settings)
    canonical = get_run(db, runs[0].id)
    merged = get_run(db, runs[1].id)
    successor = get_run(db, runs[2].id)
    assert result == preview
    assert canonical.trigger_event_ids == event_ids[:2]
    assert merged.status is RunStatus.COALESCED
    assert merged.coalesced_into_run_id == canonical.id
    assert successor.status is RunStatus.QUEUED


def test_reconciles_stale_active_run_into_manual_retry_state(db):
    run = ResearchRun(asset=SEED_ASSETS[0], status=RunStatus.RUNNING)
    save_run(db, run)
    settings = Settings(_env_file=None, research_lease_seconds=120)

    repaired = reconcile_stale_research_runs(
        db,
        EmptyRedis(),
        settings,
        now=utc_now() + timedelta(minutes=3),
    )

    stored = get_run(db, run.id)
    assert repaired == 1
    assert stored.status is RunStatus.FAILED
    assert stored.retryable_reason == "stale_worker_lease"


def test_recovers_queued_runs_when_redis_dispatch_markers_are_missing(
    db, monkeypatch
):
    now = utc_now()
    asset_run = ResearchRun(
        asset=SEED_ASSETS[0],
        celery_task_id="lost-asset-task",
        model_instance_id="research-0",
        created_at=now - timedelta(minutes=5),
    )
    event_run = EventResearchRun(
        event_id=uuid4(),
        celery_task_id=None,
        model_instance_id="research-0",
        created_at=now - timedelta(minutes=5),
    )
    save_run(db, asset_run)
    save_event_research_run(db, event_run)

    published = []
    monkeypatch.setattr(
        worker,
        "select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(
        worker.research_asset,
        "apply_async",
        lambda **kwargs: published.append(("asset", kwargs)),
    )
    monkeypatch.setattr(
        worker.research_event,
        "apply_async",
        lambda **kwargs: published.append(("event", kwargs)),
    )
    redis = MemoryRedis()
    settings = Settings(_env_file=None, research_lease_seconds=120)

    result = worker.recover_orphaned_queued_research_runs(
        db,
        redis,
        settings,
        now=now,
    )

    stored_asset = get_run(db, asset_run.id)
    stored_event = get_event_research_run(db, event_run.id)
    assert result == {
        "queued_scanned": 2,
        "queued_recovered": 2,
        "recovery_failed": 0,
    }
    assert {kind for kind, _kwargs in published} == {"asset", "event"}
    assert stored_asset.celery_task_id != "lost-asset-task"
    assert stored_event.celery_task_id is not None
    assert stored_asset.analysis_steps[-1].phase == "research_dispatch_recovery"
    assert stored_event.analysis_steps[-1].phase == "research_dispatch_recovery"
    assert all(kwargs["queue"] == "research.research-0" for _, kwargs in published)
    priorities = {kind: kwargs["priority"] for kind, kwargs in published}
    assert priorities == {
        "asset": worker.ASSET_RESEARCH_PRIORITY,
        "event": worker.EVENT_RESEARCH_PRIORITY,
    }

    repeated = worker.recover_orphaned_queued_research_runs(
        db,
        redis,
        settings,
        now=now,
    )
    assert repeated == {
        "queued_scanned": 2,
        "queued_recovered": 0,
        "recovery_failed": 0,
    }
    assert len(published) == 2


def test_asset_research_ignores_a_superseded_celery_delivery(db):
    run = ResearchRun(
        asset=SEED_ASSETS[0],
        celery_task_id="current-asset-task",
        model_instance_id="research-0",
    )
    save_run(db, run)

    stale = worker.research_asset.apply(
        args=[run.asset.asset_id, None, str(run.id)],
        kwargs={"model_instance_id": "research-0"},
        task_id="superseded-asset-task",
        throw=True,
    ).get()

    assert stale["superseded_task_id"] == "superseded-asset-task"
