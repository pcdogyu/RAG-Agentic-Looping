from datetime import timedelta
from uuid import uuid4

from backend.app.config import Settings
from backend.app.domain import ResearchRun, RunStatus, utc_now
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.research_lifecycle import (
    compact_queued_research_runs,
    reconcile_stale_research_runs,
)
from backend.app.storage import get_run, save_run


class EmptyRedis:
    def scan_iter(self, _pattern):
        return []


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
