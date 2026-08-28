from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier, Lock
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.app import worker
from backend.app.db import SessionLocal
from backend.app.domain import ResearchRun, RunStatus
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.research_admission import (
    ResearchAdmissionError,
    asset_research_admission_lock,
)
from backend.app.storage import list_runs, save_run


def _patch_research_queue(monkeypatch):
    queued = []
    queued_lock = Lock()

    monkeypatch.setattr(
        worker,
        "select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )

    def apply_async(**kwargs):
        with queued_lock:
            queued.append(kwargs)
        return SimpleNamespace(id=kwargs["task_id"])

    monkeypatch.setattr(worker.research_asset, "apply_async", apply_async)
    monkeypatch.setattr(worker, "_record_research_dispatch", lambda *_args, **_kwargs: False)
    return queued


@pytest.mark.parametrize(
    "status",
    [RunStatus.COMPLETED, RunStatus.INSUFFICIENT_EVIDENCE],
)
def test_completed_research_blocks_until_rolling_cooldown_expires(
    db, monkeypatch, status
):
    queued = _patch_research_queue(monkeypatch)
    now = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker, "utc_now", lambda: now)
    prior = ResearchRun(
        asset=SEED_ASSETS[0],
        status=status,
        completed_at=now - timedelta(hours=23, minutes=59),
    )
    save_run(db, prior)

    with pytest.raises(ResearchAdmissionError) as captured:
        worker.enqueue_research(db, prior.asset)

    assert captured.value.code == "research_cooldown_active"
    assert captured.value.run_id == str(prior.id)
    assert captured.value.eligible_at == prior.completed_at + timedelta(hours=24)
    assert queued == []

    prior.completed_at = now - timedelta(hours=24)
    save_run(db, prior)
    task_id, run = worker.enqueue_research(db, prior.asset)

    assert task_id == run.celery_task_id
    assert len(queued) == 1


@pytest.mark.parametrize(
    "status,historical_replay",
    [
        (RunStatus.FAILED, False),
        (RunStatus.CANCELLED, False),
        (RunStatus.COALESCED, False),
        (RunStatus.COMPLETED, True),
    ],
)
def test_nonqualifying_terminal_runs_do_not_start_cooldown(
    db, monkeypatch, status, historical_replay
):
    queued = _patch_research_queue(monkeypatch)
    now = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker, "utc_now", lambda: now)
    prior = ResearchRun(
        asset=SEED_ASSETS[0],
        status=status,
        historical_replay=historical_replay,
        completed_at=now - timedelta(minutes=1),
    )
    save_run(db, prior)

    worker.enqueue_research(db, prior.asset)

    assert len(queued) == 1


def test_only_retry_and_force_bypass_cooldown(
    db, monkeypatch
):
    queued = _patch_research_queue(monkeypatch)
    now = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(worker, "utc_now", lambda: now)
    prior = ResearchRun(
        asset=SEED_ASSETS[0],
        status=RunStatus.COMPLETED,
        completed_at=now - timedelta(minutes=5),
    )
    save_run(db, prior)

    with pytest.raises(ResearchAdmissionError):
        worker.enqueue_research(db, prior.asset, market_factor_refresh_days=1)

    _, retry = worker.enqueue_research(
        db,
        prior.asset,
        retry_of_run_id=uuid4(),
        retry_attempt=1,
    )
    retry.status = RunStatus.FAILED
    retry.completed_at = now
    save_run(db, retry)

    _, forced = worker.enqueue_research(db, prior.asset, force_research=True)
    forced.status = RunStatus.FAILED
    forced.completed_at = now
    save_run(db, forced)

    with pytest.raises(ResearchAdmissionError):
        worker.enqueue_research(db, prior.asset, historical_replay=True)

    assert len(queued) == 2


@pytest.mark.parametrize(
    "status",
    [RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING],
)
def test_active_research_blocks_every_admission_override(db, monkeypatch, status):
    _patch_research_queue(monkeypatch)
    active = ResearchRun(asset=SEED_ASSETS[0], status=status)
    save_run(db, active)

    attempts = (
        {"force_research": True},
        {"historical_replay": True},
        {"retry_of_run_id": uuid4(), "retry_attempt": 1},
    )
    for options in attempts:
        with pytest.raises(ResearchAdmissionError) as captured:
            worker.enqueue_research(db, active.asset, **options)
        assert captured.value.code == "research_already_active"
        assert captured.value.run_id == str(active.id)


def test_sqlite_asset_lock_allows_only_one_concurrent_queue_insert(monkeypatch):
    queued = _patch_research_queue(monkeypatch)
    barrier = Barrier(2)

    def submit():
        with SessionLocal() as session:
            barrier.wait()
            try:
                return worker.enqueue_research(session, SEED_ASSETS[0])[1].id
            except ResearchAdmissionError as exc:
                return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result() for future in [pool.submit(submit), pool.submit(submit)]]

    assert len(queued) == 1
    assert results.count("research_already_active") == 1
    with SessionLocal() as session:
        assert len(list_runs(session)) == 1


def test_postgres_admission_uses_asset_transaction_advisory_lock():
    statements = []

    class FakeSession:
        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def execute(self, statement, parameters):
            statements.append((str(statement), parameters))

        def rollback(self):
            raise AssertionError("successful lock scope must not roll back")

    with asset_research_admission_lock(FakeSession(), "equity:XNAS:AAPL"):
        pass

    assert "pg_advisory_xact_lock" in statements[0][0]
    assert statements[0][1] == {"asset_id": "equity:XNAS:AAPL"}
