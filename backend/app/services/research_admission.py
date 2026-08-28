from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Lock, RLock

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.domain import ResearchRun, RunStatus, as_utc, utc_now
from backend.app.storage import get_latest_cooldown_run, list_active_runs_for_asset


class ResearchAdmissionError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        run_id: str | None = None,
        eligible_at: datetime | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.run_id = run_id
        self.eligible_at = eligible_at

    def detail(self) -> dict[str, str]:
        payload = {"code": self.code, "message": self.message}
        if self.run_id is not None:
            payload["active_run_id" if self.code == "research_already_active" else "run_id"] = (
                self.run_id
            )
        if self.eligible_at is not None:
            payload["eligible_at"] = as_utc(self.eligible_at).isoformat()
        return payload


_asset_locks_guard = Lock()
_asset_locks: dict[str, RLock] = {}


def _process_asset_lock(asset_id: str) -> RLock:
    with _asset_locks_guard:
        return _asset_locks.setdefault(asset_id, RLock())


@contextmanager
def asset_research_admission_lock(db: Session, asset_id: str) -> Iterator[None]:
    """Serialize admission per asset in every supported database deployment."""

    dialect = db.get_bind().dialect.name
    if dialect == "postgresql":
        db.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:asset_id, 0))"),
            {"asset_id": asset_id},
        )
        try:
            yield
        except Exception:
            db.rollback()
            raise
        return

    # SQLite has no row/advisory locks. The application and tests use a process
    # lock so the check-and-insert section is still atomic inside one service.
    with _process_asset_lock(asset_id):
        yield


def active_asset_research(db: Session, asset_id: str) -> ResearchRun | None:
    runs = list_active_runs_for_asset(db, asset_id)
    return next(
        (run for run in runs if run.status in {RunStatus.RUNNING, RunStatus.VERIFYING}),
        runs[0] if runs else None,
    )


def enforce_asset_research_admission(
    db: Session,
    asset_id: str,
    *,
    cooldown_hours: int,
    now: datetime | None = None,
    bypass_cooldown: bool = False,
    check_active: bool = True,
) -> None:
    current = as_utc(now or utc_now())
    if check_active and (active := active_asset_research(db, asset_id)) is not None:
        raise ResearchAdmissionError(
            "research_already_active",
            "该标的已有排队中或执行中的研究任务。",
            run_id=str(active.id),
        )
    if bypass_cooldown or cooldown_hours <= 0:
        return

    cutoff = current - timedelta(hours=cooldown_hours)
    recent = get_latest_cooldown_run(db, asset_id, completed_after=cutoff)
    if recent is None or recent.completed_at is None:
        return
    eligible_at = as_utc(recent.completed_at) + timedelta(hours=cooldown_hours)
    raise ResearchAdmissionError(
        "research_cooldown_active",
        f"该标的在过去 {cooldown_hours} 小时内已经完成过研究。",
        run_id=str(recent.id),
        eligible_at=eligible_at,
    )
