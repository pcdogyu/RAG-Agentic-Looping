from __future__ import annotations

import json
import threading
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta

from redis import Redis
from sqlalchemy.orm import Session

from backend.app.config import Settings
from backend.app.domain import AnalysisStep, ResearchRun, RunStatus, as_utc, utc_now
from backend.app.storage import (
    list_active_runs,
    list_event_research_runs,
    list_queued_runs,
    save_event_research_run,
    save_run,
)

LEASE_PREFIX = "market-loop:research:lease:"
RESEARCH_TIME_LIMIT_REASON = "research_time_limit"


def research_lease_key(run_id: str) -> str:
    return f"{LEASE_PREFIX}{run_id}"


def live_research_run_ids(redis_client: Redis | None) -> set[str] | None:
    if redis_client is None:
        return None
    try:
        return {
            key.decode().rsplit(":", 1)[-1] if isinstance(key, bytes) else str(key).rsplit(":", 1)[-1]
            for key in redis_client.scan_iter(f"{LEASE_PREFIX}*")
        }
    except Exception:
        return None


@contextmanager
def research_lease(
    redis_client: Redis | None,
    *,
    run_id: str,
    task_id: str,
    settings: Settings,
) -> Iterator[None]:
    stop = threading.Event()
    key = research_lease_key(run_id)

    def refresh() -> None:
        if redis_client is None:
            return
        payload = json.dumps(
            {"run_id": run_id, "task_id": task_id, "updated_at": utc_now().isoformat()}
        )
        try:
            redis_client.set(key, payload, ex=settings.research_lease_seconds)
        except Exception:
            return

    def heartbeat() -> None:
        while not stop.wait(settings.research_heartbeat_seconds):
            refresh()

    refresh()
    thread = threading.Thread(target=heartbeat, name=f"research-lease-{run_id}", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1)
        if redis_client is not None:
            try:
                redis_client.delete(key)
            except Exception:
                pass


def mark_asset_research_timed_out(
    db: Session,
    run: ResearchRun,
    settings: Settings,
    *,
    limit_kind: str,
    now: datetime | None = None,
) -> bool:
    """Persist one active asset run as a retryable end-to-end timeout."""

    if run.status not in {RunStatus.RUNNING, RunStatus.VERIFYING}:
        return False
    current = as_utc(now or utc_now())
    run.status = RunStatus.FAILED
    run.completed_at = current
    run.retryable_reason = RESEARCH_TIME_LIMIT_REASON
    run.error = (
        "标的研究超过全流程时限（软超时34分钟，硬超时35分钟），可重新执行。 / "
        "Asset research exceeded the end-to-end time limit "
        "(34-minute soft limit, 35-minute hard limit) and can be retried."
    )
    run.analysis_steps.append(
        AnalysisStep(
            phase="research_time_limit",
            status="failed",
            executor="research-lifecycle",
            summary=(
                "标的研究达到全流程时间限制，已安全终止并转入可重新执行状态。 / "
                "Asset research reached its end-to-end time limit and was moved "
                "to a retryable failed state."
            ),
            metrics={
                "limit_kind": limit_kind,
                "soft_limit_seconds": settings.research_asset_soft_time_limit_seconds,
                "hard_limit_seconds": settings.research_asset_hard_time_limit_seconds,
            },
        )
    )
    save_run(db, run)
    return True


def reconcile_stale_research_runs(
    db: Session,
    redis_client: Redis | None,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> int:
    current = as_utc(now or utc_now())
    cutoff = current - timedelta(seconds=settings.research_lease_seconds)
    live_ids = live_research_run_ids(redis_client)
    if live_ids is None:
        return 0
    repaired = 0
    for run in list_active_runs(db):
        if run.status not in {RunStatus.RUNNING, RunStatus.VERIFYING}:
            continue
        if str(run.id) in live_ids or as_utc(run.updated_at) > cutoff:
            continue
        hard_timed_out = bool(
            run.started_at is not None
            and current - as_utc(run.started_at)
            >= timedelta(seconds=settings.research_asset_hard_time_limit_seconds)
        )
        if hard_timed_out:
            repaired += int(
                mark_asset_research_timed_out(
                    db,
                    run,
                    settings,
                    limit_kind="hard",
                    now=current,
                )
            )
            continue
        run.status = RunStatus.FAILED
        run.completed_at = current
        run.retryable_reason = "stale_worker_lease"
        run.error = "Research worker lease expired"
        run.analysis_steps.append(
            AnalysisStep(
                phase="research_lease",
                status="failed",
                executor="research-lifecycle",
                summary="研究 Worker 租约已失效，任务已转入可重新执行状态。",
            )
        )
        save_run(db, run)
        repaired += 1
    for run in list_event_research_runs(db, 5000):
        if run.status not in {RunStatus.RUNNING, RunStatus.VERIFYING}:
            continue
        if str(run.id) in live_ids or as_utc(run.updated_at) > cutoff:
            continue
        run.status = RunStatus.FAILED
        run.retryable_reason = "stale_worker_lease"
        run.error = "Research worker lease expired"
        run.analysis_steps.append(
            AnalysisStep(
                phase="research_lease",
                status="failed",
                executor="research-lifecycle",
                summary="事件研究 Worker 租约已失效，任务已转入可重新执行状态。",
            )
        )
        save_event_research_run(db, run)
        repaired += 1
    return repaired


def compact_queued_research_runs(
    db: Session,
    settings: Settings,
    *,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict[str, int]:
    current = as_utc(now or utc_now())
    grouped: dict[str, list[ResearchRun]] = defaultdict(list)
    for run in list_queued_runs(db):
        if run.historical_replay or run.retry_of_run_id is not None:
            continue
        grouped[run.asset.asset_id].append(run)

    canonical_count = 0
    coalesced_count = 0
    for runs in grouped.values():
        canonical: ResearchRun | None = None
        window_end: datetime | None = None
        for run in sorted(runs, key=lambda item: as_utc(item.created_at)):
            if canonical is None or window_end is None or as_utc(run.created_at) > window_end:
                if canonical is not None and not dry_run:
                    save_run(db, canonical)
                canonical = run
                window_end = as_utc(run.created_at) + timedelta(
                    hours=settings.research_coalesce_window_hours
                )
                canonical_count += 1
                continue
            canonical.trigger_event_ids = list(
                dict.fromkeys([*canonical.trigger_event_ids, *run.trigger_event_ids])
            )
            coalesced_count += 1
            if dry_run:
                continue
            run.status = RunStatus.COALESCED
            run.coalesced_into_run_id = canonical.id
            run.completed_at = current
            run.analysis_steps.append(
                AnalysisStep(
                    phase="research_coalescing",
                    executor="research-lifecycle",
                    summary=f"该任务已合并到同标的主研究任务 {canonical.id}。",
                    metrics={"canonical_run_id": str(canonical.id)},
                )
            )
            save_run(db, run)
        if canonical is not None and not dry_run:
            save_run(db, canonical)
    return {
        "scanned": sum(len(items) for items in grouped.values()),
        "canonical": canonical_count,
        "coalesced": coalesced_count,
    }
