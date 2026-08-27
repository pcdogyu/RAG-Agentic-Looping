from __future__ import annotations

from collections import Counter
from datetime import timedelta
from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from backend.app.db import EventResearchRunRow, ResearchRunRow
from backend.app.domain import AnalysisStep, EventResearchRun, ResearchRun, RunStatus, utc_now
from backend.app.services.model_instances import (
    instance_assignment,
    update_instance_assignment,
)

ACTIVE_RESEARCH_STATUSES = {
    RunStatus.QUEUED.value,
    RunStatus.RUNNING.value,
    RunStatus.VERIFYING.value,
}
FAILED_RESEARCH_CLEAR_WINDOW = timedelta(hours=24)


class ResearchCancellationResult(BaseModel):
    cancelled: int
    asset_runs: int
    event_runs: int
    counts_by_status: dict[str, int]
    celery_task_ids: list[str]


def _cancel_asset_run(run: ResearchRun, reason: str) -> ResearchRun:
    previous_status = run.status.value
    now = utc_now()
    run.status = RunStatus.CANCELLED
    run.error = None
    run.retryable_reason = None
    run.completed_at = now
    run.updated_at = now
    run.analysis_steps.append(
        AnalysisStep(
            phase="research_cancelled",
            status="cancelled",
            executor="admin-api",
            summary=reason,
            metrics={"previous_status": previous_status},
        )
    )
    return run


def _cancel_event_run(run: EventResearchRun, reason: str) -> EventResearchRun:
    previous_status = run.status.value
    run.status = RunStatus.CANCELLED
    run.error = None
    run.retryable_reason = None
    run.updated_at = utc_now()
    run.analysis_steps.append(
        AnalysisStep(
            phase="research_cancelled",
            status="cancelled",
            executor="admin-api",
            summary=reason,
            metrics={"previous_status": previous_status},
        )
    )
    return run


def cancel_research_tasks(
    db: Session,
    *,
    kind: Literal["asset_research", "event_research"] | None = None,
    entity_id: str | None = None,
    task_id: str | None = None,
    include_failed: bool = False,
    instance_id: str | None = None,
) -> ResearchCancellationResult:
    """Cancel one displayed card, or clear active and optionally failed runs."""
    if kind == "asset_research" and not entity_id:
        raise ValueError("asset research cancellation requires entity_id")
    if kind == "event_research" and not task_id:
        raise ValueError("event research cancellation requires task_id")

    asset_status_filter = ResearchRunRow.status.in_(ACTIVE_RESEARCH_STATUSES)
    event_status_filter = EventResearchRunRow.status.in_(ACTIVE_RESEARCH_STATUSES)
    if include_failed:
        failed_cutoff = utc_now() - FAILED_RESEARCH_CLEAR_WINDOW
        asset_status_filter = or_(
            asset_status_filter,
            and_(
                ResearchRunRow.status == RunStatus.FAILED.value,
                ResearchRunRow.updated_at >= failed_cutoff,
            ),
        )
        event_status_filter = or_(
            event_status_filter,
            and_(
                EventResearchRunRow.status == RunStatus.FAILED.value,
                EventResearchRunRow.updated_at >= failed_cutoff,
            ),
        )
    asset_statement = select(ResearchRunRow).where(asset_status_filter)
    event_statement = select(EventResearchRunRow).where(event_status_filter)
    if kind == "asset_research":
        asset_statement = asset_statement.where(ResearchRunRow.asset_id == entity_id)
        event_rows = []
    elif kind == "event_research":
        try:
            run_id = UUID(task_id or "")
        except ValueError as exc:
            raise ValueError("invalid event research task_id") from exc
        event_statement = event_statement.where(EventResearchRunRow.id == run_id)
        asset_rows = []
    else:
        kind = None

    if kind != "event_research":
        asset_rows = list(db.scalars(asset_statement).all())
    if kind != "asset_research":
        event_rows = list(db.scalars(event_statement).all())
    if instance_id is not None:
        def assigned_instance(run: ResearchRun | EventResearchRun) -> str | None:
            return run.model_instance_id or (
                instance_assignment("research", run.celery_task_id)
                if run.celery_task_id
                else None
            )

        asset_rows = [
            row
            for row in asset_rows
            if assigned_instance(ResearchRun.model_validate(row.payload)) == instance_id
        ]
        event_rows = [
            row
            for row in event_rows
            if assigned_instance(EventResearchRun.model_validate(row.payload))
            == instance_id
        ]

    statuses: Counter[str] = Counter()
    celery_task_ids: list[str] = []
    now = utc_now()
    for row in asset_rows:
        run = ResearchRun.model_validate(row.payload)
        statuses[run.status.value] += 1
        if run.status.value in ACTIVE_RESEARCH_STATUSES and run.celery_task_id:
            celery_task_ids.append(run.celery_task_id)
        reason = (
            "用户清空了该标的失败研究记录。"
            if run.status is RunStatus.FAILED
            else "用户取消了该标的当前研究任务。"
        )
        run = _cancel_asset_run(run, reason)
        if run.celery_task_id:
            update_instance_assignment(
                "research",
                run.celery_task_id,
                status="cancelled",
                instance_id=run.model_instance_id,
            )
        row.status = RunStatus.CANCELLED.value
        row.payload = run.model_dump(mode="json")
        row.updated_at = now
        db.add(row)
    for row in event_rows:
        run = EventResearchRun.model_validate(row.payload)
        statuses[run.status.value] += 1
        if run.status.value in ACTIVE_RESEARCH_STATUSES and run.celery_task_id:
            celery_task_ids.append(run.celery_task_id)
        reason = (
            "用户清空了失败的中性事件研究记录。"
            if run.status is RunStatus.FAILED
            else "用户取消了当前中性事件研究任务。"
        )
        run = _cancel_event_run(run, reason)
        if run.celery_task_id:
            update_instance_assignment(
                "research",
                run.celery_task_id,
                status="cancelled",
                instance_id=run.model_instance_id,
            )
        row.status = RunStatus.CANCELLED.value
        row.payload = run.model_dump(mode="json")
        row.updated_at = now
        db.add(row)
    db.commit()
    return ResearchCancellationResult(
        cancelled=len(asset_rows) + len(event_rows),
        asset_runs=len(asset_rows),
        event_runs=len(event_rows),
        counts_by_status=dict(statuses),
        celery_task_ids=list(dict.fromkeys(celery_task_ids)),
    )
