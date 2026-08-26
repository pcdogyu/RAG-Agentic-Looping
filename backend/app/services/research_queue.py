from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from backend.app.domain import (
    AssetClass,
    Market,
    ResearchRun,
    RunStatus,
    as_utc,
    utc_now,
)

ACTIVE_RUN_STATUSES = {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}
STATUS_PRIORITY = {
    RunStatus.QUEUED: 1,
    RunStatus.RUNNING: 2,
    RunStatus.VERIFYING: 3,
}


class ResearchQueueCounts(BaseModel):
    queued: int = 0
    running: int = 0
    verifying: int = 0


class ResearchQueueItem(BaseModel):
    asset_id: str
    symbol: str
    name: str
    market: Market
    asset_class: AssetClass
    status: RunStatus
    task_count: int = Field(ge=1)
    queued_at: datetime
    representative_queued_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    queue_duration_ms: int | None = Field(default=None, ge=0)
    execution_duration_ms: int | None = Field(default=None, ge=0)
    updated_at: datetime


class ResearchQueueResponse(BaseModel):
    generated_at: datetime
    model: str
    total_assets: int
    total_runs: int
    counts: ResearchQueueCounts
    average_queue_duration_ms: int | None = Field(default=None, ge=0)
    average_execution_duration_ms: int | None = Field(default=None, ge=0)
    queue_duration_sample_count: int = Field(default=0, ge=0)
    execution_duration_sample_count: int = Field(default=0, ge=0)
    truncated: bool
    items: list[ResearchQueueItem]


def _duration_ms(start: datetime, end: datetime) -> int:
    return max(0, int((as_utc(end) - as_utc(start)).total_seconds() * 1000))


def _run_durations(run: ResearchRun, generated_at: datetime) -> tuple[int | None, int | None]:
    if run.started_at is not None:
        queue_duration = _duration_ms(run.created_at, run.started_at)
        execution_duration = _duration_ms(
            run.started_at,
            run.completed_at or generated_at,
        )
        return queue_duration, execution_duration
    if run.status is RunStatus.QUEUED:
        return _duration_ms(run.created_at, generated_at), None
    return None, None


def build_research_queue(
    runs: list[ResearchRun],
    limit: int,
    model: str,
    generated_at: datetime | None = None,
) -> ResearchQueueResponse:
    now = as_utc(generated_at or utc_now())
    active_runs = [run for run in runs if run.status in ACTIVE_RUN_STATUSES]
    counts = ResearchQueueCounts()
    grouped: dict[str, ResearchQueueItem] = {}
    representatives: dict[str, ResearchRun] = {}
    queue_durations: list[int] = []
    execution_durations: list[int] = []

    for run in active_runs:
        setattr(counts, run.status.value, getattr(counts, run.status.value) + 1)
        queue_duration, execution_duration = _run_durations(run, now)
        if queue_duration is not None:
            queue_durations.append(queue_duration)
        if execution_duration is not None:
            execution_durations.append(execution_duration)
        current = grouped.get(run.asset.asset_id)
        if current is None:
            grouped[run.asset.asset_id] = ResearchQueueItem(
                asset_id=run.asset.asset_id,
                symbol=run.asset.symbol,
                name=run.asset.name,
                market=run.asset.market,
                asset_class=run.asset.asset_class,
                status=run.status,
                task_count=1,
                queued_at=as_utc(run.created_at),
                representative_queued_at=as_utc(run.created_at),
                started_at=as_utc(run.started_at) if run.started_at else None,
                completed_at=as_utc(run.completed_at) if run.completed_at else None,
                queue_duration_ms=queue_duration,
                execution_duration_ms=execution_duration,
                updated_at=as_utc(run.updated_at),
            )
            representatives[run.asset.asset_id] = run
            continue

        current.task_count += 1
        current.queued_at = min(current.queued_at, as_utc(run.created_at))
        current.updated_at = max(current.updated_at, as_utc(run.updated_at))
        representative = representatives[run.asset.asset_id]
        should_replace = STATUS_PRIORITY[run.status] > STATUS_PRIORITY[representative.status]
        if STATUS_PRIORITY[run.status] == STATUS_PRIORITY[representative.status]:
            should_replace = as_utc(run.updated_at) > as_utc(representative.updated_at)
        if should_replace:
            representatives[run.asset.asset_id] = run
            current.status = run.status
            current.representative_queued_at = as_utc(run.created_at)
            current.started_at = as_utc(run.started_at) if run.started_at else None
            current.completed_at = as_utc(run.completed_at) if run.completed_at else None
            current.queue_duration_ms = queue_duration
            current.execution_duration_ms = execution_duration

    def sort_key(item: ResearchQueueItem) -> tuple[int, float]:
        if item.status is RunStatus.QUEUED:
            return (2, item.queued_at.timestamp())
        return (-STATUS_PRIORITY[item.status], -item.updated_at.timestamp())

    all_items = sorted(grouped.values(), key=sort_key)
    return ResearchQueueResponse(
        generated_at=now,
        model=model,
        total_assets=len(all_items),
        total_runs=len(active_runs),
        counts=counts,
        average_queue_duration_ms=(
            sum(queue_durations) // len(queue_durations) if queue_durations else None
        ),
        average_execution_duration_ms=(
            sum(execution_durations) // len(execution_durations)
            if execution_durations
            else None
        ),
        queue_duration_sample_count=len(queue_durations),
        execution_duration_sample_count=len(execution_durations),
        truncated=len(all_items) > limit,
        items=all_items[:limit],
    )


class NewsExtractionQueueCounts(BaseModel):
    queued: int = 0
    running: int = 0
    retrying: int = 0
    completed: int = 0
    failed: int = 0


class NewsExtractionQueueItem(BaseModel):
    task_id: str
    news_id: str
    title: str
    source: str
    published_at: datetime
    status: str
    attempt: int = Field(default=0, ge=0)
    queued_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    queue_duration_ms: int | None = Field(default=None, ge=0)
    execution_duration_ms: int | None = Field(default=None, ge=0)
    updated_at: datetime
    error: str | None = None


class NewsExtractionQueueResponse(BaseModel):
    generated_at: datetime
    model: str
    scan_task_id: str | None = None
    state: str
    total_items: int
    counts: NewsExtractionQueueCounts
    average_queue_duration_ms: int | None = Field(default=None, ge=0)
    average_execution_duration_ms: int | None = Field(default=None, ge=0)
    queue_duration_sample_count: int = Field(default=0, ge=0)
    execution_duration_sample_count: int = Field(default=0, ge=0)
    truncated: bool
    items: list[NewsExtractionQueueItem]
    error: str | None = None
