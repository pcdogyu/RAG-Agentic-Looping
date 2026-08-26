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
    updated_at: datetime


class ResearchQueueResponse(BaseModel):
    generated_at: datetime
    model: str
    total_assets: int
    total_runs: int
    counts: ResearchQueueCounts
    truncated: bool
    items: list[ResearchQueueItem]


def build_research_queue(
    runs: list[ResearchRun],
    limit: int,
    model: str,
    generated_at: datetime | None = None,
) -> ResearchQueueResponse:
    active_runs = [run for run in runs if run.status in ACTIVE_RUN_STATUSES]
    counts = ResearchQueueCounts()
    grouped: dict[str, ResearchQueueItem] = {}

    for run in active_runs:
        setattr(counts, run.status.value, getattr(counts, run.status.value) + 1)
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
                updated_at=as_utc(run.updated_at),
            )
            continue

        current.task_count += 1
        current.queued_at = min(current.queued_at, as_utc(run.created_at))
        current.updated_at = max(current.updated_at, as_utc(run.updated_at))
        if STATUS_PRIORITY[run.status] > STATUS_PRIORITY[current.status]:
            current.status = run.status

    def sort_key(item: ResearchQueueItem) -> tuple[int, float]:
        if item.status is RunStatus.QUEUED:
            return (2, item.queued_at.timestamp())
        return (-STATUS_PRIORITY[item.status], -item.updated_at.timestamp())

    all_items = sorted(grouped.values(), key=sort_key)
    return ResearchQueueResponse(
        generated_at=generated_at or utc_now(),
        model=model,
        total_assets=len(all_items),
        total_runs=len(active_runs),
        counts=counts,
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
    updated_at: datetime
    error: str | None = None


class NewsExtractionQueueResponse(BaseModel):
    generated_at: datetime
    model: str
    scan_task_id: str | None = None
    state: str
    total_items: int
    counts: NewsExtractionQueueCounts
    truncated: bool
    items: list[NewsExtractionQueueItem]
    error: str | None = None
