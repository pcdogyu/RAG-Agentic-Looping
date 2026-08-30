from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from backend.app.db import (
    EventResearchRunRow,
    EventRow,
    NewsProcessingRow,
    NewsRow,
    NewsSourceStateRow,
    ResearchRunRow,
)
from backend.app.domain import (
    EventResearchRun,
    NewsEvent,
    ResearchRun,
    RunStatus,
    SourceQuality,
    as_utc,
    utc_now,
)
from backend.app.services.news_processing import (
    DISPATCH_FAILED,
    DISPATCH_PENDING,
    EXTRACTION_FAILED,
    QUEUED,
    RETRYING,
    RUNNING,
    processing_rows_by_news,
)

NewsProcessingStatus = Literal[
    "dispatch_pending",
    "queued",
    "extracting",
    "mapping",
    "researching",
    "revising",
    "completed",
    "insufficient_evidence",
    "failed",
    "orphaned",
    "pending",
]

STATUS_PRIORITY: dict[NewsProcessingStatus, int] = {
    "pending": 0,
    "orphaned": 1,
    "failed": 2,
    "insufficient_evidence": 3,
    "completed": 4,
    "dispatch_pending": 5,
    "queued": 6,
    "extracting": 7,
    "mapping": 8,
    "researching": 9,
    "revising": 10,
}


class NewsBoardAsset(BaseModel):
    asset_id: str
    symbol: str
    name: str
    market: str


class NewsBoardEvent(BaseModel):
    id: UUID
    headline: str
    event_type: str
    priority: float


class NewsBoardItem(BaseModel):
    id: UUID
    title: str
    summary: str
    url: str
    source_quality: SourceQuality
    published_at: datetime
    observed_at: datetime
    status: NewsProcessingStatus
    status_updated_at: datetime
    status_detail: str | None = None
    retryable: bool = False
    events: list[NewsBoardEvent] = Field(default_factory=list)
    assets: list[NewsBoardAsset] = Field(default_factory=list)


class NewsBoardSource(BaseModel):
    source: str
    latest_published_at: datetime | None = None
    item_count: int = Field(ge=0)
    items: list[NewsBoardItem] = Field(default_factory=list)
    error: str | None = None
    discovery_status: str = "unchecked"
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    watermark_at: datetime | None = None
    last_error: str | None = None
    last_discovered_count: int = Field(default=0, ge=0)
    last_new_count: int = Field(default=0, ge=0)


class NewsBoardResponse(BaseModel):
    generated_at: datetime
    last_refresh_at: datetime | None = None
    last_success_at: datetime | None = None
    per_source: int = Field(ge=1, le=50)
    total_sources: int = Field(ge=0)
    sources: list[NewsBoardSource]


def _event_status(event: NewsEvent) -> tuple[NewsProcessingStatus, datetime]:
    for step in reversed(event.analysis_steps):
        if step.phase == "asset_mapping" and step.status in {"running", "retrying"}:
            return "mapping", as_utc(step.occurred_at)
        if step.phase == "asset_mapping_queue" and step.status == "queued":
            return "mapping", as_utc(step.occurred_at)
    return "pending", as_utc(event.as_of)


def _run_status(run: ResearchRun | EventResearchRun) -> NewsProcessingStatus:
    if run.status is RunStatus.VERIFYING:
        return "revising"
    if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}:
        return "researching"
    if run.status is RunStatus.COALESCED:
        return "researching"
    if run.status is RunStatus.COMPLETED:
        return "completed"
    if run.status is RunStatus.INSUFFICIENT_EVIDENCE:
        return "insufficient_evidence"
    if run.status is RunStatus.FAILED:
        return "failed"
    return "pending"


def _choose_status(
    candidates: list[tuple[NewsProcessingStatus, datetime]],
    fallback_at: datetime,
) -> tuple[NewsProcessingStatus, datetime]:
    if not candidates:
        return "pending", as_utc(fallback_at)
    return max(
        candidates,
        key=lambda value: (STATUS_PRIORITY[value[0]], as_utc(value[1]).timestamp()),
    )


def _extraction_candidates(
    extraction_items: list[dict],
) -> dict[UUID, tuple[NewsProcessingStatus, datetime]]:
    result: dict[UUID, tuple[NewsProcessingStatus, datetime]] = {}
    for item in extraction_items:
        try:
            news_id = UUID(str(item.get("news_id")))
            updated_at = datetime.fromisoformat(str(item.get("updated_at")))
        except (TypeError, ValueError):
            continue
        status = str(item.get("status") or "")
        if status in {"queued", "running", "retrying"}:
            result[news_id] = ("extracting", as_utc(updated_at))
        elif status == "failed":
            result[news_id] = ("failed", as_utc(updated_at))
    return result


def _durable_processing_status(
    row: NewsProcessingRow,
) -> tuple[NewsProcessingStatus, datetime] | None:
    updated_at = as_utc(row.updated_at)
    if row.status == DISPATCH_PENDING:
        return "dispatch_pending", updated_at
    if row.status == QUEUED:
        return "queued", updated_at
    if row.status in {RUNNING, RETRYING}:
        return "extracting", updated_at
    if row.status in {DISPATCH_FAILED, EXTRACTION_FAILED}:
        return "failed", updated_at
    return None


def build_news_board(
    db: Session,
    *,
    per_source: int = 50,
    extraction_items: list[dict] | None = None,
    generated_at: datetime | None = None,
) -> NewsBoardResponse:
    now = as_utc(generated_at or utc_now())
    source_rows = db.execute(
        select(NewsRow.source, func.max(NewsRow.published_at).label("latest"))
        .group_by(NewsRow.source)
        .order_by(desc("latest"), NewsRow.source)
    ).all()
    state_rows = list(db.scalars(select(NewsSourceStateRow)).all())
    states_by_source = {row.source: row for row in state_rows}

    grouped_rows: list[tuple[str, datetime | None, list[NewsRow], str | None]] = []
    selected_rows: list[NewsRow] = []
    for source, latest in source_rows:
        try:
            rows = list(
                db.scalars(
                    select(NewsRow)
                    .where(NewsRow.source == source)
                    .order_by(desc(NewsRow.published_at), desc(NewsRow.observed_at))
                    .limit(per_source)
                )
            )
            grouped_rows.append((source, as_utc(latest) if latest else None, rows, None))
            selected_rows.extend(rows)
        except Exception as exc:
            db.rollback()
            grouped_rows.append(
                (
                    source,
                    as_utc(latest) if latest else None,
                    [],
                    f"{type(exc).__name__}: source query failed",
                )
            )
    known_sources = {source for source, *_rest in grouped_rows}
    for state in state_rows:
        if state.source in known_sources:
            continue
        if state.status != "error" and state.watermark_at is None:
            continue
        grouped_rows.append((state.source, None, [], None))
    grouped_rows.sort(
        key=lambda value: (
            as_utc(value[1]).timestamp()
            if value[1] is not None
            else (
                as_utc(states_by_source[value[0]].watermark_at).timestamp()
                if states_by_source.get(value[0]) is not None
                and states_by_source[value[0]].watermark_at is not None
                else 0
            )
        ),
        reverse=True,
    )

    selected_ids = {row.id for row in selected_rows}
    events_by_news: dict[UUID, list[NewsEvent]] = defaultdict(list)
    selected_events: dict[UUID, NewsEvent] = {}
    if selected_rows:
        earliest_as_of = min(as_utc(row.as_of) for row in selected_rows)
        event_rows = db.scalars(
            select(EventRow).where(EventRow.as_of >= earliest_as_of)
        ).all()
        for row in event_rows:
            event = NewsEvent.model_validate(row.payload)
            matched_ids = selected_ids.intersection(event.news_item_ids)
            if not matched_ids:
                continue
            selected_events[event.id] = event
            for news_id in matched_ids:
                events_by_news[news_id].append(event)

    runs_by_event: dict[UUID, list[ResearchRun | EventResearchRun]] = defaultdict(list)
    if selected_events:
        event_ids = list(selected_events)
        run_rows = db.scalars(
            select(ResearchRunRow).where(ResearchRunRow.event_id.in_(event_ids))
        ).all()
        for row in run_rows:
            run = ResearchRun.model_validate(row.payload)
            if run.event_id is not None:
                runs_by_event[run.event_id].append(run)
        event_run_rows = db.scalars(
            select(EventResearchRunRow).where(EventResearchRunRow.event_id.in_(event_ids))
        ).all()
        for row in event_run_rows:
            run = EventResearchRun.model_validate(row.payload)
            runs_by_event[run.event_id].append(run)

    extraction_by_news = _extraction_candidates(extraction_items or [])
    processing_by_news = processing_rows_by_news(db, selected_ids)
    sources: list[NewsBoardSource] = []
    for source, latest, rows, error in grouped_rows:
        source_state = states_by_source.get(source)
        items: list[NewsBoardItem] = []
        for row in rows:
            events = sorted(
                events_by_news.get(row.id, []),
                key=lambda event: (event.priority, as_utc(event.as_of).timestamp()),
                reverse=True,
            )
            status_candidates: list[tuple[NewsProcessingStatus, datetime]] = []
            extraction = extraction_by_news.get(row.id)
            if extraction:
                status_candidates.append(extraction)
            durable_processing = processing_by_news.get(row.id)
            if durable_processing and (
                durable_status := _durable_processing_status(durable_processing)
            ):
                status_candidates.append(durable_status)
            for event in events:
                status_candidates.append(_event_status(event))
                for run in runs_by_event.get(event.id, []):
                    status_candidates.append((_run_status(run), as_utc(run.updated_at)))
            status, status_updated_at = _choose_status(status_candidates, row.observed_at)
            if not events and not status_candidates:
                status = "orphaned"
                status_updated_at = as_utc(row.observed_at)
            retryable = status in {"orphaned", "failed"}
            status_detail = None
            if durable_processing and durable_processing.last_error:
                status_detail = durable_processing.last_error
            elif status == "orphaned":
                status_detail = "新闻已入库，但没有抽取任务或关联事件。"
            elif status == "dispatch_pending":
                status_detail = "新闻已持久化，等待可靠派发到抽取队列。"
            elif status == "queued":
                status_detail = "抽取任务已创建，等待模型实例执行。"

            assets: dict[str, NewsBoardAsset] = {}
            for event in events:
                for candidate in event.candidates:
                    asset = candidate.asset
                    assets[asset.asset_id] = NewsBoardAsset(
                        asset_id=asset.asset_id,
                        symbol=asset.symbol,
                        name=asset.name,
                        market=asset.market.value,
                    )
            items.append(
                NewsBoardItem(
                    id=row.id,
                    title=row.title,
                    summary=row.summary,
                    url=row.url,
                    source_quality=SourceQuality(row.source_quality),
                    published_at=as_utc(row.published_at),
                    observed_at=as_utc(row.observed_at),
                    status=status,
                    status_updated_at=status_updated_at,
                    status_detail=status_detail,
                    retryable=retryable,
                    events=[
                        NewsBoardEvent(
                            id=event.id,
                            headline=event.headline,
                            event_type=event.event_type.value,
                            priority=event.priority,
                        )
                        for event in events
                    ],
                    assets=list(assets.values()),
                )
            )
        sources.append(
            NewsBoardSource(
                source=source,
                latest_published_at=latest,
                item_count=len(items),
                items=items,
                error=error,
                discovery_status=(source_state.status if source_state else "unchecked"),
                last_attempt_at=(
                    as_utc(source_state.last_attempt_at)
                    if source_state and source_state.last_attempt_at
                    else None
                ),
                last_success_at=(
                    as_utc(source_state.last_success_at)
                    if source_state and source_state.last_success_at
                    else None
                ),
                watermark_at=(
                    as_utc(source_state.watermark_at)
                    if source_state and source_state.watermark_at
                    else None
                ),
                last_error=source_state.last_error if source_state else None,
                last_discovered_count=(
                    source_state.last_discovered_count if source_state else 0
                ),
                last_new_count=source_state.last_new_count if source_state else 0,
            )
        )

    attempts = [as_utc(row.last_attempt_at) for row in state_rows if row.last_attempt_at]
    successes = [as_utc(row.last_success_at) for row in state_rows if row.last_success_at]
    return NewsBoardResponse(
        generated_at=now,
        last_refresh_at=max(attempts) if attempts else None,
        last_success_at=max(successes) if successes else None,
        per_source=per_source,
        total_sources=len(sources),
        sources=sources,
    )
