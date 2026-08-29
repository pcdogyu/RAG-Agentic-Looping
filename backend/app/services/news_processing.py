from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.db import NewsProcessingOutboxRow, NewsProcessingRow, NewsRow
from backend.app.domain import NewsItem, as_utc, utc_now
from backend.app.services.source_lineage import enrich_news_lineage
from backend.app.storage import (
    event_news_item_ids,
    get_news_by_content_hash,
    news_from_row,
    news_row_from_item,
)

DISPATCH_PENDING = "dispatch_pending"
QUEUED = "queued"
RUNNING = "running"
RETRYING = "retrying"
COMPLETED = "completed"
DISPATCH_FAILED = "dispatch_failed"
EXTRACTION_FAILED = "extraction_failed"
CANCELLED = "cancelled"

ACTIVE_STATUSES = {QUEUED, RUNNING, RETRYING}
FAILURE_STATUSES = {DISPATCH_FAILED, EXTRACTION_FAILED}
RECOVERABLE_STATUSES = {DISPATCH_PENDING, COMPLETED, *FAILURE_STATUSES}
OUTBOX_DUE_STATUSES = {"pending", "failed"}


@dataclass(frozen=True)
class ClaimedNewsDispatch:
    outbox_id: UUID
    news_id: UUID
    force_asset_mapping: bool
    dispatch_attempt: int


def _processing_row(db: Session, news_id: UUID, now: datetime) -> NewsProcessingRow:
    row = db.get(NewsProcessingRow, news_id)
    if row is None:
        row = NewsProcessingRow(
            news_id=news_id,
            status=DISPATCH_PENDING,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    return row


def _outbox_row(
    db: Session,
    news_id: UUID,
    now: datetime,
    *,
    force_asset_mapping: bool,
    available_at: datetime,
) -> NewsProcessingOutboxRow:
    row = db.scalar(
        select(NewsProcessingOutboxRow).where(
            NewsProcessingOutboxRow.news_id == news_id
        )
    )
    if row is None:
        row = NewsProcessingOutboxRow(
            news_id=news_id,
            status="pending",
            force_asset_mapping=force_asset_mapping,
            available_at=available_at,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.status = "pending"
        row.force_asset_mapping = row.force_asset_mapping or force_asset_mapping
        row.available_at = available_at
        row.dispatched_at = None
        row.last_error = None
        row.updated_at = now
    return row


def _stage_existing_news(
    db: Session,
    news: NewsItem,
    *,
    scan_task_id: str | None,
    dispatch_delay_seconds: int,
) -> NewsItem:
    now = utc_now()
    processing = _processing_row(db, news.id, now)
    processing.status = DISPATCH_PENDING
    processing.scan_task_id = scan_task_id
    processing.celery_task_id = None
    processing.last_error = None
    processing.queued_at = None
    processing.started_at = None
    processing.completed_at = None
    processing.heartbeat_at = now
    processing.updated_at = now
    _outbox_row(
        db,
        news.id,
        now,
        force_asset_mapping=False,
        available_at=now + timedelta(seconds=max(0, dispatch_delay_seconds)),
    )
    db.commit()
    return news


def stage_news_for_extraction(
    db: Session,
    item: NewsItem,
    *,
    scan_task_id: str | None = None,
    dispatch_delay_seconds: int = 120,
) -> NewsItem:
    """Atomically persist a news item and its durable dispatch intent."""

    item = enrich_news_lineage(item)
    stored = get_news_by_content_hash(db, item.content_hash)
    if stored is not None:
        return _stage_existing_news(
            db,
            stored,
            scan_task_id=scan_task_id,
            dispatch_delay_seconds=dispatch_delay_seconds,
        )

    db.add(news_row_from_item(item))
    now = utc_now()
    processing = _processing_row(db, item.id, now)
    processing.scan_task_id = scan_task_id
    processing.heartbeat_at = now
    _outbox_row(
        db,
        item.id,
        now,
        force_asset_mapping=False,
        available_at=now + timedelta(seconds=max(0, dispatch_delay_seconds)),
    )
    try:
        db.commit()
        return item
    except IntegrityError:
        db.rollback()
        stored = get_news_by_content_hash(db, item.content_hash)
        if stored is None:
            raise
        return _stage_existing_news(
            db,
            stored,
            scan_task_id=scan_task_id,
            dispatch_delay_seconds=dispatch_delay_seconds,
        )


def mark_news_processing(
    db: Session,
    news_id: UUID,
    status: str,
    *,
    task_id: str | None = None,
    scan_task_id: str | None = None,
    attempt_count: int | None = None,
    error: str | None = None,
    commit: bool = True,
) -> NewsProcessingRow:
    now = utc_now()
    row = _processing_row(db, news_id, now)
    row.status = status
    row.updated_at = now
    row.heartbeat_at = now
    row.last_error = error
    if task_id is not None:
        row.celery_task_id = task_id
    if scan_task_id is not None:
        row.scan_task_id = scan_task_id
    if attempt_count is not None:
        row.attempt_count = max(row.attempt_count or 0, attempt_count)
    if status == QUEUED:
        row.queued_at = now
        row.started_at = None
        row.completed_at = None
    elif status == RUNNING:
        row.started_at = row.started_at or now
        row.completed_at = None
    elif status in {COMPLETED, EXTRACTION_FAILED, CANCELLED}:
        row.completed_at = now
    if commit:
        db.commit()
    return row


def mark_scan_news_queued(
    db: Session,
    scan_task_id: str,
    assignments: list[tuple[UUID, str]],
) -> None:
    now = utc_now()
    for news_id, task_id in assignments:
        processing = mark_news_processing(
            db,
            news_id,
            QUEUED,
            task_id=task_id,
            scan_task_id=scan_task_id,
            commit=False,
        )
        processing.queued_at = now
        outbox = db.scalar(
            select(NewsProcessingOutboxRow).where(
                NewsProcessingOutboxRow.news_id == news_id
            )
        )
        if outbox is not None:
            outbox.status = "dispatched"
            outbox.dispatched_at = now
            outbox.last_error = None
            outbox.updated_at = now
    db.commit()


def request_news_retry(
    db: Session,
    news_id: UUID,
    *,
    force_asset_mapping: bool = True,
    allow_active: bool = False,
) -> NewsItem:
    row = db.get(NewsRow, news_id)
    if row is None:
        raise LookupError("news item not found")
    processing = db.get(NewsProcessingRow, news_id)
    if processing is not None and processing.status in ACTIVE_STATUSES and not allow_active:
        raise RuntimeError("news extraction is already active")
    now = utc_now()
    processing = _processing_row(db, news_id, now)
    processing.status = DISPATCH_PENDING
    processing.scan_task_id = None
    processing.celery_task_id = None
    processing.last_error = None
    processing.queued_at = None
    processing.started_at = None
    processing.completed_at = None
    processing.heartbeat_at = now
    processing.updated_at = now
    _outbox_row(
        db,
        news_id,
        now,
        force_asset_mapping=force_asset_mapping,
        available_at=now,
    )
    db.commit()
    return news_from_row(row)


def recover_orphaned_news(
    db: Session,
    *,
    limit: int = 100,
    grace_seconds: int = 120,
    stale_seconds: int = 600,
    retention_days: int = 7,
    now: datetime | None = None,
) -> dict[str, int]:
    """Create dispatch intents for durable news that never reached an event."""

    current = as_utc(now or utc_now())
    grace_cutoff = current - timedelta(seconds=max(0, grace_seconds))
    stale_cutoff = current - timedelta(seconds=max(1, stale_seconds))
    retention_cutoff = current - timedelta(days=max(1, retention_days))
    processed_ids = event_news_item_ids(db)
    rows = list(
        db.scalars(
            select(NewsRow)
            .where(
                NewsRow.observed_at >= retention_cutoff,
                NewsRow.observed_at <= grace_cutoff,
            )
            .order_by(NewsRow.observed_at, NewsRow.id)
            .limit(max(limit * 5, limit))
        )
    )
    recovered = 0
    stale = 0
    for news in rows:
        if recovered >= limit:
            break
        processing = db.get(NewsProcessingRow, news.id)
        if news.id in processed_ids:
            if processing is not None and processing.status != COMPLETED:
                mark_news_processing(db, news.id, COMPLETED, commit=False)
            continue
        if processing is not None and processing.status == CANCELLED:
            continue
        if processing is not None and processing.status in ACTIVE_STATUSES:
            heartbeat = as_utc(
                processing.heartbeat_at or processing.updated_at or processing.created_at
            )
            if heartbeat > stale_cutoff:
                continue
            stale += 1
        elif processing is not None and processing.status == DISPATCH_PENDING:
            outbox = db.scalar(
                select(NewsProcessingOutboxRow).where(
                    NewsProcessingOutboxRow.news_id == news.id
                )
            )
            if outbox is not None and outbox.status in OUTBOX_DUE_STATUSES:
                continue

        processing = _processing_row(db, news.id, current)
        processing.status = DISPATCH_PENDING
        processing.scan_task_id = None
        processing.celery_task_id = None
        processing.last_error = None
        processing.queued_at = None
        processing.started_at = None
        processing.completed_at = None
        processing.heartbeat_at = current
        processing.updated_at = current
        _outbox_row(
            db,
            news.id,
            current,
            force_asset_mapping=True,
            available_at=current,
        )
        recovered += 1
    db.commit()
    return {"recovered": recovered, "stale": stale}


def claim_news_dispatches(
    db: Session,
    *,
    limit: int = 50,
    news_ids: set[UUID] | None = None,
    now: datetime | None = None,
) -> list[ClaimedNewsDispatch]:
    current = as_utc(now or utc_now())
    statement = (
        select(NewsProcessingOutboxRow)
        .where(
            NewsProcessingOutboxRow.status.in_(OUTBOX_DUE_STATUSES),
            NewsProcessingOutboxRow.available_at <= current,
        )
        .order_by(
            NewsProcessingOutboxRow.available_at,
            NewsProcessingOutboxRow.created_at,
        )
        .limit(limit)
    )
    if news_ids:
        statement = statement.where(NewsProcessingOutboxRow.news_id.in_(news_ids))
    if db.bind is not None and db.bind.dialect.name == "postgresql":
        statement = statement.with_for_update(skip_locked=True)
    rows = list(db.scalars(statement))
    claims: list[ClaimedNewsDispatch] = []
    for row in rows:
        row.status = "dispatching"
        row.dispatch_attempts += 1
        row.updated_at = current
        claims.append(
            ClaimedNewsDispatch(
                outbox_id=row.id,
                news_id=row.news_id,
                force_asset_mapping=row.force_asset_mapping,
                dispatch_attempt=row.dispatch_attempts,
            )
        )
    db.commit()
    return claims


def mark_news_dispatched(
    db: Session,
    claim: ClaimedNewsDispatch,
    task_id: str,
) -> None:
    now = utc_now()
    outbox = db.get(NewsProcessingOutboxRow, claim.outbox_id)
    if outbox is not None:
        outbox.status = "dispatched"
        outbox.dispatched_at = now
        outbox.last_error = None
        outbox.updated_at = now
    mark_news_processing(
        db,
        claim.news_id,
        QUEUED,
        task_id=task_id,
        attempt_count=claim.dispatch_attempt,
        commit=False,
    )
    db.commit()


def mark_news_dispatch_failed(
    db: Session,
    claim: ClaimedNewsDispatch,
    error: str,
) -> None:
    now = utc_now()
    outbox = db.get(NewsProcessingOutboxRow, claim.outbox_id)
    if outbox is not None:
        outbox.status = "failed"
        outbox.available_at = now + timedelta(
            seconds=min(300, 2 ** min(claim.dispatch_attempt, 8))
        )
        outbox.last_error = error[:500]
        outbox.updated_at = now
    mark_news_processing(
        db,
        claim.news_id,
        DISPATCH_FAILED,
        attempt_count=claim.dispatch_attempt,
        error=error[:500],
        commit=False,
    )
    db.commit()


def processing_rows_by_news(
    db: Session, news_ids: set[UUID]
) -> dict[UUID, NewsProcessingRow]:
    if not news_ids:
        return {}
    rows = db.scalars(
        select(NewsProcessingRow).where(NewsProcessingRow.news_id.in_(news_ids))
    ).all()
    return {row.news_id: row for row in rows}
