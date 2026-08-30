from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.app.db import NewsRow, NewsSourceStateRow
from backend.app.domain import NewsItem, as_utc, utc_now


@dataclass(frozen=True)
class NewsSourceDiscoveryReport:
    source: str
    provider: str
    status: str
    attempted_at: datetime
    discovered_count: int = 0
    latest_published_at: datetime | None = None
    error: str | None = None


def news_source_watermarks(db: Session) -> dict[str, datetime]:
    """Load durable watermarks, backfilling legacy sources from stored news."""

    watermarks = {
        row.source: as_utc(row.watermark_at)
        for row in db.scalars(select(NewsSourceStateRow)).all()
        if row.watermark_at is not None
    }
    legacy = db.execute(
        select(NewsRow.source, func.max(NewsRow.published_at)).group_by(NewsRow.source)
    ).all()
    for source, latest in legacy:
        if latest is None:
            continue
        latest_utc = as_utc(latest)
        current = watermarks.get(source)
        if current is None or latest_utc > current:
            watermarks[source] = latest_utc
    return watermarks


def filter_news_by_source_watermark(
    items: list[NewsItem],
    watermarks: dict[str, datetime],
    *,
    lookback_start: datetime,
    overlap_minutes: int,
) -> list[NewsItem]:
    """Retain catch-up news newer than each source's overlapping watermark."""

    floor = as_utc(lookback_start)
    overlap = timedelta(minutes=max(1, overlap_minutes))
    output: list[NewsItem] = []
    for item in items:
        watermark = watermarks.get(item.source)
        threshold = floor
        if watermark is not None:
            threshold = max(floor, as_utc(watermark) - overlap)
        if as_utc(item.published_at) >= threshold:
            output.append(item)
    return output


def record_news_source_reports(
    db: Session,
    reports: list[NewsSourceDiscoveryReport],
    *,
    new_counts: dict[str, int] | None = None,
) -> None:
    """Persist source health without moving a watermark backwards on failures."""

    counts = new_counts or {}
    now = utc_now()
    for report in reports:
        row = db.get(NewsSourceStateRow, report.source)
        if row is None:
            row = NewsSourceStateRow(
                source=report.source,
                provider=report.provider,
                created_at=now,
                updated_at=now,
            )
            db.add(row)
        row.provider = report.provider
        row.status = report.status
        row.last_attempt_at = as_utc(report.attempted_at)
        row.last_discovered_count = max(0, report.discovered_count)
        row.last_new_count = max(0, counts.get(report.source, 0))
        row.updated_at = now
        if report.status == "healthy":
            row.last_success_at = as_utc(report.attempted_at)
            row.last_error = None
            row.consecutive_failures = 0
            if report.latest_published_at is not None:
                latest = as_utc(report.latest_published_at)
                if row.watermark_at is None or latest > as_utc(row.watermark_at):
                    row.watermark_at = latest
        else:
            row.last_error = (report.error or "unknown source error")[:500]
            row.consecutive_failures = (row.consecutive_failures or 0) + 1
    db.commit()
