from datetime import UTC, datetime, timedelta
from hashlib import sha256

from backend.app.db import NewsSourceStateRow
from backend.app.domain import NewsItem, SourceQuality
from backend.app.services.news_sources import (
    NewsSourceDiscoveryReport,
    filter_news_by_source_watermark,
    news_source_watermarks,
    record_news_source_reports,
)
from backend.app.storage import save_news


def _news(source: str, published_at: datetime, suffix: str) -> NewsItem:
    return NewsItem(
        source=source,
        source_quality=SourceQuality.PROFESSIONAL,
        title=f"{source}-{suffix}",
        url=f"https://example.com/{source}/{suffix}",
        published_at=published_at,
        as_of=published_at,
        content_hash=sha256(f"{source}-{suffix}".encode()).hexdigest(),
    )


def test_source_watermark_uses_legacy_news_and_overlap(db):
    now = datetime(2026, 8, 30, 4, tzinfo=UTC)
    assert save_news(db, _news("东方财富/AkShare", now - timedelta(hours=2), "stored"))

    watermarks = news_source_watermarks(db)
    items = [
        _news("东方财富/AkShare", now - timedelta(hours=2, minutes=5), "overlap"),
        _news("东方财富/AkShare", now - timedelta(hours=3), "too-old"),
        _news("金十数据", now - timedelta(hours=10), "catch-up"),
    ]

    filtered = filter_news_by_source_watermark(
        items,
        watermarks,
        lookback_start=now - timedelta(hours=24),
        overlap_minutes=10,
    )

    assert [item.title for item in filtered] == [
        "东方财富/AkShare-overlap",
        "金十数据-catch-up",
    ]


def test_source_report_persists_success_and_preserves_watermark_on_error(db):
    attempted = datetime(2026, 8, 30, 4, tzinfo=UTC)
    latest = attempted - timedelta(minutes=2)
    record_news_source_reports(
        db,
        [
            NewsSourceDiscoveryReport(
                source="金十数据",
                provider="mcp-news",
                status="healthy",
                attempted_at=attempted,
                discovered_count=8,
                latest_published_at=latest,
            )
        ],
        new_counts={"金十数据": 3},
    )

    row = db.get(NewsSourceStateRow, "金十数据")
    assert row is not None
    assert row.status == "healthy"
    assert row.last_discovered_count == 8
    assert row.last_new_count == 3
    assert row.watermark_at == latest.replace(tzinfo=None)

    record_news_source_reports(
        db,
        [
            NewsSourceDiscoveryReport(
                source="金十数据",
                provider="mcp-news",
                status="error",
                attempted_at=attempted + timedelta(minutes=10),
                error="TimeoutError: source unavailable",
            )
        ],
    )
    db.refresh(row)

    assert row.status == "error"
    assert row.last_error == "TimeoutError: source unavailable"
    assert row.consecutive_failures == 1
    assert row.watermark_at == latest.replace(tzinfo=None)
