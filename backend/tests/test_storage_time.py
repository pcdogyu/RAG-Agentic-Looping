import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from backend.app.domain import NewsEvent, NewsItem, SourceQuality
from backend.app.main import event_stream
from backend.app.storage import (
    get_news,
    list_events,
    list_news,
    normalize_legacy_akshare_timestamps,
    save_event,
    save_news,
)


def test_point_in_time_query_excludes_future_observation(db):
    as_of = datetime(2025, 1, 1, tzinfo=UTC)
    item = NewsItem(
        source="test",
        title="future item",
        url="https://example.com/future",
        published_at=as_of - timedelta(days=1),
        observed_at=as_of + timedelta(days=1),
        as_of=as_of + timedelta(days=1),
        content_hash=sha256(b"future").hexdigest(),
    )
    assert save_news(db, item)
    assert list_news(db, as_of=as_of) == []


def test_events_are_ordered_by_latest_publication_before_priority(db):
    old_time = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
    new_time = old_time + timedelta(hours=1)
    for headline, published_at, priority in (
        ("old high priority", old_time, 1.0),
        ("new low priority", new_time, 0.1),
    ):
        save_event(
            db,
            NewsEvent(
                news_item_ids=[],
                headline=headline,
                event_type="other",
                direct_impact=headline,
                source_quality=SourceQuality.AGGREGATOR,
                published_at=published_at,
                observed_at=published_at,
                as_of=published_at,
                priority=priority,
            ),
        )

    assert [event.headline for event in list_events(db, 2)] == [
        "new low priority",
        "old high priority",
    ]


def test_legacy_akshare_timestamp_is_backfilled_once(db):
    incorrectly_tagged = datetime(2026, 8, 22, 17, 30, tzinfo=UTC)
    item = NewsItem(
        source="东方财富/AkShare",
        source_quality=SourceQuality.AGGREGATOR,
        title="legacy local timestamp",
        url="https://example.com/legacy-time",
        published_at=incorrectly_tagged,
        observed_at=datetime(2026, 8, 22, 9, 31, tzinfo=UTC),
        as_of=incorrectly_tagged,
        content_hash=sha256(b"legacy-akshare-time").hexdigest(),
    )
    assert save_news(db, item)
    event = NewsEvent(
        news_item_ids=[item.id],
        headline=item.title,
        event_type="other",
        direct_impact=item.title,
        source_quality=item.source_quality,
        published_at=item.published_at,
        observed_at=item.observed_at,
        as_of=item.as_of,
    )
    save_event(db, event)

    assert normalize_legacy_akshare_timestamps(db) == {"news": 1, "events": 1}
    expected = datetime(2026, 8, 22, 9, 30, tzinfo=UTC)
    assert get_news(db, item.id).published_at == expected
    assert list_events(db, 1)[0].published_at == expected
    assert normalize_legacy_akshare_timestamps(db) == {"news": 0, "events": 0}


@pytest.mark.asyncio
async def test_sse_snapshot_contains_thirty_recent_events(db):
    published = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
    for index in range(35):
        event_time = published + timedelta(minutes=index)
        save_event(
            db,
            NewsEvent(
                news_item_ids=[],
                headline=f"event {index}",
                event_type="other",
                direct_impact="test",
                source_quality=SourceQuality.AGGREGATOR,
                published_at=event_time,
                observed_at=event_time,
                as_of=event_time,
            ),
        )

    stream = event_stream()
    message = await anext(stream)
    await stream.aclose()
    payload = json.loads(message.split("data: ", 1)[1])

    assert len(payload["events"]) == 30
    assert payload["events"][0]["headline"] == "event 34"
