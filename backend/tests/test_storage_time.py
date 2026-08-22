from datetime import UTC, datetime, timedelta
from hashlib import sha256

from backend.app.domain import NewsItem
from backend.app.storage import list_news, save_news


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
