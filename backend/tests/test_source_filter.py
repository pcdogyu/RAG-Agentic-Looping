from datetime import UTC, datetime, timedelta
from hashlib import sha256

from fastapi.testclient import TestClient

from backend.app.db import NewsFilterLogRow
from backend.app.domain import NewsItem, SourceQuality, utc_now
from backend.app.main import app
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService, ExtractedEvent
from backend.app.services.source_filter import (
    SourceFilterConfig,
    evaluate_title,
    filter_news_items,
    list_filter_logs,
)
from backend.app.storage import get_news, list_events


def news(title: str, *, summary: str = "Market update", suffix: str = "item") -> NewsItem:
    observed = datetime(2026, 8, 25, 4, 0, tzinfo=UTC)
    return NewsItem(
        source="Example Wire",
        source_quality=SourceQuality.PROFESSIONAL,
        title=title,
        summary=summary,
        url=f"https://example.com/{suffix}",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(suffix.encode()).hexdigest(),
    )


def test_default_blacklist_matches_title_only_and_filter_can_be_disabled():
    config = SourceFilterConfig()
    assert evaluate_title("今日天气预报", config).allowed is False
    assert evaluate_title("Apple earnings", config).allowed is True
    assert evaluate_title("今日天气预报", SourceFilterConfig(enabled=False)).allowed is True


def test_whitelist_overrides_blacklist_and_keywords_are_normalized():
    config = SourceFilterConfig(
        whitelist_keywords=[" Apple ", "ＡＰＰＬＥ", ""],
        blacklist_keywords=["WEATHER", "weather"],
    )
    assert config.whitelist_keywords == ["Apple"]
    assert config.blacklist_keywords == ["WEATHER"]
    decision = evaluate_title("APPLE WEATHER supply-chain disruption", config)
    assert decision.allowed is True
    assert decision.matched_keyword == "Apple"


def test_filtered_news_is_audited_but_never_enters_news_or_event_tables(db):
    blocked = news("城市天气预报", suffix="weather")
    allowed = news("Apple earnings growth", summary="天气改善", suffix="earnings")
    accepted, filtered = filter_news_items(db, [blocked, allowed])

    class CountingLlm:
        calls = 0

        def generate_json(self, **_kwargs):
            self.calls += 1
            return ExtractedEvent(direct_impact="earnings", entities=[]).model_dump(mode="json")

    llm = CountingLlm()
    events = EventService(ProviderRegistry(), llm=llm).ingest(db, accepted)

    assert filtered == 1
    assert accepted == [allowed]
    assert get_news(db, blocked.id) is None
    assert get_news(db, allowed.id) is not None
    assert len(events) == 1
    assert len(list_events(db)) == 1
    assert llm.calls == 1
    assert list_filter_logs(db)[0]["matched_keyword"] == "天气"

    filter_news_items(db, [blocked])
    assert list_filter_logs(db)[0]["hit_count"] == 2


def test_filter_log_retention_removes_old_and_overflow_rows(db, monkeypatch):
    import backend.app.services.source_filter as source_filter

    monkeypatch.setattr(source_filter, "FILTER_LOG_MAX_ROWS", 2)
    old = utc_now() - timedelta(days=31)
    db.add(
        NewsFilterLogRow(
            content_hash=sha256(b"old").hexdigest(),
            source="Old",
            title="旧天气",
            url="https://example.com/old",
            matched_keyword="天气",
            published_at=old,
            first_filtered_at=old,
            last_filtered_at=old,
        )
    )
    db.commit()
    filter_news_items(
        db,
        [news(f"天气 {index}", suffix=f"weather-{index}") for index in range(3)],
    )
    assert len(list_filter_logs(db, limit=100)) == 2


def test_public_filter_api_supports_update_validation_logs_and_reset(db):
    filter_news_items(db, [news("天气提醒", suffix="api-weather")])
    with TestClient(app) as client:
        default = client.get("/api/v1/source-filter")
        updated = client.put(
            "/api/v1/source-filter",
            json={
                "enabled": True,
                "whitelist_keywords": ["Apple"],
                "blacklist_keywords": ["天气", "Weather"],
            },
        )
        invalid = client.put(
            "/api/v1/source-filter",
            json={"enabled": True, "whitelist_keywords": [], "blacklist_keywords": ["x" * 81]},
        )
        logs = client.get("/api/v1/source-filter/logs?limit=1")
        reset = client.delete("/api/v1/source-filter")

    assert default.status_code == 200
    assert default.json()["blacklist_keywords"] == ["天气"]
    assert default.json()["retained_log_count"] == 1
    assert updated.status_code == 200
    assert updated.json()["whitelist_keywords"] == ["Apple"]
    assert updated.json()["blacklist_keywords"] == ["天气", "Weather"]
    assert invalid.status_code == 422
    assert len(logs.json()["items"]) == 1
    assert reset.json()["blacklist_keywords"] == ["天气"]
    assert reset.json()["whitelist_keywords"] == []
