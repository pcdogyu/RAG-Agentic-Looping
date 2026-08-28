from datetime import UTC, datetime, timedelta
from hashlib import sha256

from fastapi.testclient import TestClient

from backend.app.db import NewsFilterLogRow
from backend.app.domain import NewsItem, SourceQuality, utc_now
from backend.app.main import app
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService, ExtractedEvent
from backend.app.services.mcp_registry import normalize_news_feed_page
from backend.app.services.source_filter import (
    WHITELIST_MISS_REASON,
    SourceFilterConfig,
    evaluate_title,
    filter_news_items,
    list_filter_logs,
    save_source_filter,
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


def test_enabled_filter_requires_a_whitelist_match_and_empty_whitelist_blocks_all():
    empty = SourceFilterConfig()
    decision = evaluate_title("Apple earnings", empty)
    assert decision.allowed is False
    assert decision.matched_keyword == WHITELIST_MISS_REASON

    config = SourceFilterConfig(whitelist_keywords=["Apple"], blacklist_keywords=[])
    assert evaluate_title("Apple earnings", config).allowed is True
    assert evaluate_title("Microsoft earnings", config).allowed is False
    assert evaluate_title("今日天气预报", SourceFilterConfig(enabled=False)).allowed is True


def test_whitelist_miss_is_recorded_as_a_filter_reason(db):
    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["Apple"], blacklist_keywords=["天气"]),
    )

    accepted, filtered = filter_news_items(
        db,
        [news("Microsoft earnings", suffix="whitelist-miss")],
    )

    assert accepted == []
    assert filtered == 1
    assert list_filter_logs(db)[0]["matched_keyword"] == WHITELIST_MISS_REASON
    assert list_filter_logs(db)[0]["rescan_allowed"] is True


def test_blacklist_vetoes_whitelist_and_keywords_are_normalized():
    config = SourceFilterConfig(
        whitelist_keywords=[" Apple ", "ＡＰＰＬＥ", ""],
        blacklist_keywords=["WEATHER", "weather"],
    )
    assert config.whitelist_keywords == ["Apple"]
    assert config.blacklist_keywords == ["WEATHER"]
    decision = evaluate_title("APPLE WEATHER supply-chain disruption", config)
    assert decision.allowed is False
    assert decision.matched_keyword == "WEATHER"

    allowed = evaluate_title("ＡＰＰＬＥ supply-chain disruption", config)
    assert allowed.allowed is True
    assert allowed.matched_keyword == "Apple"


def test_filtered_news_is_audited_but_never_enters_news_or_event_tables(db):
    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["Apple"], blacklist_keywords=["天气"]),
    )
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


def test_jin10_synthesized_flash_title_uses_the_existing_filter_chain(db):
    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["股票"], blacklist_keywords=["天气"]),
    )
    now = utc_now().replace(microsecond=0)
    items, _, _, _ = normalize_news_feed_page(
        {
            "data": {
                "items": [
                    {
                        "title": "",
                        "content": "北京今日天气晴朗。该内容与股票研究无关。",
                        "time": now.isoformat(),
                        "url": "https://jin10.example/flash/weather",
                    }
                ]
            }
        },
        "金十",
        "jin10_flash_v1",
        now - timedelta(minutes=1),
    )

    accepted, filtered = filter_news_items(db, items)

    assert accepted == []
    assert filtered == 1
    assert list_filter_logs(db)[0]["source"] == "金十"
    assert list_filter_logs(db)[0]["matched_keyword"] == "天气"


def test_filter_log_retention_removes_old_and_overflow_rows(db, monkeypatch):
    import backend.app.services.source_filter as source_filter

    monkeypatch.setattr(source_filter, "FILTER_LOG_MAX_ROWS", 2)
    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["市场"], blacklist_keywords=["天气"]),
    )
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


def test_whitelist_miss_rescan_restores_news_and_queues_full_pipeline(
    db, monkeypatch
):
    from backend.app import worker

    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["Apple"], blacklist_keywords=["天气"]),
    )
    blocked = news("Microsoft earnings", suffix="manual-rescan")
    filter_news_items(db, [blocked])
    log = list_filter_logs(db)[0]
    queued = []
    monkeypatch.setattr(
        worker,
        "enqueue_news_extraction_retry",
        lambda item, **kwargs: queued.append((item, kwargs)) or "extract-task",
    )

    with TestClient(app) as client:
        response = client.post(f"/api/v1/source-filter/logs/{log['id']}/rescan")

    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    assert response.json()["task_id"] == "extract-task"
    assert queued[0][0].title == blocked.title
    assert queued[0][0].raw_metadata["manual_source_filter_rescan"] is True
    assert queued[0][1] == {"force_asset_mapping": True}
    assert get_news(db, queued[0][0].id) is not None
    assert list_filter_logs(db) == []


def test_blacklist_filter_record_cannot_be_manually_rescanned(db, monkeypatch):
    from backend.app import worker

    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["天气"], blacklist_keywords=["天气"]),
    )
    filter_news_items(db, [news("城市天气预报", suffix="blocked-rescan")])
    log = list_filter_logs(db)[0]
    monkeypatch.setattr(
        worker,
        "enqueue_news_extraction_retry",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("blacklist record was queued")
        ),
    )

    with TestClient(app) as client:
        response = client.post(f"/api/v1/source-filter/logs/{log['id']}/rescan")

    assert response.status_code == 409
    assert len(list_filter_logs(db)) == 1


def test_whitelist_miss_rescan_restores_audit_when_queue_publish_fails(
    db, monkeypatch
):
    from backend.app import worker

    save_source_filter(
        db,
        SourceFilterConfig(whitelist_keywords=["Apple"], blacklist_keywords=[]),
    )
    blocked = news("Microsoft earnings", suffix="rescan-publish-failure")
    filter_news_items(db, [blocked])
    log = list_filter_logs(db)[0]
    monkeypatch.setattr(
        worker,
        "enqueue_news_extraction_retry",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("broker down")),
    )

    with TestClient(app) as client:
        response = client.post(f"/api/v1/source-filter/logs/{log['id']}/rescan")

    assert response.status_code == 503
    restored = list_filter_logs(db)
    assert len(restored) == 1
    assert restored[0]["id"] == log["id"]
    assert restored[0]["rescan_allowed"] is True
