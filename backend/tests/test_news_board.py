from datetime import UTC, datetime, timedelta
from hashlib import sha256

from fastapi.testclient import TestClient

from backend.app.db import NewsSourceStateRow
from backend.app.domain import (
    AnalysisStep,
    CandidateAsset,
    EventResearchRun,
    EventType,
    NewsEvent,
    NewsItem,
    ResearchRun,
    RunStatus,
    SourceQuality,
)
from backend.app.main import app
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.news_board import build_news_board
from backend.app.storage import save_event, save_event_research_run, save_news, save_run

BASE_TIME = datetime(2026, 8, 26, 4, 0, tzinfo=UTC)


def news(title: str, *, source: str = "金十", minutes: int = 0) -> NewsItem:
    published_at = BASE_TIME + timedelta(minutes=minutes)
    return NewsItem(
        source=source,
        source_quality=SourceQuality.PROFESSIONAL,
        title=title,
        summary=f"{title} summary",
        url=f"https://example.com/{source}/{minutes}/{title}",
        language="zh-CN",
        published_at=published_at,
        observed_at=published_at,
        as_of=published_at,
        content_hash=sha256(f"{source}:{minutes}:{title}".encode()).hexdigest(),
    )


def event_for(
    item: NewsItem,
    *,
    status: str = "completed",
    candidates: bool = True,
) -> NewsEvent:
    asset = SEED_ASSETS[1]
    return NewsEvent(
        news_item_ids=[item.id],
        headline=item.title,
        event_type=EventType.EARNINGS,
        direct_impact=item.summary,
        source_quality=item.source_quality,
        published_at=item.published_at,
        observed_at=item.observed_at,
        as_of=item.as_of,
        candidates=(
            [
                CandidateAsset(
                    asset=asset,
                    relationship="direct",
                    relevance=0.9,
                    rationale="test mapping",
                )
            ]
            if candidates
            else []
        ),
        priority=0.8,
        analysis_steps=[
            AnalysisStep(
                phase="asset_mapping",
                status=status,
                executor="test",
                occurred_at=item.observed_at,
                summary="mapping state",
            )
        ],
    )


def test_news_board_limits_each_source_and_orders_sources_by_latest_item(db):
    for index in range(52):
        assert save_news(db, news(f"金十-{index}", minutes=index))
        assert save_news(db, news(f"FMP-{index}", source="FMP Stock News", minutes=index + 100))

    payload = build_news_board(db, per_source=50)

    assert payload.total_sources == 2
    assert [group.source for group in payload.sources] == ["FMP Stock News", "金十"]
    assert all(group.item_count == 50 for group in payload.sources)
    assert payload.sources[0].items[0].title == "FMP-51"
    assert payload.sources[1].items[-1].title == "金十-2"


def test_news_board_exposes_source_refresh_health_and_errors(db):
    attempted = BASE_TIME + timedelta(hours=3)
    db.add_all(
        [
            NewsSourceStateRow(
                source="金十",
                provider="mcp-news",
                status="healthy",
                watermark_at=attempted,
                last_attempt_at=attempted,
                last_success_at=attempted,
                last_discovered_count=5,
                last_new_count=2,
            ),
            NewsSourceStateRow(
                source="东方财富/AkShare",
                provider="akshare",
                status="error",
                last_attempt_at=attempted + timedelta(minutes=1),
                last_error="ProviderError: upstream unavailable",
                consecutive_failures=1,
            ),
        ]
    )
    db.commit()
    assert save_news(db, news("金十刷新", minutes=1))

    payload = build_news_board(db)

    groups = {group.source: group for group in payload.sources}
    assert payload.last_refresh_at == attempted + timedelta(minutes=1)
    assert payload.last_success_at == attempted
    assert groups["金十"].discovery_status == "healthy"
    assert groups["金十"].last_discovered_count == 5
    assert groups["金十"].last_new_count == 2
    assert groups["东方财富/AkShare"].discovery_status == "error"
    assert groups["东方财富/AkShare"].last_error == "ProviderError: upstream unavailable"


def test_news_board_projects_pipeline_states_and_status_priority(db):
    items = {
        name: news(name, minutes=index)
        for index, name in enumerate(
            [
                "pending",
                "extracting",
                "mapping",
                "researching",
                "revising",
                "completed",
                "insufficient",
                "failed",
                "priority",
                "event-revising",
            ]
        )
    }
    for item in items.values():
        assert save_news(db, item)

    events: dict[str, NewsEvent] = {}
    for name in ["mapping", "researching", "revising", "completed", "insufficient", "failed"]:
        events[name] = event_for(
            items[name],
            status="running" if name == "mapping" else "completed",
        )
        save_event(db, events[name])

    priority_event = event_for(items["priority"], status="running")
    save_event(db, priority_event)
    events["priority"] = priority_event
    event_revising = event_for(items["event-revising"], candidates=False)
    save_event(db, event_revising)
    events["event-revising"] = event_revising
    save_event_research_run(
        db,
        EventResearchRun(event_id=event_revising.id, status=RunStatus.VERIFYING),
    )

    for name, status in [
        ("researching", RunStatus.RUNNING),
        ("revising", RunStatus.VERIFYING),
        ("completed", RunStatus.COMPLETED),
        ("insufficient", RunStatus.INSUFFICIENT_EVIDENCE),
        ("failed", RunStatus.FAILED),
        ("priority", RunStatus.VERIFYING),
    ]:
        save_run(
            db,
            ResearchRun(
                event_id=events[name].id,
                asset=SEED_ASSETS[1],
                status=status,
                created_at=items[name].observed_at,
                updated_at=items[name].observed_at,
            ),
        )

    payload = build_news_board(
        db,
        per_source=50,
        extraction_items=[
            {
                "news_id": str(items[name].id),
                "status": "running",
                "updated_at": items[name].observed_at.isoformat(),
            }
            for name in ["extracting", "priority"]
        ],
    )
    statuses = {item.title: item.status for item in payload.sources[0].items}

    assert statuses == {
        "pending": "orphaned",
        "extracting": "extracting",
        "mapping": "mapping",
        "researching": "researching",
        "revising": "revising",
        "completed": "completed",
        "insufficient": "insufficient_evidence",
        "failed": "failed",
        "priority": "revising",
        "event-revising": "revising",
    }
    revising = next(item for item in payload.sources[0].items if item.title == "revising")
    assert revising.events[0].event_type == "earnings"
    assert revising.assets[0].symbol == SEED_ASSETS[1].symbol


def test_news_board_prefers_success_when_related_terminal_runs_disagree(db):
    item = news("terminal-priority")
    assert save_news(db, item)
    event = event_for(item)
    save_event(db, event)
    for status in [RunStatus.FAILED, RunStatus.INSUFFICIENT_EVIDENCE, RunStatus.COMPLETED]:
        save_run(
            db,
            ResearchRun(event_id=event.id, asset=SEED_ASSETS[1], status=status),
        )

    payload = build_news_board(db)

    assert payload.sources[0].items[0].status == "completed"


def test_news_board_api_remains_available_when_extraction_registry_is_unavailable(
    db, monkeypatch
):
    assert save_news(db, news("redis-fallback"))
    monkeypatch.setattr(
        "backend.app.main.get_news_extraction_queue",
        lambda limit: {"state": "unavailable", "items": [], "error": "redis unavailable"},
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/news-board?per_source=50")

    assert response.status_code == 200
    item = response.json()["sources"][0]["items"][0]
    assert item["status"] == "orphaned"
    assert item["retryable"] is True
    with TestClient(app) as client:
        invalid = client.get("/api/v1/news-board?per_source=51")
    assert invalid.status_code == 422
