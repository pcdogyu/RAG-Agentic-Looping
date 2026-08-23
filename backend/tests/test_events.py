from datetime import UTC, datetime
from hashlib import sha256
from uuid import uuid4

import pytest

from backend.app.config import Settings
from backend.app.domain import NewsItem, SourceQuality
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService, ExtractedEvent
from backend.app.services.source_lineage import LINEAGE_KEY
from backend.app.storage import get_news, list_events, save_news


class FakeLlm:
    def generate_json(self, **kwargs):
        return ExtractedEvent(
            event_type="earnings",
            entities=["Apple"],
            direct_impact="Services revenue grew",
            impact_direction=1,
            novelty=0.7,
            priority=0.8,
            search_queries=["AAPL"],
        ).model_dump(mode="json")


def test_extracts_event_and_maps_asset():
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    item = NewsItem(
        source="Example Wire",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Apple reports Services growth",
        summary="Apple reported quarterly Services revenue growth.",
        url="https://example.com/apple",
        published_at=datetime(2025, 1, 31, tzinfo=UTC),
        as_of=datetime(2025, 1, 31, tzinfo=UTC),
        content_hash=sha256(b"apple").hexdigest(),
        symbols=["AAPL"],
    )
    event = EventService(registry, settings, FakeLlm()).extract(item)
    assert event.event_type.value == "earnings"
    assert event.candidates[0].asset.asset_id == "equity:XNAS:AAPL"
    assert event.candidates[0].impact_direction == 1
    assert event.priority == pytest.approx(0.64)
    assert [step.phase for step in event.analysis_steps] == [
        "news_collection",
        "event_extraction",
        "asset_mapping",
    ]
    assert event.analysis_steps[-2].model == settings.ollama_extract_model
    assert event.analysis_steps[-1].metrics["candidate_count"] == 1


def test_ingest_clusters_duplicate_reprints(db):
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    published = datetime(2025, 1, 31, tzinfo=UTC)
    items = [
        NewsItem(
            source=source,
            source_quality=SourceQuality.PROFESSIONAL,
            title=title,
            summary="Services revenue grew.",
            url=f"https://{domain}/apple",
            published_at=published,
            observed_at=published,
            as_of=published,
            content_hash=sha256(title.encode()).hexdigest(),
            symbols=["AAPL"],
            raw_metadata={"site": domain},
        )
        for source, domain, title in (
            ("Wire A", "a.example", "Apple reports Services revenue growth"),
            ("Wire B", "b.example", "Apple reports growth in Services revenue"),
        )
    ]

    events = EventService(registry, settings, FakeLlm()).ingest(db, items)

    assert len(events) == 1
    assert len(events[0].news_item_ids) == 2
    assert len(list_events(db)) == 1
    assert len(list_events(db)[0].news_item_ids) == 2


def test_ingest_merges_matching_story_across_scan_batches(db):
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    published = datetime(2025, 1, 31, tzinfo=UTC)
    first = NewsItem(
        source="Wire A",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Apple reports Services revenue growth",
        summary="Services revenue grew.",
        url="https://a.example/apple?utm_source=feed",
        published_at=published,
        observed_at=published,
        as_of=published,
        content_hash=sha256(b"cross-scan-a").hexdigest(),
        symbols=["AAPL"],
    )
    second = NewsItem(
        source="Wire B",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Apple reports growth in Services revenue",
        summary="Apple Services revenue grew.",
        url="https://b.example/apple",
        published_at=published,
        observed_at=published,
        as_of=published,
        content_hash=sha256(b"cross-scan-b").hexdigest(),
        symbols=["AAPL"],
    )

    first_batch = EventService(registry, settings, FakeLlm()).ingest(db, [first])
    second_batch = EventService(registry, settings, FakeLlm()).ingest(db, [second])

    assert first_batch[0].id == second_batch[0].id
    assert len(list_events(db)) == 1
    assert len(list_events(db)[0].news_item_ids) == 2
    stored = get_news(db, first.id)
    assert stored is not None
    assert stored.raw_metadata[LINEAGE_KEY]["canonical_url"] == "https://a.example/apple"
    assert stored.raw_metadata[LINEAGE_KEY]["publisher_domain"] == "a.example"


def test_persistent_clustering_keeps_unrelated_asset_stories_separate(db):
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    published = datetime(2025, 1, 31, tzinfo=UTC)
    items = [
        NewsItem(
            source="Wire",
            source_quality=SourceQuality.PROFESSIONAL,
            title=title,
            summary=title,
            url=url,
            published_at=published,
            observed_at=published,
            as_of=published,
            content_hash=sha256(url.encode()).hexdigest(),
            symbols=["AAPL"],
        )
        for title, url in (
            ("Apple reports Services revenue growth", "https://a.example/earnings"),
            ("Apple opens a new retail store in Shanghai", "https://b.example/retail"),
        )
    ]

    EventService(registry, settings, FakeLlm()).ingest(db, [items[0]])
    EventService(registry, settings, FakeLlm()).ingest(db, [items[1]])

    assert len(list_events(db)) == 2


def test_ingest_resumes_news_saved_before_event_extraction(db):
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    published = datetime(2025, 1, 31, tzinfo=UTC)
    stored = NewsItem(
        source="Wire A",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Apple reports Services revenue growth",
        summary="Services revenue grew.",
        url="https://a.example/apple",
        published_at=published,
        observed_at=published,
        as_of=published,
        content_hash=sha256(b"orphaned-news").hexdigest(),
        symbols=["AAPL"],
    )
    assert save_news(db, stored)

    rediscovered = stored.model_copy(update={"id": uuid4()})
    events = EventService(registry, settings, FakeLlm()).ingest(db, [rediscovered])

    assert len(events) == 1
    assert events[0].news_item_ids == [stored.id]
    assert len(list_events(db)) == 1
