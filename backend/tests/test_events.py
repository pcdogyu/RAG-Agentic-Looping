from datetime import UTC, datetime
from hashlib import sha256

import pytest

from backend.app.config import Settings
from backend.app.domain import NewsItem, SourceQuality
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService, ExtractedEvent


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
