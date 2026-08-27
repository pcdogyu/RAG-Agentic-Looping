from datetime import UTC, datetime
from hashlib import sha256
from uuid import uuid4

import pytest

from backend.app.config import Settings
from backend.app.domain import (
    AssetClass,
    AssetRef,
    CandidateAsset,
    EventType,
    Market,
    NewsEvent,
    NewsItem,
    SourceQuality,
)
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


class StaticExtractLlm:
    def __init__(self, *, entities: list[str], search_queries: list[str]):
        self.entities = entities
        self.search_queries = search_queries

    def generate_json(self, **kwargs):
        return ExtractedEvent(
            event_type="other",
            entities=self.entities,
            direct_impact="身份映射测试",
            search_queries=self.search_queries,
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


def test_explicit_adverse_news_corrects_an_incorrect_positive_model_direction():
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    item = NewsItem(
        source="Example Wire",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Securities Fraud Class Action Lawsuit Filed Against Apple",
        summary="The complaint alleges material misstatements and investor losses.",
        url="https://example.com/apple-lawsuit",
        published_at=datetime(2025, 1, 31, tzinfo=UTC),
        as_of=datetime(2025, 1, 31, tzinfo=UTC),
        content_hash=sha256(b"apple-lawsuit").hexdigest(),
        symbols=["AAPL"],
    )

    event = EventService(registry, settings, FakeLlm()).extract(item)

    assert event.candidates[0].impact_direction == -1
    normalization = next(
        step for step in event.analysis_steps if step.phase == "direction_normalization"
    )
    assert normalization.metrics == {"model_direction": 1, "resolved_direction": -1}


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


def test_asset_mapping_does_not_extract_link_ticker_from_starlink(monkeypatch):
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        fmp_enabled=False,
        akshare_asset_master_enabled=False,
    )
    link = AssetRef(
        asset_id="crypto:coingecko:chainlink",
        asset_class=AssetClass.CRYPTO,
        market=Market.CRYPTO,
        symbol="LINK",
        name="Chainlink",
        exchange_or_provider="coingecko",
    )
    registry = ProviderRegistry(settings, assets=[link])
    monkeypatch.setattr(registry.crypto, "resolve_assets", lambda _query: [])
    item = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="SpaceX expands Starlink satellite service",
        summary="Starlink added coverage in another market.",
        url="https://example.com/starlink",
        published_at=datetime(2026, 8, 25, tzinfo=UTC),
        as_of=datetime(2026, 8, 25, tzinfo=UTC),
        content_hash=sha256(b"starlink-not-link").hexdigest(),
    )

    event = EventService(
        registry,
        settings,
        StaticExtractLlm(entities=["Starlink"], search_queries=["LINK"]),
    ).extract(item)

    assert event.candidates == []


def test_generic_robot_name_requires_explicit_ticker(monkeypatch):
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        fmp_enabled=False,
        akshare_asset_master_enabled=False,
    )
    robot = AssetRef(
        asset_id="equity:XSHE:300024",
        asset_class=AssetClass.EQUITY,
        market=Market.CN,
        symbol="300024",
        name="机器人",
        exchange_or_provider="XSHE",
        currency="CNY",
        lot_size=100,
    )
    registry = ProviderRegistry(settings, assets=[robot])
    monkeypatch.setattr(registry.crypto, "resolve_assets", lambda _query: [])
    generic = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="机器人产业迎来新一轮政策支持",
        summary="多地加快建设机器人产业集群。",
        url="https://example.com/robot-industry",
        published_at=datetime(2026, 8, 25, tzinfo=UTC),
        as_of=datetime(2026, 8, 25, tzinfo=UTC),
        content_hash=sha256(b"generic-robot").hexdigest(),
    )
    explicit = generic.model_copy(
        update={
            "title": "机器人（300024）发布半年度报告",
            "summary": "证券代码 300024。",
            "url": "https://example.com/robot-company",
            "content_hash": sha256(b"explicit-robot").hexdigest(),
            "symbols": ["300024"],
        }
    )
    llm = StaticExtractLlm(entities=["机器人"], search_queries=["300024"])
    service = EventService(registry, settings, llm)

    assert service.extract(generic).candidates == []
    assert service.extract(explicit).candidates[0].asset.asset_id == robot.asset_id


def test_short_ticker_in_ordinary_text_is_not_a_security_mention(monkeypatch):
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        fmp_enabled=False,
        akshare_asset_master_enabled=False,
    )
    asset = AssetRef(
        asset_id="equity:XNYS:AI",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="AI",
        name="C3.ai, Inc.",
        aliases=["C3.ai"],
        exchange_or_provider="XNYS",
    )
    registry = ProviderRegistry(settings, assets=[asset])
    monkeypatch.setattr(registry.crypto, "resolve_assets", lambda _query: [])
    item = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="AI investment accelerates across the software industry",
        summary="Enterprises increased AI infrastructure spending.",
        url="https://example.com/ai-industry",
        published_at=datetime(2026, 8, 25, tzinfo=UTC),
        as_of=datetime(2026, 8, 25, tzinfo=UTC),
        content_hash=sha256(b"ordinary-ai-text").hexdigest(),
    )

    event = EventService(
        registry,
        settings,
        StaticExtractLlm(entities=["AI"], search_queries=["AI"]),
    ).extract(item)

    assert event.candidates == []


@pytest.mark.parametrize("explicit_mention", ["$AI", "(AI)", "NYSE:AI"])
def test_short_ticker_requires_explicit_security_syntax(monkeypatch, explicit_mention):
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        fmp_enabled=False,
        akshare_asset_master_enabled=False,
    )
    asset = AssetRef(
        asset_id="equity:XNYS:AI",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="AI",
        name="C3.ai, Inc.",
        aliases=["C3.ai"],
        exchange_or_provider="XNYS",
    )
    registry = ProviderRegistry(settings, assets=[asset])
    monkeypatch.setattr(registry.crypto, "resolve_assets", lambda _query: [])
    item = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title=f"{explicit_mention} reports quarterly results",
        summary="The listed company published its results.",
        url=f"https://example.com/ai-{explicit_mention}",
        published_at=datetime(2026, 8, 25, tzinfo=UTC),
        as_of=datetime(2026, 8, 25, tzinfo=UTC),
        content_hash=sha256(explicit_mention.encode()).hexdigest(),
    )

    event = EventService(
        registry,
        settings,
        StaticExtractLlm(entities=[], search_queries=["AI"]),
    ).extract(item)

    assert event.candidates[0].asset.asset_id == asset.asset_id
    assert event.candidates[0].relationship == "direct"


def test_short_ticker_from_provider_symbols_is_explicit(monkeypatch):
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        fmp_enabled=False,
        akshare_asset_master_enabled=False,
    )
    asset = AssetRef(
        asset_id="equity:XNYS:AI",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="AI",
        name="C3.ai, Inc.",
        aliases=["C3.ai"],
        exchange_or_provider="XNYS",
    )
    registry = ProviderRegistry(settings, assets=[asset])
    monkeypatch.setattr(registry.crypto, "resolve_assets", lambda _query: [])
    item = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="AI investment accelerates across the software industry",
        summary="The provider attached a verified listing symbol.",
        url="https://example.com/ai-provider-symbol",
        published_at=datetime(2026, 8, 25, tzinfo=UTC),
        as_of=datetime(2026, 8, 25, tzinfo=UTC),
        content_hash=sha256(b"ai-provider-symbol").hexdigest(),
        symbols=["AI"],
    )

    event = EventService(
        registry,
        settings,
        StaticExtractLlm(entities=["AI"], search_queries=["AI"]),
    ).extract(item)

    assert event.candidates[0].asset.asset_id == asset.asset_id
    assert event.candidates[0].relationship == "direct"


def test_similar_filing_titles_from_different_issuers_do_not_cluster():
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    service = EventService(registry, settings, FakeLlm())
    published = datetime(2026, 8, 25, tzinfo=UTC)

    def event(name: str, symbol: str, title: str) -> NewsEvent:
        asset = AssetRef(
            asset_id=f"equity:XSHE:{symbol}",
            asset_class=AssetClass.EQUITY,
            market=Market.CN,
            symbol=symbol,
            name=name,
            exchange_or_provider="XSHE",
            currency="CNY",
            lot_size=100,
        )
        return NewsEvent(
            news_item_ids=[uuid4()],
            headline=title,
            event_type=EventType.EARNINGS,
            entities=[name],
            direct_impact="发布半年度报告",
            source_quality=SourceQuality.OFFICIAL,
            published_at=published,
            observed_at=published,
            as_of=published,
            candidates=[
                CandidateAsset(
                    asset=asset,
                    relationship="direct",
                    relevance=0.95,
                    rationale="标题明确提及发行人",
                )
            ],
        )

    boyun = event("博云新材", "002297", "博云新材发布2026年半年度报告")
    boke = event("铂科新材", "300811", "铂科新材发布2026年半年度报告")

    assert service._same_story(boyun, boke) is False


def test_clustering_does_not_merge_on_shared_secondary_candidate():
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    service = EventService(registry, settings, FakeLlm())
    published = datetime(2026, 8, 25, tzinfo=UTC)
    apple = registry.get_asset("equity:XNAS:AAPL")
    assert apple is not None

    def event(name: str, symbol: str, title: str) -> NewsEvent:
        primary = AssetRef(
            asset_id=f"equity:XSHE:{symbol}",
            asset_class=AssetClass.EQUITY,
            market=Market.CN,
            symbol=symbol,
            name=name,
            exchange_or_provider="XSHE",
            currency="CNY",
            lot_size=100,
        )
        return NewsEvent(
            news_item_ids=[uuid4()],
            headline=title,
            event_type=EventType.EARNINGS,
            entities=[name, "Apple"],
            direct_impact="发布半年度报告",
            source_quality=SourceQuality.OFFICIAL,
            published_at=published,
            observed_at=published,
            as_of=published,
            candidates=[
                CandidateAsset(
                    asset=primary,
                    relationship="direct",
                    relevance=0.90,
                    rationale="标题明确提及发行人",
                ),
                CandidateAsset(
                    asset=apple,
                    relationship="related",
                    relevance=0.99,
                    rationale="次要供应链关系",
                ),
            ],
        )

    boyun = event("博云新材", "002297", "博云新材发布2026年半年度报告")
    boke = event("铂科新材", "300811", "铂科新材发布2026年半年度报告")

    assert service._same_story(boyun, boke) is False


def test_cross_market_listings_share_issuer_identity():
    settings = Settings(fmp_access_token="", fmp_mcp_url="")
    registry = ProviderRegistry(settings)
    mophy = AssetRef(
        asset_id="equity:OTC:MOPHY",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MOPHY",
        name="Monadelphous Group Limited Sponsored ADR",
        exchange_or_provider="OTC",
        issuer_id="fmp:monadelphous-group",
        primary_listing_asset_id="equity:ASX:MND.AX",
    )
    underlying = AssetRef(
        asset_id="equity:ASX:MND.AX",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MND.AX",
        name="Monadelphous Group Limited",
        exchange_or_provider="ASX",
        currency="AUD",
        issuer_id="fmp:monadelphous-group",
    )

    assert registry.same_issuer(mophy, underlying) is True
    assert registry.issuer_key(mophy) == registry.issuer_key(underlying)
    assert registry._broad_benchmark(mophy).symbol == "STW.AX"

    published = datetime(2026, 8, 25, tzinfo=UTC)

    def event(asset: AssetRef, headline: str) -> NewsEvent:
        return NewsEvent(
            news_item_ids=[uuid4()],
            headline=headline,
            event_type=EventType.EARNINGS,
            entities=["Monadelphous"],
            direct_impact="Annual earnings",
            source_quality=SourceQuality.PROFESSIONAL,
            published_at=published,
            observed_at=published,
            as_of=published,
            candidates=[
                CandidateAsset(
                    asset=asset,
                    relationship="direct",
                    relevance=0.95,
                    rationale="explicit listing",
                )
            ],
        )

    service = EventService(registry, settings, FakeLlm())
    assert service._same_story(
        event(underlying, "Monadelphous reports annual earnings on ASX"),
        event(mophy, "Monadelphous reports annual earnings via OTC ADR"),
    )
