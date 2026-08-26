from datetime import UTC, datetime
from hashlib import sha256

from backend.app.config import Settings
from backend.app.domain import AssetClass, AssetRef, Market, NewsEvent, NewsItem, SourceQuality
from backend.app.services.asset_mapping import AssetMappingOutput, AssetMappingService


class StaticRegistry:
    def __init__(self, assets):
        self.assets = assets

    def resolve_assets(self, query):
        lowered = query.lower()
        return [
            asset
            for asset in self.assets
            if asset.symbol.lower() == lowered
            or asset.name.lower() == lowered
            or lowered in {alias.lower() for alias in asset.aliases}
        ]


class MappingLlm:
    def __init__(self):
        self.last_request = None

    def generate_json(self, **kwargs):
        self.last_request = kwargs
        return AssetMappingOutput(
            candidates=[
                {
                    "source_mention": "中际旭创",
                    "name": "中际旭创",
                    "symbol": "300308",
                    "market": "CN",
                    "asset_class": "equity",
                    "relationship": "direct",
                    "confidence": 0.94,
                    "rationale": "公司在原文中被明确提及",
                },
                {
                    "source_mention": "四川黄金",
                    "name": "四川黄金",
                    "symbol": "001337",
                    "market": "CN",
                    "asset_class": "equity",
                    "relationship": "entity",
                    "confidence": 0.81,
                    "rationale": "公司在原文中被明确提及",
                },
                {
                    "source_mention": "并未出现的公司",
                    "name": "虚构公司",
                    "symbol": "999999",
                    "market": "CN",
                    "asset_class": "equity",
                    "relationship": "entity",
                    "confidence": 0.99,
                    "rationale": "模型推断",
                },
                {
                    "source_mention": "调仓",
                    "name": "机器人ETF",
                    "symbol": "ETF1",
                    "market": "US",
                    "asset_class": "equity",
                    "relationship": "entity",
                    "confidence": 0.99,
                    "rationale": "代理标的",
                },
            ]
        ).model_dump(mode="json")


def test_7b_mapping_accepts_only_mentioned_master_verified_assets():
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    assets = [
        AssetRef(
            asset_id="equity:XSHE:300308",
            asset_class=AssetClass.EQUITY,
            market=Market.CN,
            symbol="300308",
            name="中际旭创",
            exchange_or_provider="XSHE",
            currency="CNY",
        ),
        AssetRef(
            asset_id="equity:XSHE:001337",
            asset_class=AssetClass.EQUITY,
            market=Market.CN,
            symbol="001337",
            name="四川黄金",
            exchange_or_provider="XSHE",
            currency="CNY",
        ),
    ]
    news = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="章建平调仓中际旭创与四川黄金",
        summary="两家公司出现在公开持仓信息中。",
        url="https://example.com/holdings",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"mapping").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="other",
        entities=["中际旭创", "四川黄金"],
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )

    mapping_llm = MappingLlm()
    result = AssetMappingService(
        StaticRegistry(assets),
        Settings(fmp_access_token="", fmp_mcp_url=""),
        mapping_llm,
    ).map_event(event, [news])

    assert mapping_llm.last_request["model"] == "qwen2.5:7b"
    assert mapping_llm.last_request["operation"] == "asset_mapping"
    assert [item.asset.symbol for item in result.candidates] == ["300308", "001337"]
    assert result.proposed_count == 4
    assert result.rejected_count == 2


def test_7b_mapping_rejects_substring_ticker_and_generic_issuer_name():
    observed = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    assets = [
        AssetRef(
            asset_id="crypto:coingecko:chainlink",
            asset_class=AssetClass.CRYPTO,
            market=Market.CRYPTO,
            symbol="LINK",
            name="Chainlink",
            exchange_or_provider="coingecko",
        ),
        AssetRef(
            asset_id="equity:XSHE:300024",
            asset_class=AssetClass.EQUITY,
            market=Market.CN,
            symbol="300024",
            name="机器人",
            exchange_or_provider="XSHE",
            currency="CNY",
            lot_size=100,
        ),
    ]
    news = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Starlink expands while robot adoption accelerates",
        summary="The report discusses Starlink and the robotics industry.",
        url="https://example.com/ambiguous-identities",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"ambiguous-identities").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="other",
        entities=["Starlink", "机器人"],
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )

    class AmbiguousMappingLlm:
        def generate_json(self, **kwargs):
            return AssetMappingOutput(
                candidates=[
                    {
                        "source_mention": "Starlink",
                        "name": "Chainlink",
                        "symbol": "LINK",
                        "market": "CRYPTO",
                        "asset_class": "crypto",
                        "relationship": "entity",
                        "confidence": 0.99,
                        "rationale": "substring ticker",
                    },
                    {
                        "source_mention": "机器人",
                        "name": "机器人",
                        "symbol": "300024",
                        "market": "CN",
                        "asset_class": "equity",
                        "relationship": "entity",
                        "confidence": 0.99,
                        "rationale": "generic industry term",
                    },
                ]
            ).model_dump(mode="json")

    result = AssetMappingService(
        StaticRegistry(assets),
        Settings(fmp_access_token="", fmp_mcp_url=""),
        AmbiguousMappingLlm(),
    ).map_event(event, [news])

    assert result.candidates == []
    assert result.proposed_count == 2
    assert result.rejected_count == 2


def test_7b_mapping_marks_issuer_only_adr_as_cross_listing():
    observed = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    mophy = AssetRef(
        asset_id="equity:OTC:MOPHY",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MOPHY",
        name="Monadelphous Group Limited Sponsored ADR",
        aliases=["Monadelphous"],
        exchange_or_provider="OTC",
        issuer_id="fmp:monadelphous-group",
        primary_listing_asset_id="equity:ASX:MND.AX",
    )
    news = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Monadelphous reports annual earnings in Australia",
        summary="The ASX-listed issuer published its results.",
        url="https://example.com/monadelphous",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"monadelphous-cross-listing").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="earnings",
        entities=["Monadelphous"],
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )

    class CrossListingLlm:
        def generate_json(self, **kwargs):
            return AssetMappingOutput(
                candidates=[
                    {
                        "source_mention": "Monadelphous",
                        "name": "Monadelphous Group Limited",
                        "symbol": "MOPHY",
                        "market": "US",
                        "asset_class": "equity",
                        "relationship": "entity",
                        "confidence": 0.94,
                        "rationale": "same issuer, different listing",
                    }
                ]
            ).model_dump(mode="json")

    result = AssetMappingService(
        StaticRegistry([mophy]),
        Settings(fmp_access_token="", fmp_mcp_url=""),
        CrossListingLlm(),
    ).map_event(event, [news])

    assert result.candidates[0].relationship == "cross_listing_issuer"
    assert result.candidates[0].relevance == 0.55
    assert result.candidates[0].mapping_confidence == 0.75
    assert "explicit_primary_listing" in result.candidates[0].identity_basis


def test_7b_symbol_hint_cannot_switch_to_sibling_listing():
    observed = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    underlying = AssetRef(
        asset_id="equity:ASX:MND.AX",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MND.AX",
        name="Monadelphous Group Limited",
        aliases=["Monadelphous"],
        exchange_or_provider="ASX",
        currency="AUD",
        issuer_id="fmp:monadelphous-group",
    )
    mophy = AssetRef(
        asset_id="equity:OTC:MOPHY",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MOPHY",
        name="Monadelphous Group Limited Sponsored ADR",
        aliases=["Monadelphous"],
        exchange_or_provider="OTC",
        issuer_id="fmp:monadelphous-group",
        primary_listing_asset_id=underlying.asset_id,
    )
    news = NewsItem(
        source="Example",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Monadelphous reports annual earnings in Australia",
        summary="The ASX-listed issuer published its results.",
        url="https://example.com/monadelphous-exact-listing",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"monadelphous-exact-listing").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="earnings",
        entities=["Monadelphous"],
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )

    class NoisyRegistry:
        @staticmethod
        def resolve_assets(_query):
            # Provider search can return sibling listings in arbitrary order.
            return [underlying, mophy]

    class ExactListingLlm:
        @staticmethod
        def generate_json(**kwargs):
            return AssetMappingOutput(
                candidates=[
                    {
                        "source_mention": "Monadelphous",
                        "name": "Monadelphous Group Limited Sponsored ADR",
                        "symbol": "MOPHY",
                        "market": "US",
                        "asset_class": "equity",
                        "relationship": "entity",
                        "confidence": 0.94,
                        "rationale": "the hint identifies the OTC ADR",
                    }
                ]
            ).model_dump(mode="json")

    result = AssetMappingService(
        NoisyRegistry(),
        Settings(fmp_access_token="", fmp_mcp_url=""),
        ExactListingLlm(),
    ).map_event(event, [news])

    assert [candidate.asset.asset_id for candidate in result.candidates] == [mophy.asset_id]
