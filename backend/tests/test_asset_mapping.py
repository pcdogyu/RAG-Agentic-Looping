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
