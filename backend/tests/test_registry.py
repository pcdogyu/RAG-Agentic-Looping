from datetime import UTC, datetime, timedelta
from hashlib import sha256

from backend.app.config import Settings
from backend.app.domain import AssetClass, AssetRef, Market, NewsItem, SourceQuality
from backend.app.providers.registry import ProviderRegistry


class PriceProvider:
    name = "akshare"

    def __init__(self, *, available: bool = True):
        self.available = available
        self.requested_symbols: list[str] = []

    def get_prices(self, asset, **kwargs):
        self.requested_symbols.append(asset.symbol)
        if not self.available:
            raise RuntimeError("unavailable")
        start = datetime(2025, 1, 1, tzinfo=UTC)
        return [
            {
                "日期": (start + timedelta(days=index)).date().isoformat(),
                "收盘": index + 1,
                "成交量": 1000 + index,
            }
            for index in reversed(range(300))
        ]

    @staticmethod
    def get_fundamentals(asset):
        return {}

    @staticmethod
    def get_filings(asset):
        return []


class FundamentalsProvider(PriceProvider):
    @staticmethod
    def get_fundamentals(asset):
        return {
            "provider": "akshare",
            "financial_indicators": [
                {
                    "报告日期": "2025-06-30",
                    "营业收入": 120,
                    "归母净利润": 18,
                }
            ],
        }


def _cn_asset() -> AssetRef:
    return AssetRef(
        asset_id="equity:XSHG:688251",
        asset_class=AssetClass.EQUITY,
        market=Market.CN,
        symbol="688251",
        name="井松智能",
        exchange_or_provider="XSHG",
        currency="CNY",
        lot_size=100,
    )


def test_news_discovery_keeps_an_independent_budget_per_source(db, monkeypatch):
    now = datetime(2026, 8, 30, 3, tzinfo=UTC)

    class NewsProvider:
        def __init__(self, name, source):
            self.name = name
            self.source = source

        def discover_news(self, *, since, limit):
            assert since < now
            return [
                NewsItem(
                    source=self.source,
                    source_quality=SourceQuality.PROFESSIONAL,
                    title=f"{self.source}-{index}",
                    url=f"https://example.com/{self.source}/{index}",
                    published_at=now - timedelta(minutes=index),
                    as_of=now - timedelta(minutes=index),
                    content_hash=sha256(f"{self.source}-{index}".encode()).hexdigest(),
                )
                for index in range(3)
            ]

    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    registry.providers = [NewsProvider("one", "来源一"), NewsProvider("two", "来源二")]
    monkeypatch.setattr(
        "backend.app.providers.registry.fetch_enabled_news_feeds_sync",
        lambda since, limit: ([], []),
    )

    items = registry.discover_news(since=now - timedelta(hours=1), limit=2)

    assert len(items) == 4
    assert {source: sum(item.source == source for item in items) for source in {"来源一", "来源二"}} == {
        "来源一": 2,
        "来源二": 2,
    }


def test_special_purpose_companies_are_not_industry_representatives():
    shell = AssetRef(
        asset_id="equity:XNAS:SHELL",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="SHELL",
        name="Example Acquisition Corp",
        exchange_or_provider="NASDAQ",
        sector_id="sector:financials",
        industry_id="industry:special_purpose",
        instrument_type="shell_company",
        market_cap=1_000_000_000,
    )
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""), assets=[shell])

    assert registry.industry_representatives(["industry:special_purpose"]) == []


def test_research_data_includes_normalized_market_and_benchmark_prices(monkeypatch):
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    provider = PriceProvider()
    monkeypatch.setattr(registry, "provider_for", lambda _asset: provider)
    monkeypatch.setattr(
        "backend.app.providers.registry.call_enabled_purpose_sync",
        lambda *args, **kwargs: ([], []),
    )

    payload = registry.get_research_data(_cn_asset())

    assert provider.requested_symbols[:2] == ["688251", "510300"]
    assert len(payload["prices"]) == 250
    assert len(payload["benchmark_prices"]) == 250
    assert payload["industry_prices"] == []
    assert payload["expectations"] == {}
    assert payload["prices"][0] == {
        "date": "2025-02-20",
        "close": 51.0,
        "volume": 1050.0,
    }
    assert set(payload["factor_sources"]) == {"market", "benchmark"}
    assert payload["factor_sources"]["market"]["quality"] == "aggregator"
    assert payload["factor_sources"]["benchmark"]["name"].endswith("510300 历史行情")


def test_research_data_keeps_missing_prices_empty_without_source_metadata(monkeypatch):
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    provider = PriceProvider(available=False)
    monkeypatch.setattr(registry, "provider_for", lambda _asset: provider)
    monkeypatch.setattr(
        "backend.app.providers.registry.call_enabled_purpose_sync",
        lambda *args, **kwargs: ([], []),
    )

    payload = registry.get_research_data(_cn_asset())

    assert payload["prices"] == []
    assert payload["benchmark_prices"] == []
    assert payload["industry_prices"] == []
    assert payload["factor_sources"] == {}


def test_native_fundamentals_include_traceable_source_without_fabricated_feeds(
    monkeypatch,
):
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    provider = FundamentalsProvider()
    monkeypatch.setattr(registry, "provider_for", lambda _asset: provider)
    monkeypatch.setattr(
        "backend.app.providers.registry.call_enabled_purpose_sync",
        lambda *args, **kwargs: ([], []),
    )

    payload = registry.get_research_data(_cn_asset())

    source = payload["factor_sources"]["fundamentals"]
    assert source["name"].startswith("东方财富/AkShare")
    assert "code=SH688251" in source["url"]
    assert source["independent_group"].endswith("equity:XSHG:688251")
    assert payload["expectations"] == {}
    assert payload["industry_prices"] == []
    assert "expectations" not in payload["factor_sources"]
    assert "industry" not in payload["factor_sources"]


def test_mixed_unattributed_mcp_fundamentals_do_not_claim_native_source(monkeypatch):
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    provider = FundamentalsProvider()
    monkeypatch.setattr(registry, "provider_for", lambda _asset: provider)

    def mcp_payload(purpose, *_args, **_kwargs):
        if purpose == "fundamentals":
            return [
                (
                    "unattributed-mcp",
                    {"data": {"income": [{"date": "2025-06-30", "revenue": 120}]}},
                )
            ], []
        return [], []

    monkeypatch.setattr(
        "backend.app.providers.registry.call_enabled_purpose_sync",
        mcp_payload,
    )

    payload = registry.get_research_data(_cn_asset())

    assert "income" in payload["fundamentals"]
    assert "financial_indicators" in payload["fundamentals"]
    assert "fundamentals" not in payload["factor_sources"]


def test_cross_listing_uses_traded_listing_for_prices_and_primary_for_issuer_facts(
    monkeypatch,
):
    registry = ProviderRegistry(Settings(fmp_access_token="", fmp_mcp_url=""))
    asset = AssetRef(
        asset_id="equity:OTC:MOPHY",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MOPHY",
        name="Monadelphous Group Limited ADR",
        exchange_or_provider="OTC",
        currency="USD",
        issuer_id="issuer:monadelphous",
        primary_listing_asset_id="equity:ASX:MND.AX",
    )
    price_symbols: list[str] = []
    fundamental_symbols: list[str] = []
    filing_symbols: list[str] = []

    def prices(requested, **_kwargs):
        price_symbols.append(requested.symbol)
        return [{"date": "2026-08-25", "close": 10}]

    def fundamentals(requested):
        fundamental_symbols.append(requested.symbol)
        return {"income": [{"date": "2026-06-30", "revenue": 100}]}

    def filings(requested):
        filing_symbols.append(requested.symbol)
        return [{"symbol": requested.symbol, "link": "https://example.invalid/filing"}]

    monkeypatch.setattr(registry, "_source_enabled", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(registry.fmp, "get_prices", prices)
    monkeypatch.setattr(registry.fmp, "get_fundamentals", fundamentals)
    monkeypatch.setattr(registry.fmp, "get_filings", filings)
    monkeypatch.setattr(
        registry.sec,
        "get_filings",
        lambda _asset: (_ for _ in ()).throw(AssertionError("ASX issuer queried via SEC")),
    )
    monkeypatch.setattr(
        "backend.app.providers.registry.call_enabled_purpose_sync",
        lambda *args, **kwargs: ([], []),
    )

    payload = registry.get_research_data(asset)

    assert price_symbols == ["MOPHY", "STW.AX"]
    assert fundamental_symbols == ["MND.AX"]
    assert filing_symbols == ["MND.AX"]
    assert payload["issuer_research_asset"]["asset_id"] == "equity:ASX:MND.AX"
    assert payload["factor_sources"]["fundamentals"]["name"].endswith("MND.AX 财务报表")
