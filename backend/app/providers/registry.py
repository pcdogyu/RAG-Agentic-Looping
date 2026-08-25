from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select

from backend.app.config import Settings, get_settings
from backend.app.db import McpSourceRow, SessionLocal
from backend.app.domain import AssetClass, AssetRef, Market, NewsItem
from backend.app.providers.akshare_provider import AkShareProvider
from backend.app.providers.crypto import CryptoProvider
from backend.app.providers.fmp import FmpProvider
from backend.app.providers.rss import RssProvider
from backend.app.providers.sec import SecProvider
from backend.app.services.mcp_registry import call_enabled_purpose_sync

SEED_ASSETS = [
    AssetRef(
        asset_id="equity:XNAS:AAPL",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="AAPL",
        name="Apple Inc.",
        exchange_or_provider="XNAS",
        aliases=["Apple", "苹果公司"],
        products=["iPhone", "Mac", "Services"],
        competitors=["MSFT", "GOOGL", "SAMSUNG"],
    ),
    AssetRef(
        asset_id="equity:XSHG:600519",
        asset_class=AssetClass.EQUITY,
        market=Market.CN,
        symbol="600519",
        name="贵州茅台",
        exchange_or_provider="XSHG",
        currency="CNY",
        aliases=["茅台", "Kweichow Moutai"],
        lot_size=100,
    ),
    AssetRef(
        asset_id="equity:XHKG:00700",
        asset_class=AssetClass.EQUITY,
        market=Market.HK,
        symbol="00700",
        name="腾讯控股",
        exchange_or_provider="XHKG",
        currency="HKD",
        aliases=["腾讯", "Tencent"],
        products=["微信", "游戏", "云服务"],
        competitors=["9988", "NTES"],
        lot_size=100,
    ),
    AssetRef(
        asset_id="crypto:coingecko:bitcoin",
        asset_class=AssetClass.CRYPTO,
        market=Market.CRYPTO,
        symbol="BTC",
        name="Bitcoin",
        exchange_or_provider="coingecko",
        aliases=["bitcoin", "比特币"],
    ),
    AssetRef(
        asset_id="crypto:coingecko:ethereum",
        asset_class=AssetClass.CRYPTO,
        market=Market.CRYPTO,
        symbol="ETH",
        name="Ethereum",
        exchange_or_provider="coingecko",
        aliases=["ethereum", "以太坊"],
    ),
]


class ProviderRegistry:
    def __init__(
        self,
        settings: Settings | None = None,
        assets: Iterable[AssetRef] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.fmp = FmpProvider(self.settings)
        self.crypto = CryptoProvider(self.settings)
        self.rss = RssProvider(self.settings)
        self.akshare = AkShareProvider()
        self.sec = SecProvider(self.settings)
        self.providers = [self.fmp, self.rss, self.akshare]
        self._assets = {asset.asset_id: asset for asset in SEED_ASSETS}
        self.add_assets(assets or [])
        self.last_errors: list[str] = []
        self.mapping_errors: list[str] = []

    def _source_enabled(self, name: str, default: bool = True) -> bool:
        try:
            with SessionLocal() as db:
                value = db.scalar(select(McpSourceRow.enabled).where(McpSourceRow.name == name))
            return default if value is None else bool(value)
        except Exception:
            return default

    def add_assets(self, assets: Iterable[AssetRef]) -> None:
        self._assets.update({asset.asset_id: asset for asset in assets})

    def refresh_crypto_universe(self) -> list[AssetRef]:
        assets = self.crypto.top_assets(20)
        self._assets.update({asset.asset_id: asset for asset in assets})
        return assets

    def all_assets(self) -> list[AssetRef]:
        return list(self._assets.values())

    def get_asset(self, asset_id: str) -> AssetRef | None:
        return self._assets.get(asset_id)

    def discover_news(self, *, since: datetime, limit: int = 200) -> list[NewsItem]:
        unique: dict[str, NewsItem] = {}
        self.last_errors = []
        for provider in self.providers:
            if provider is self.fmp and not self._source_enabled("FMP", self.settings.fmp_enabled):
                continue
            try:
                for item in provider.discover_news(since=since, limit=limit):
                    unique[item.content_hash] = item
            except Exception as exc:
                self.last_errors.append(f"{provider.name}: {type(exc).__name__}")
                continue
        return sorted(unique.values(), key=lambda item: item.published_at, reverse=True)[:limit]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        lowered = query.lower()
        exact: list[AssetRef] = []
        for asset in self._assets.values():
            values = [asset.symbol, asset.name, *asset.aliases, *asset.products]
            if any(
                value.lower() in lowered or lowered in value.lower() for value in values if value
            ):
                exact.append(asset)
        if exact:
            return exact
        output: dict[str, AssetRef] = {}
        providers = [self.fmp, self.crypto]
        if not self._source_enabled("FMP", self.settings.fmp_enabled):
            providers.remove(self.fmp)
        if self.settings.akshare_asset_master_enabled:
            providers.insert(0, self.akshare)
        for provider in providers:
            try:
                for asset in provider.resolve_assets(query):
                    output[asset.asset_id] = asset
                for detail in getattr(provider, "last_errors", []):
                    error = f"{provider.name}: {detail}"
                    if error not in self.mapping_errors:
                        self.mapping_errors.append(error)
            except Exception as exc:
                error = f"{provider.name}: {type(exc).__name__}"
                if error not in self.mapping_errors:
                    self.mapping_errors.append(error)
                continue
        self._assets.update(output)
        return list(output.values())

    def provider_for(self, asset: AssetRef):
        if asset.asset_class is AssetClass.CRYPTO:
            return self.crypto
        if asset.market in {Market.CN, Market.HK}:
            return self.akshare
        return self.fmp

    def get_research_data(self, asset: AssetRef) -> dict[str, Any]:
        provider = self.provider_for(asset)
        if asset.asset_class is AssetClass.CRYPTO:
            metrics: dict[str, Any] = {}
            try:
                metrics = provider.get_crypto_metrics(asset)
            except Exception:
                pass
            if self._source_enabled("FMP", self.settings.fmp_enabled):
                try:
                    metrics["fmp_quote"] = self.fmp.get_crypto_metrics(asset)
                except Exception:
                    pass
            return {"crypto_metrics": metrics}
        fundamentals: dict[str, Any] = {}
        filings: list[dict[str, Any]] = []
        today = datetime.now().date()
        canonical_args = {
            "asset_id": asset.asset_id,
            "symbol": asset.symbol,
            "market": asset.market.value,
            "from_date": (today - timedelta(days=730)).isoformat(),
            "to": today.isoformat(),
        }
        try:
            mcp_fundamentals, errors = call_enabled_purpose_sync("fundamentals", canonical_args)
            self.last_errors.extend(f"{item['source']}: MCP fundamentals" for item in errors)
            for source, payload in mcp_fundamentals:
                if isinstance(payload, dict):
                    candidate = payload.get("data", payload)
                    if isinstance(candidate, dict):
                        for key, value in candidate.items():
                            if (
                                key in fundamentals
                                and isinstance(fundamentals[key], list)
                                and isinstance(value, list)
                            ):
                                fundamentals[key].extend(value)
                            else:
                                fundamentals.setdefault(key, value)
                        fundamentals.setdefault("mcp_sources", []).append(source)
        except Exception:
            pass
        try:
            mcp_filings, errors = call_enabled_purpose_sync("filings", canonical_args)
            self.last_errors.extend(f"{item['source']}: MCP filings" for item in errors)
            for source, payload in mcp_filings:
                candidate = (
                    payload.get("results") or payload.get("data")
                    if isinstance(payload, dict)
                    else payload
                )
                if isinstance(candidate, dict):
                    candidate = [candidate]
                if isinstance(candidate, list):
                    filings.extend(
                        {**item, "source": item.get("source") or source}
                        for item in candidate
                        if isinstance(item, dict)
                    )
        except Exception:
            pass
        try:
            mcp_quotes, errors = call_enabled_purpose_sync("quote", canonical_args)
            self.last_errors.extend(f"{item['source']}: MCP quote" for item in errors)
            if mcp_quotes:
                fundamentals["quote"] = mcp_quotes[0][1]
        except Exception:
            pass
        if provider is not self.fmp or self._source_enabled("FMP", self.settings.fmp_enabled):
            try:
                provider_fundamentals = provider.get_fundamentals(asset)
                for key, value in provider_fundamentals.items():
                    if (
                        key in fundamentals
                        and isinstance(fundamentals[key], list)
                        and isinstance(value, list)
                    ):
                        fundamentals[key].extend(value)
                    else:
                        fundamentals.setdefault(key, value)
                filings.extend(provider.get_filings(asset))
            except Exception:
                pass
        if asset.market is Market.US:
            official = self.sec.get_filings(asset)
            seen = {
                item.get("accessionNumber") or item.get("finalLink") or item.get("link")
                for item in filings
            }
            filings.extend(
                item
                for item in official
                if (item.get("accessionNumber") or item.get("finalLink")) not in seen
            )
        return {"fundamentals": fundamentals, "filings": filings}
