from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from typing import Any

import httpx

from backend.app.config import Settings, get_settings
from backend.app.domain import AssetClass, AssetRef, Market, NewsItem
from backend.app.providers.cache import cache
from backend.app.services.industry_taxonomy import normalize_industry


class CryptoProvider:
    name = "crypto-public"

    excluded_categories = {"stablecoin", "wrapped-tokens"}

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=30, headers={"User-Agent": "market-loop-agent/0.1"})

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        return []

    def top_assets(self, limit: int = 20) -> list[AssetRef]:
        key = cache.key("coingecko-top", {"limit": limit})

        def loader() -> list[dict[str, Any]]:
            response = self.client.get(
                f"{self.settings.coingecko_base_url}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": max(limit + 15, 35),
                    "page": 1,
                    "sparkline": "false",
                },
            )
            response.raise_for_status()
            return response.json()

        payload = cache.remember(key, 1800, loader)
        excluded = {"usdt", "usdc", "dai", "fdusd", "usde", "wbtc", "weth", "steth"}
        assets: list[AssetRef] = []
        for item in payload:
            symbol = item["symbol"].upper()
            if item["symbol"].lower() in excluded:
                continue
            assets.append(
                AssetRef(
                    asset_id=f"crypto:coingecko:{item['id']}",
                    asset_class=AssetClass.CRYPTO,
                    market=Market.CRYPTO,
                    symbol=symbol,
                    name=item["name"],
                    exchange_or_provider="coingecko",
                    currency="USD",
                    aliases=[item["id"]],
                    sector_id="sector:digital_assets",
                    industry_id="industry:cryptocurrency",
                    raw_sector="Digital Assets",
                    raw_industry="Cryptocurrency",
                    instrument_type="crypto",
                    market_cap=item.get("market_cap"),
                    market_cap_rank=item.get("market_cap_rank"),
                )
            )
            if len(assets) >= limit:
                break
        return assets

    def all_assets(self) -> list[AssetRef]:
        """Return CoinGecko's complete active coin identity directory."""

        key = cache.key("coingecko-all-coins", {"version": 1})

        def loader() -> list[dict[str, Any]]:
            response = self.client.get(
                f"{self.settings.coingecko_base_url}/coins/list",
                params={"include_platform": "false"},
            )
            response.raise_for_status()
            return response.json()

        payload = cache.remember(key, 24 * 60 * 60, loader)
        sector_id, industry_id = normalize_industry("Digital Assets", "Cryptocurrency")
        return [
            AssetRef(
                asset_id=f"crypto:coingecko:{item['id']}",
                asset_class=AssetClass.CRYPTO,
                market=Market.CRYPTO,
                symbol=str(item.get("symbol") or "").upper(),
                name=str(item.get("name") or item.get("id") or ""),
                exchange_or_provider="coingecko",
                aliases=[str(item["id"])],
                sector_id=sector_id,
                industry_id=industry_id,
                raw_sector="Digital Assets",
                raw_industry="Cryptocurrency",
                instrument_type="crypto",
            )
            for item in payload
            if item.get("id") and item.get("symbol") and item.get("name")
        ]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        lowered = query.lower()
        return [
            asset
            for asset in self.top_assets()
            if lowered
            in {asset.symbol.lower(), asset.name.lower(), *[a.lower() for a in asset.aliases]}
        ]

    def get_prices(
        self, asset: AssetRef, *, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        coin_id = asset.asset_id.rsplit(":", 1)[-1]
        days = 180
        if start and end:
            days = max(1, (end - start).days)
        response = self.client.get(
            f"{self.settings.coingecko_base_url}/coins/{coin_id}/market_chart",
            params={"vs_currency": "usd", "days": min(days, 365), "interval": "daily"},
        )
        response.raise_for_status()
        return [
            {"timestamp": ts, "price": price} for ts, price in response.json().get("prices", [])
        ]

    def get_fundamentals(self, asset: AssetRef) -> dict[str, Any]:
        return {}

    def get_filings(self, asset: AssetRef) -> list[dict[str, Any]]:
        return []

    def get_crypto_metrics(self, asset: AssetRef) -> dict[str, Any]:
        coin_id = asset.asset_id.rsplit(":", 1)[-1]
        key = cache.key("crypto-metrics", coin_id)

        def loader() -> dict[str, Any]:
            coin = self.client.get(
                f"{self.settings.coingecko_base_url}/coins/{coin_id}",
                params={"localization": "false", "tickers": "false", "community_data": "true"},
            )
            coin.raise_for_status()
            llama = self.client.get(f"{self.settings.defillama_base_url}/protocol/{coin_id}")
            protocol = llama.json() if llama.status_code == 200 else {}
            data = coin.json()
            market = data.get("market_data", {})
            coingecko_price = market.get("current_price", {}).get("usd")
            return {
                "market": market,
                "community": data.get("community_data", {}),
                "developer": data.get("developer_data", {}),
                "links": data.get("links", {}),
                "defillama": protocol,
                "exchange_check": self._exchange_check(asset.symbol, coingecko_price),
                "integrity": sha256(str(data.get("last_updated", "")).encode()).hexdigest(),
            }

        return cache.remember(key, 1800, loader)

    @staticmethod
    def _exchange_check(symbol: str, reference_price: float | None) -> dict[str, Any]:
        """Cross-check spot price/liquidity through a public CCXT exchange endpoint."""
        try:
            import ccxt

            exchange = ccxt.kraken({"enableRateLimit": True, "timeout": 15_000})
            ticker = exchange.fetch_ticker(f"{symbol}/USD")
            price = float(ticker.get("last") or ticker.get("close") or 0)
            divergence = None
            if reference_price and price:
                divergence = price / float(reference_price) - 1
            return {
                "exchange": "kraken",
                "pair": f"{symbol}/USD",
                "last": price,
                "quote_volume": ticker.get("quoteVolume"),
                "reference_divergence": divergence,
                "verified": bool(price) and (divergence is None or abs(divergence) <= 0.03),
            }
        except Exception:
            return {"exchange": "kraken", "verified": False, "unavailable": True}
