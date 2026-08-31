from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from typing import Any

import httpx

from backend.app.config import Settings, get_settings
from backend.app.domain import AssetClass, AssetRef, AssociationTier, Market, NewsItem
from backend.app.providers.cache import cache
from backend.app.services.industry_taxonomy import normalize_industry


class CryptoProvider:
    name = "crypto-public"

    excluded_categories = {"stablecoin", "wrapped-tokens"}
    manual_only_ids = {
        "tether",
        "usd-coin",
        "dai",
        "first-digital-usd",
        "ethena-usde",
        "true-usd",
        "usdd",
        "pax-dollar",
        "paypal-usd",
        "frax",
        "liquity-usd",
        "gemini-dollar",
        "wrapped-bitcoin",
        "weth",
        "staked-ether",
    }
    manual_only_symbols = {
        "USDT",
        "USDC",
        "DAI",
        "FDUSD",
        "USDE",
        "TUSD",
        "USDD",
        "USDP",
        "PYUSD",
        "FRAX",
        "LUSD",
        "GUSD",
        "USDS",
        "USD0",
        "USD1",
        "USDA",
        "USDB",
        "USDN",
        "USDX",
        "USDF",
        "GHO",
        "EURC",
        "EURT",
        "WBTC",
        "WETH",
        "STETH",
    }

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=30, headers={"User-Agent": "market-loop-agent/0.1"})

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        return []

    def _ranked_market_assets(self, limit: int = 500) -> list[dict[str, Any]]:
        key = cache.key("coingecko-ranked", {"limit": limit, "version": 2})

        def loader() -> list[dict[str, Any]]:
            output: list[dict[str, Any]] = []
            for page in range(1, (max(1, limit) + 249) // 250 + 1):
                response = self.client.get(
                    f"{self.settings.coingecko_base_url}/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": min(250, max(1, limit - len(output))),
                        "page": page,
                        "sparkline": "false",
                    },
                )
                response.raise_for_status()
                rows = response.json()
                if not isinstance(rows, list):
                    raise RuntimeError("CoinGecko ranked market response is invalid")
                output.extend(row for row in rows if isinstance(row, dict))
                if len(output) >= limit or len(rows) < 250:
                    break
            return output[:limit]

        return cache.remember(key, 1800, loader)

    @classmethod
    def _manual_only(cls, coin_id: str, symbol: str, name: str) -> bool:
        lowered = f"{coin_id} {name}".casefold()
        stable_markers = (
            " stablecoin",
            " stable coin",
            " dollar",
            " usd",
            " euro coin",
            " eur stable",
        )
        return (
            coin_id.casefold() in cls.manual_only_ids
            or symbol.upper() in cls.manual_only_symbols
            or "wrapped " in lowered
            or "bridged " in lowered
            or any(marker in lowered for marker in stable_markers)
        )

    def top_assets(self, limit: int = 20) -> list[AssetRef]:
        payload = self._ranked_market_assets(max(limit + 15, 35))

        assets: list[AssetRef] = []
        for item in payload:
            symbol = item["symbol"].upper()
            if self._manual_only(str(item["id"]), symbol, str(item["name"])):
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
                    association_tier=AssociationTier.STANDARD,
                    association_reason="coingecko_market_cap_top_500",
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
        valid_items = [
            item
            for item in payload
            if item.get("id") and item.get("symbol") and item.get("name")
        ]
        symbol_counts: dict[str, int] = {}
        name_counts: dict[str, int] = {}
        for item in valid_items:
            symbol = str(item["symbol"]).upper()
            name = str(item["name"]).strip().casefold()
            symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
            name_counts[name] = name_counts.get(name, 0) + 1
        ranked = {
            str(item.get("id")): item
            for item in self._ranked_market_assets(500)
            if item.get("id")
        }
        sector_id, industry_id = normalize_industry("Digital Assets", "Cryptocurrency")
        assets: list[AssetRef] = []
        for item in valid_items:
            coin_id = str(item["id"])
            symbol = str(item["symbol"]).upper()
            name = str(item["name"])
            market = ranked.get(coin_id, {})
            ambiguous = not market and (
                symbol_counts.get(symbol, 0) > 1
                or name_counts.get(name.strip().casefold(), 0) > 1
            )
            manual_only = self._manual_only(coin_id, symbol, name) or ambiguous
            tier = (
                AssociationTier.MANUAL_ONLY
                if manual_only
                else AssociationTier.STANDARD
                if market
                else AssociationTier.EXACT_ONLY
            )
            assets.append(AssetRef(
                asset_id=f"crypto:coingecko:{item['id']}",
                asset_class=AssetClass.CRYPTO,
                market=Market.CRYPTO,
                symbol=symbol,
                name=name,
                exchange_or_provider="coingecko",
                aliases=[str(item["id"])],
                sector_id=sector_id,
                industry_id=industry_id,
                raw_sector="Digital Assets",
                raw_industry="Cryptocurrency",
                instrument_type="crypto",
                market_cap=market.get("market_cap"),
                market_cap_rank=market.get("market_cap_rank"),
                association_tier=tier,
                association_reason=(
                    "stable_or_wrapped_manual_only"
                    if self._manual_only(coin_id, symbol, name)
                    else "ambiguous_crypto_identity_manual_only"
                    if ambiguous
                    else "coingecko_market_cap_top_500"
                    if market
                    else "coingecko_long_tail_exact_identity"
                ),
            ))
        return assets

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
