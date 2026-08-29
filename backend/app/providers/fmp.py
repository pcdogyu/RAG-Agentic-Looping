from __future__ import annotations

import json
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import uuid4

import httpx
from dateutil.parser import parse as parse_datetime
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AssetClass,
    AssetRef,
    Market,
    NewsItem,
    SourceQuality,
    utc_now,
)
from backend.app.providers.base import ProviderError
from backend.app.providers.cache import cache
from backend.app.services.industry_taxonomy import normalize_industry


class MinuteRateLimiter:
    def __init__(self, calls_per_minute: int) -> None:
        self.minimum_interval = 60.0 / calls_per_minute
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            delay = self.minimum_interval - (time.monotonic() - self._last_call)
            if delay > 0:
                time.sleep(delay)
            self._last_call = time.monotonic()


class FmpMcpClient:
    """Minimal Streamable HTTP MCP client restricted to explicitly named tools."""

    allowed_tools = {
        "searchSymbol",
        "getCompanyProfile",
        "getStockPeers",
        "getIncomeStatement",
        "getBalanceSheetStatement",
        "getCashFlowStatement",
        "getKeyMetrics",
        "getRatios",
        "getQuote",
        "getFullChart",
        "getGeneralNews",
        "getStockNews",
        "getCryptoNews",
        "getFilingsBySymbol",
        "getEarningsCalendar",
        "getCryptocurrencyQuote",
        "getCryptocurrencyBatchQuotes",
    }

    def __init__(self, url: str, timeout: int = 30) -> None:
        self.url = url
        self.client_id = f"market-loop-{uuid4()}"
        self.session_id: str | None = None
        self.client = httpx.Client(timeout=timeout)

    @staticmethod
    def _decode(response: httpx.Response) -> dict[str, Any]:
        response.raise_for_status()
        if not response.content:
            return {}
        if "text/event-stream" in response.headers.get("content-type", ""):
            for line in reversed(response.text.splitlines()):
                if line.startswith("data:"):
                    return json.loads(line[5:].strip())
            raise ProviderError("MCP response contained no data event")
        return response.json()

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "accept": "application/json, text/event-stream",
            "content-type": "application/json",
            "mcp-client-id": self.client_id,
        }
        if self.session_id:
            headers["mcp-session-id"] = self.session_id
        response = self.client.post(self.url, json=payload, headers=headers)
        self.session_id = response.headers.get("mcp-session-id", self.session_id)
        return self._decode(response)

    def initialize(self) -> None:
        if self.session_id:
            return
        result = self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "market-loop-agent", "version": "0.1.0"},
                },
            }
        )
        if "error" in result:
            raise ProviderError(f"MCP initialize failed: {result['error']}")
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})

    def call(self, name: str, arguments: dict[str, Any]) -> Any:
        if name not in self.allowed_tools:
            raise ProviderError(f"FMP MCP tool is not allowlisted: {name}")
        self.initialize()
        response = self._post(
            {
                "jsonrpc": "2.0",
                "id": int(time.time() * 1000),
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        if "error" in response:
            raise ProviderError(f"FMP MCP call failed: {response['error']}")
        result = response.get("result", {})
        if result.get("isError"):
            raise ProviderError(f"FMP MCP tool returned an error: {result}")
        for block in result.get("content", []):
            if block.get("type") == "text":
                text = block.get("text", "")
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return text
        return result.get("structuredContent", result)


class FmpProvider:
    name = "fmp"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=30)
        self.limiter = MinuteRateLimiter(self.settings.fmp_rate_limit_per_minute)
        self.mcp = FmpMcpClient(self.settings.fmp_mcp_url) if self.settings.fmp_mcp_url else None

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, ProviderError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        reraise=True,
    )
    def _rest(self, endpoint: str, params: dict[str, Any], ttl: int = 900) -> Any:
        if not self.settings.fmp_access_token:
            return []
        safe_params = dict(params)
        key = cache.key("fmp", {"endpoint": endpoint, "params": safe_params})

        def loader() -> Any:
            self.limiter.wait()
            response = self.client.get(
                f"{self.settings.fmp_base_url.rstrip('/')}/{endpoint.lstrip('/')}",
                params=safe_params,
                headers={"apikey": self.settings.fmp_access_token},
            )
            if response.status_code == 429:
                raise ProviderError("FMP rate limit reached")
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("Error Message"):
                raise ProviderError(payload["Error Message"])
            return payload

        return cache.remember(key, ttl, loader)

    def _mcp_or_rest(
        self,
        tool: str,
        arguments: dict[str, Any],
        endpoint: str,
        params: dict[str, Any],
        ttl: int = 900,
    ) -> Any:
        if self.mcp:
            try:
                return self.mcp.call(tool, arguments)
            except Exception:
                pass
        return self._rest(endpoint, params, ttl)

    @staticmethod
    def _time(value: str | None) -> datetime:
        if not value:
            return utc_now()
        parsed = parse_datetime(value)
        return parsed.replace(tzinfo=parsed.tzinfo or UTC)

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        fmp_boundary = utc_now() - timedelta(hours=self.settings.fmp_news_lookback_hours)
        effective_since = min(since, fmp_boundary)
        feeds = [
            ("getStockNews", "news/stock-latest", "FMP Stock News", False),
            ("getCryptoNews", "news/crypto-latest", "FMP Crypto News", True),
            ("getGeneralNews", "news/general-latest", "FMP General News", False),
        ]
        output: list[NewsItem] = []
        for tool, endpoint, source, crypto in feeds:
            payload = self._mcp_or_rest(
                tool,
                {"page": 0, "limit": min(limit, 100)},
                endpoint,
                {"page": 0, "limit": min(limit, 100)},
                ttl=300,
            )
            if isinstance(payload, dict):
                payload = payload.get("data", payload.get("results", []))
            if not isinstance(payload, list):
                continue
            for item in payload:
                published = self._time(item.get("publishedDate") or item.get("date"))
                if published < effective_since:
                    continue
                title = item.get("title") or ""
                url = item.get("url") or item.get("link") or ""
                if not title or not url:
                    continue
                digest = sha256(f"{title}|{url}".encode()).hexdigest()
                symbols = item.get("symbol") or item.get("symbols") or []
                if isinstance(symbols, str):
                    symbols = [part.strip() for part in symbols.split(",") if part.strip()]
                output.append(
                    NewsItem(
                        source=source,
                        source_quality=SourceQuality.AGGREGATOR,
                        title=title,
                        summary=item.get("text") or item.get("snippet") or "",
                        url=url,
                        language="en",
                        published_at=published,
                        as_of=published,
                        content_hash=digest,
                        symbols=symbols,
                        raw_metadata={"crypto": crypto, "site": item.get("site")},
                    )
                )
        return output[:limit]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        payload = self._mcp_or_rest(
            "searchSymbol", {"query": query}, "search-symbol", {"query": query}, ttl=86400
        )
        if isinstance(payload, dict):
            payload = payload.get("data", payload.get("results", []))
        output = []
        for item in payload if isinstance(payload, list) else []:
            symbol = str(item.get("symbol") or "").strip()
            exchange = str(item.get("exchangeShortName") or item.get("exchange") or "US").strip()
            if not symbol:
                continue
            name = str(item.get("name") or item.get("companyName") or symbol).strip()
            aliases = {
                str(value).strip()
                for value in (
                    item.get("companyName"),
                    item.get("shortName"),
                    item.get("underlyingName"),
                    self._underlying_issuer_name(name),
                )
                if value and str(value).strip() not in {name, symbol}
            }
            market = self._legacy_market_for_exchange(exchange)
            output.append(
                AssetRef(
                    asset_id=f"equity:{exchange}:{symbol}",
                    asset_class=AssetClass.EQUITY,
                    market=market,
                    symbol=symbol,
                    name=name,
                    exchange_or_provider=exchange,
                    currency=str(item.get("currency") or "USD").strip().upper(),
                    aliases=sorted(aliases),
                    issuer_id=self._explicit_issuer_id(item),
                    primary_listing_asset_id=self._explicit_primary_listing_id(
                        item, symbol=symbol, exchange=exchange
                    ),
                )
            )
        return output

    def list_equity_universe(self) -> list[AssetRef]:
        """Load active US operating companies and issuer-verified OTC ADRs."""

        output: dict[str, AssetRef] = {}
        for exchange in ("NASDAQ", "NYSE", "AMEX", "OTC"):
            payload = self._rest(
                "company-screener",
                {
                    "exchange": exchange,
                    "isEtf": "false",
                    "isFund": "false",
                    "isActivelyTrading": "true",
                    "limit": 10_000,
                },
                ttl=24 * 60 * 60,
            )
            if isinstance(payload, dict):
                payload = payload.get("data", payload.get("results", []))
            for item in payload if isinstance(payload, list) else []:
                symbol = str(item.get("symbol") or "").strip().upper()
                name = str(item.get("companyName") or item.get("name") or symbol).strip()
                if not symbol or not name:
                    continue
                lowered = name.casefold()
                is_adr = " adr" in f" {lowered}" or "depositary" in lowered
                if exchange == "OTC" and not is_adr:
                    continue
                raw_sector = str(item.get("sector") or "").strip()
                raw_industry = str(item.get("industry") or "").strip()
                sector_id, industry_id = normalize_industry(raw_sector, raw_industry)
                market_cap = item.get("marketCap")
                try:
                    market_cap = float(market_cap) if market_cap is not None else None
                except (TypeError, ValueError):
                    market_cap = None
                asset_id = f"equity:{exchange}:{symbol}"
                output[asset_id] = AssetRef(
                    asset_id=asset_id,
                    asset_class=AssetClass.EQUITY,
                    market=Market.US,
                    symbol=symbol,
                    name=name,
                    exchange_or_provider=exchange,
                    currency=str(item.get("currency") or "USD").strip().upper(),
                    aliases=[
                        value
                        for value in {
                            str(item.get("companyName") or "").strip(),
                            str(item.get("shortName") or "").strip(),
                            self._underlying_issuer_name(name),
                        }
                        if value and value not in {name, symbol}
                    ],
                    issuer_id=self._explicit_issuer_id(item),
                    primary_listing_asset_id=self._explicit_primary_listing_id(
                        item, symbol=symbol, exchange=exchange
                    ),
                    sector_id=sector_id,
                    industry_id=industry_id,
                    raw_sector=raw_sector,
                    raw_industry=raw_industry,
                    instrument_type=(
                        "shell_company"
                        if industry_id == "industry:special_purpose"
                        else "adr"
                        if is_adr
                        else "common_stock"
                    ),
                    market_cap=market_cap,
                )
        return list(output.values())

    def list_macro_assets(self) -> list[AssetRef]:
        """Load FMP continuous commodity benchmarks and spot FX master data."""

        output: dict[str, AssetRef] = {}
        for endpoint, asset_class, market in (
            ("commodities-list", AssetClass.COMMODITY, Market.COMMODITY),
            ("forex-list", AssetClass.FX, Market.FX),
        ):
            payload = self._rest(endpoint, {}, ttl=86400)
            if isinstance(payload, dict):
                payload = payload.get("data", payload.get("results", []))
            for item in payload if isinstance(payload, list) else []:
                symbol = str(item.get("symbol") or item.get("ticker") or "").strip().upper()
                if not symbol:
                    continue
                name = str(item.get("name") or item.get("companyName") or symbol).strip()
                asset_id = f"{asset_class.value}:fmp:{symbol}"
                output[asset_id] = AssetRef(
                    asset_id=asset_id,
                    asset_class=asset_class,
                    market=market,
                    symbol=symbol,
                    name=name,
                    exchange_or_provider="fmp",
                    currency=str(item.get("currency") or "USD").strip().upper(),
                    aliases=[
                        value
                        for value in {
                            str(item.get("shortName") or "").strip(),
                            str(item.get("underlyingName") or "").strip(),
                        }
                        if value and value not in {symbol, name}
                    ],
                )
        return list(output.values())

    @staticmethod
    def _explicit_issuer_id(item: dict[str, Any]) -> str | None:
        for key, namespace in (
            ("issuerId", "fmp"),
            ("issuer_id", "fmp"),
            ("cik", "sec-cik"),
        ):
            value = str(item.get(key) or "").strip()
            if value:
                return f"{namespace}:{value.casefold()}"
        return None

    @staticmethod
    def _explicit_primary_listing_id(
        item: dict[str, Any], *, symbol: str, exchange: str
    ) -> str | None:
        provided_id = str(
            item.get("primaryListingAssetId") or item.get("primary_listing_asset_id") or ""
        ).strip()
        if provided_id:
            return provided_id

        primary_symbol = str(
            item.get("underlyingSymbol") or item.get("primarySymbol") or ""
        ).strip()
        primary_exchange = str(
            item.get("underlyingExchangeShortName")
            or item.get("underlyingExchange")
            or item.get("primaryExchangeShortName")
            or item.get("primaryExchange")
            or ""
        ).strip()
        if not primary_symbol or not primary_exchange:
            return None
        if (
            primary_symbol.casefold() == symbol.casefold()
            and primary_exchange.casefold() == exchange.casefold()
        ):
            return None
        return f"equity:{primary_exchange}:{primary_symbol}"

    @staticmethod
    def _underlying_issuer_name(name: str) -> str:
        """Remove listing wrappers while retaining the issuer's legal name."""

        cleaned = re.sub(
            r"\b(?:sponsored|unsponsored)\s+(?:adr|ads)\b",
            "",
            name,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"\bamerican\s+de(?:positary|pository)\s+(?:receipt|receipts|share|shares)\b",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        return re.sub(r"\s{2,}", " ", cleaned).strip(" -(),")

    @staticmethod
    def _legacy_market_for_exchange(exchange: str) -> Market:
        """Map supported markets without changing the persisted Market enum."""

        normalized = exchange.casefold()
        if normalized in {"hkse", "hkg", "xhongkong"}:
            return Market.HK
        if normalized in {"shh", "shanghai", "shz", "shenzhen"}:
            return Market.CN
        # Existing rows model all other FMP listings as US.  Preserve that
        # representation; exchange_or_provider still distinguishes OTC/ASX.
        return Market.US

    def get_prices(
        self, asset: AssetRef, *, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": asset.symbol}
        if start:
            params["from"] = start.date().isoformat()
        if end:
            params["to"] = end.date().isoformat()
        payload = self._mcp_or_rest("getFullChart", params, "historical-price-eod/full", params)
        if isinstance(payload, dict):
            return payload.get("historical", payload.get("data", []))
        return payload if isinstance(payload, list) else []

    def get_quote(self, asset: AssetRef) -> dict[str, Any]:
        payload = self._mcp_or_rest(
            "getQuote", {"symbol": asset.symbol}, "quote", {"symbol": asset.symbol}, ttl=60
        )
        if isinstance(payload, list):
            return payload[0] if payload else {}
        return payload if isinstance(payload, dict) else {}

    def get_fundamentals(self, asset: AssetRef) -> dict[str, Any]:
        if asset.asset_class in {AssetClass.CRYPTO, AssetClass.COMMODITY, AssetClass.FX}:
            return {}
        symbol = asset.symbol
        calls = {
            "profile": ("getCompanyProfile", "profile"),
            "income": ("getIncomeStatement", "income-statement"),
            "balance": ("getBalanceSheetStatement", "balance-sheet-statement"),
            "cashflow": ("getCashFlowStatement", "cash-flow-statement"),
            "ratios": ("getRatios", "ratios"),
        }
        result: dict[str, Any] = {}
        for key, (tool, endpoint) in calls.items():
            result[key] = self._mcp_or_rest(
                tool,
                {"symbol": symbol, "period": "annual", "limit": 5},
                endpoint,
                {"symbol": symbol, "period": "annual", "limit": 5},
                ttl=21600,
            )
        return result

    def get_filings(self, asset: AssetRef) -> list[dict[str, Any]]:
        payload = self._mcp_or_rest(
            "getFilingsBySymbol",
            {"symbol": asset.symbol},
            "sec-filings-search/symbol",
            {"symbol": asset.symbol, "from": "2020-01-01", "to": utc_now().date().isoformat()},
            ttl=21600,
        )
        if isinstance(payload, dict):
            payload = payload.get("data", [])
        return payload if isinstance(payload, list) else []

    def get_crypto_metrics(self, asset: AssetRef) -> dict[str, Any]:
        symbol = asset.symbol if asset.symbol.endswith("USD") else f"{asset.symbol}USD"
        payload = self._mcp_or_rest(
            "getCryptocurrencyQuote",
            {"symbol": symbol},
            "quote",
            {"symbol": symbol},
            ttl=60,
        )
        return {"quote": payload}
