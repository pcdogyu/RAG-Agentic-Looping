from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
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
                if published < since:
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
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeShortName") or item.get("exchange") or "US"
            if not symbol:
                continue
            output.append(
                AssetRef(
                    asset_id=f"equity:{exchange}:{symbol}",
                    asset_class=AssetClass.EQUITY,
                    market=Market.US,
                    symbol=symbol,
                    name=item.get("name") or symbol,
                    exchange_or_provider=exchange,
                    currency="USD",
                )
            )
        return output

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
        if asset.asset_class is AssetClass.CRYPTO:
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
