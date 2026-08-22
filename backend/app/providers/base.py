from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from backend.app.domain import AssetRef, NewsItem


class MarketDataProvider(Protocol):
    name: str

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]: ...

    def resolve_assets(self, query: str) -> list[AssetRef]: ...

    def get_prices(
        self, asset: AssetRef, *, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]: ...

    def get_fundamentals(self, asset: AssetRef) -> dict[str, Any]: ...

    def get_filings(self, asset: AssetRef) -> list[dict[str, Any]]: ...

    def get_crypto_metrics(self, asset: AssetRef) -> dict[str, Any]: ...


class ProviderError(RuntimeError):
    pass
