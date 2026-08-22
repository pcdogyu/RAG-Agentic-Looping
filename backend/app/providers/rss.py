from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from time import mktime
from typing import Any

import feedparser

from backend.app.config import Settings, get_settings
from backend.app.domain import AssetRef, NewsItem, SourceQuality


class RssProvider:
    name = "rss"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        output: list[NewsItem] = []
        feeds = [
            *((url, SourceQuality.PROFESSIONAL) for url in self.settings.rss_feeds),
            *((url, SourceQuality.OFFICIAL) for url in self.settings.official_rss_feeds),
        ]
        for url, quality in feeds:
            feed = feedparser.parse(url)
            source = feed.feed.get("title", url)
            for entry in feed.entries:
                title = entry.get("title", "")
                link = entry.get("link", "")
                parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                published = (
                    datetime.fromtimestamp(mktime(parsed), UTC) if parsed else datetime.now(UTC)
                )
                if not title or not link or published < since:
                    continue
                output.append(
                    NewsItem(
                        source=source,
                        source_quality=quality,
                        title=title,
                        summary=entry.get("summary", ""),
                        url=link,
                        published_at=published,
                        as_of=published,
                        content_hash=sha256(f"{title}|{link}".encode()).hexdigest(),
                    )
                )
        return sorted(output, key=lambda item: item.published_at, reverse=True)[:limit]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        return []

    def get_prices(self, asset: AssetRef, **kwargs: Any) -> list[dict[str, Any]]:
        return []

    def get_fundamentals(self, asset: AssetRef) -> dict[str, Any]:
        return {}

    def get_filings(self, asset: AssetRef) -> list[dict[str, Any]]:
        return []

    def get_crypto_metrics(self, asset: AssetRef) -> dict[str, Any]:
        return {}
