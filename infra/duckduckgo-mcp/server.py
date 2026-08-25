from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from ddgs import DDGS
from mcp.server import MCPServer

TIMEOUT_SECONDS = int(os.getenv("DUCKDUCKGO_TIMEOUT_SECONDS", "15"))
MIN_INTERVAL_SECONDS = float(os.getenv("DUCKDUCKGO_MIN_INTERVAL_SECONDS", "2"))
TIME_LIMITS = {"day": "d", "week": "w", "month": "m", "year": "y"}
LANGUAGE_REGIONS = {"zh-CN": "cn-zh", "zh": "cn-zh", "en": "us-en", "all": "wt-wt"}

mcp = MCPServer("Market Loop DuckDuckGo")
_request_lock = asyncio.Lock()
_last_request_started = 0.0


def region_for(language: str) -> str:
    return LANGUAGE_REGIONS.get(language, "wt-wt")


def time_limit_for(time_range: str) -> str | None:
    return TIME_LIMITS.get(time_range)


def normalize_results(payload: Any, limit: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("href") or item.get("url") or "").strip()
        snippet = str(item.get("body") or item.get("snippet") or "").strip()
        if not title or not snippet or not url.startswith(("http://", "https://")):
            continue
        results.append(
            {
                "title": title,
                "url": url,
                "snippet": snippet,
                "published_at": item.get("date"),
                "engine": "duckduckgo",
            }
        )
        if len(results) >= limit:
            break
    return results


def _search_sync(
    query: str, *, limit: int, language: str, time_range: str
) -> list[dict[str, Any]]:
    with DDGS(timeout=TIMEOUT_SECONDS) as client:
        payload = client.text(
            query,
            region=region_for(language),
            safesearch="off",
            timelimit=time_limit_for(time_range),
            max_results=limit,
            backend="duckduckgo",
        )
    return normalize_results(payload, limit)


async def search_duckduckgo(
    query: str,
    limit: int = 5,
    language: str = "zh-CN",
    time_range: str = "",
) -> dict[str, list[dict[str, Any]]]:
    global _last_request_started

    query = query.strip()
    if not query:
        raise ValueError("query is required")
    if len(query) > 500:
        raise ValueError("query must not exceed 500 characters")
    bounded_limit = max(1, min(limit, 20))
    async with _request_lock:
        delay = MIN_INTERVAL_SECONDS - (time.monotonic() - _last_request_started)
        if delay > 0:
            await asyncio.sleep(delay)
        _last_request_started = time.monotonic()
        results = await asyncio.to_thread(
            _search_sync,
            query,
            limit=bounded_limit,
            language=language,
            time_range=time_range,
        )
    return {"results": results}


@mcp.tool()
async def web_search(
    query: str,
    limit: int = 5,
    language: str = "zh-CN",
    time_range: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """Search DuckDuckGo directly and return original HTTP(S) result links."""

    return await search_duckduckgo(query, limit, language, time_range)


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8080)
