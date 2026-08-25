from __future__ import annotations

import os
from typing import Any

import httpx
from mcp.server import MCPServer

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://searxng:8080").rstrip("/")
TIME_RANGES = {"day", "week", "month", "year"}

mcp = MCPServer("Market Loop Search")


@mcp.tool()
async def web_search(
    query: str,
    limit: int = 5,
    language: str = "zh-CN",
    time_range: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """Search the public web and return results with original HTTP(S) links."""

    params: dict[str, str | int] = {
        "q": query,
        "format": "json",
        "language": language,
        "safesearch": 0,
    }
    if time_range in TIME_RANGES:
        params["time_range"] = time_range
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        response = await client.get(f"{SEARXNG_URL}/search", params=params)
        response.raise_for_status()
        payload = response.json()
    results: list[dict[str, Any]] = []
    for item in payload.get("results", []):
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        snippet = str(item.get("content") or "").strip()
        if not title or not url.startswith(("http://", "https://")) or not snippet:
            continue
        results.append(
            {
                "title": title,
                "url": url,
                "snippet": snippet,
                "published_at": item.get("publishedDate"),
                "engine": item.get("engine"),
            }
        )
        if len(results) >= max(1, min(limit, 20)):
            break
    return {"results": results}


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8080)
