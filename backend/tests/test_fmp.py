from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from backend.app.config import Settings
from backend.app.providers.base import ProviderError
from backend.app.providers.fmp import FmpMcpClient, FmpProvider


def test_mcp_empty_notification_response_is_valid():
    response = httpx.Response(
        202, content=b"", request=httpx.Request("POST", "https://example.invalid/mcp")
    )
    assert FmpMcpClient._decode(response) == {}


def test_mcp_rejects_non_allowlisted_tool():
    with pytest.raises(ProviderError, match="not allowlisted"):
        FmpMcpClient("https://example.invalid/mcp").call("arbitraryTool", {})


def test_rest_key_is_sent_in_header_not_url():
    credential = "-".join(("unit", "test", "credential"))
    settings = Settings(
        fmp_access_token=credential,
        fmp_mcp_url="",
        fmp_base_url="https://example.invalid/stable",
    )
    provider = FmpProvider(settings)
    with respx.mock:
        route = respx.get("https://example.invalid/stable/profile").mock(
            return_value=httpx.Response(200, json=[])
        )
        provider._rest("profile", {"symbol": "AAPL"})

    request = route.calls.last.request
    assert request.headers["apikey"] == credential
    assert credential not in str(request.url)


def test_news_discovery_uses_fmp_specific_lookback(monkeypatch):
    now = datetime(2026, 8, 22, 12, tzinfo=UTC)
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        fmp_news_lookback_hours=12,
    )
    provider = FmpProvider(settings)

    def fake_news(tool, arguments, endpoint, params, ttl=900):
        if tool != "getStockNews":
            return []
        return [
            {
                "title": "Inside the FMP lookback",
                "url": "https://example.invalid/recent",
                "publishedDate": (now - timedelta(hours=11)).isoformat(),
            },
            {
                "title": "Outside the FMP lookback",
                "url": "https://example.invalid/stale",
                "publishedDate": (now - timedelta(hours=13)).isoformat(),
            },
        ]

    monkeypatch.setattr("backend.app.providers.fmp.utc_now", lambda: now)
    monkeypatch.setattr(provider, "_mcp_or_rest", fake_news)

    items = provider.discover_news(since=now - timedelta(minutes=20), limit=100)

    assert [item.title for item in items] == ["Inside the FMP lookback"]
