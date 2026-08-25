from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest

SERVER_PATH = Path(__file__).parents[2] / "infra" / "duckduckgo-mcp" / "server.py"
SPEC = importlib.util.spec_from_file_location("duckduckgo_mcp_server", SERVER_PATH)
assert SPEC and SPEC.loader
SERVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SERVER)


def test_duckduckgo_parameter_mappings():
    assert SERVER.region_for("zh-CN") == "cn-zh"
    assert SERVER.region_for("en") == "us-en"
    assert SERVER.region_for("unknown") == "wt-wt"
    assert SERVER.time_limit_for("day") == "d"
    assert SERVER.time_limit_for("week") == "w"
    assert SERVER.time_limit_for("month") == "m"
    assert SERVER.time_limit_for("year") == "y"
    assert SERVER.time_limit_for("") is None


def test_duckduckgo_result_normalization_rejects_invalid_urls():
    items = SERVER.normalize_results(
        [
            {"title": "Result", "href": "https://example.com/a", "body": "Summary"},
            {"title": "Unsafe", "href": "javascript:alert(1)", "body": "Summary"},
            {"title": "No summary", "href": "https://example.com/empty"},
            {"title": "Second", "url": "http://example.org/b", "snippet": "Second"},
        ],
        1,
    )
    assert items == [
        {
            "title": "Result",
            "url": "https://example.com/a",
            "snippet": "Summary",
            "published_at": None,
            "engine": "duckduckgo",
        }
    ]


def test_duckduckgo_search_bounds_and_forwards_parameters(monkeypatch):
    captured = {}

    def fake_search(query, *, limit, language, time_range):
        captured.update(
            query=query,
            limit=limit,
            language=language,
            time_range=time_range,
        )
        return []

    monkeypatch.setattr(SERVER, "_search_sync", fake_search)
    monkeypatch.setattr(SERVER, "MIN_INTERVAL_SECONDS", 0)
    SERVER._last_request_started = 0
    result = asyncio.run(SERVER.search_duckduckgo("  市场新闻  ", 99, "zh-CN", "week"))
    assert result == {"results": []}
    assert captured == {
        "query": "市场新闻",
        "limit": 20,
        "language": "zh-CN",
        "time_range": "week",
    }


def test_duckduckgo_search_does_not_retry_errors(monkeypatch):
    calls = 0

    def failing_search(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise TimeoutError("upstream timeout")

    monkeypatch.setattr(SERVER, "_search_sync", failing_search)
    monkeypatch.setattr(SERVER, "MIN_INTERVAL_SECONDS", 0)
    SERVER._last_request_started = 0
    with pytest.raises(TimeoutError, match="upstream timeout"):
        asyncio.run(SERVER.search_duckduckgo("market news"))
    assert calls == 1


@pytest.mark.parametrize("query", ["", " ", "x" * 501])
def test_duckduckgo_search_rejects_invalid_queries(query):
    with pytest.raises(ValueError):
        asyncio.run(SERVER.search_duckduckgo(query))
