from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from backend.app.config import Settings
from backend.app.db import IntegrationSettingRow, McpSourceRow
from backend.app.domain import (
    AnalysisStep,
    AssetClass,
    AssetRef,
    ConfidenceFactors,
    HorizonUnit,
    ImpactFactors,
    Market,
    Recommendation,
    ResearchRun,
    ScoringFactor,
    SignalStatus,
    Thesis,
    utc_now,
)
from backend.app.main import app
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.fact_sources import (
    get_effective_settings,
    save_native_group_config,
)
from backend.app.services.mcp_registry import (
    JIN10_MCP_URL,
    JIN10_TOOL_MAPPINGS,
    SearchRequest,
    fetch_enabled_news_feeds,
    flash_headline,
    normalize_news_feed_page,
    normalize_search_results,
    require_admin_token,
    search_enabled_sources,
    seed_integrations,
    validate_mappings,
)
from backend.app.services.secret_store import decrypt_secret, encrypt_secret
from backend.app.storage import save_recommendation, save_run

ADMIN = {"X-Admin-Token": "test-admin-token"}


def test_mcp_registry_is_open_and_secret_round_trip_is_safe():
    require_admin_token("test-admin-token")
    ciphertext = encrypt_secret("very-secret")
    assert "very-secret" not in ciphertext
    assert decrypt_secret(ciphertext) == "very-secret"

    with TestClient(app) as client:
        missing = client.get("/api/v1/admin/mcp-sources")
        wrong = client.get("/api/v1/admin/mcp-sources", headers={"X-Admin-Token": "wrong"})
    assert missing.status_code == 200
    assert wrong.status_code == 200


def test_mcp_crud_masks_preserves_and_clears_credentials(db):
    payload = {
        "name": "Private Search",
        "url": "https://mcp.example.test/mcp",
        "description": "test",
        "priority": 80,
        "enabled": True,
        "auth_type": "bearer",
        "secret": "source-token",
        "tool_mappings": {},
        "group_id": "search",
    }
    with TestClient(app) as client:
        created = client.post("/api/v1/admin/mcp-sources", json=payload)
        assert created.status_code == 201
        body = created.json()
        assert body["secret_configured"] is True
        assert body["group_id"] == "search"
        assert "secret" not in body and "encrypted_secret" not in body

        source_id = body["id"]
        payload["description"] = "updated"
        payload["secret"] = None
        updated = client.put(f"/api/v1/admin/mcp-sources/{source_id}", json=payload)
        assert updated.status_code == 200
        assert updated.json()["secret_configured"] is True

        payload["clear_secret"] = True
        cleared = client.put(f"/api/v1/admin/mcp-sources/{source_id}", json=payload)
        assert cleared.json()["secret_configured"] is False

        disabled = client.patch(
            f"/api/v1/admin/mcp-sources/{source_id}/enabled",
            json={"enabled": False},
        )
        assert disabled.json()["enabled"] is False
        assert db.get(McpSourceRow, source_id).enabled is False

        deleted = client.delete(f"/api/v1/admin/mcp-sources/{source_id}")
        assert deleted.status_code == 200


def test_managed_sources_seed_and_cannot_be_deleted():
    with TestClient(app) as client:
        items = client.get("/api/v1/admin/mcp-sources").json()
        names = {item["name"] for item in items}
        assert {"SearXNG", "DuckDuckGo", "FMP", "金十数据"} <= names
        searxng = next(item for item in items if item["name"] == "SearXNG")
        duckduckgo = next(item for item in items if item["name"] == "DuckDuckGo")
        fmp = next(item for item in items if item["name"] == "FMP")
        jin10 = next(item for item in items if item["name"] == "金十数据")
        assert "web_search" in searxng["tool_mappings"]
        assert searxng["group_id"] == "search"
        assert duckduckgo["url"] == "http://duckduckgo-mcp:8080/mcp"
        assert duckduckgo["priority"] == 40
        assert duckduckgo["enabled"] is True
        assert duckduckgo["managed"] is True
        assert duckduckgo["group_id"] == "search"
        assert fmp["url"] == "http://fmp-mcp:8080/mcp"
        assert fmp["group_id"] == "fmp"
        assert set(fmp["tool_mappings"]) == {"quote", "fundamentals", "filings"}
        assert jin10["url"] == JIN10_MCP_URL
        assert jin10["priority"] == 80
        assert jin10["enabled"] is False
        assert jin10["managed"] is True
        assert jin10["auth_type"] == "bearer"
        assert jin10["secret_configured"] is False
        assert jin10["group_id"] == "cn_news"
        assert jin10["tool_mappings"] == JIN10_TOOL_MAPPINGS
        changed_group = client.put(
            f"/api/v1/admin/mcp-sources/{fmp['id']}",
            json={
                "name": fmp["name"],
                "url": fmp["url"],
                "description": fmp["description"],
                "priority": fmp["priority"],
                "enabled": fmp["enabled"],
                "auth_type": fmp["auth_type"],
                "tool_mappings": fmp["tool_mappings"],
                "group_id": "search",
            },
        )
        response = client.delete(f"/api/v1/admin/mcp-sources/{searxng['id']}")
    assert changed_group.status_code == 409
    assert response.status_code == 409


def test_fact_source_groups_nest_managed_sources_and_hide_other_when_empty():
    with TestClient(app) as client:
        groups = client.get("/api/v1/admin/fact-source-groups")
    assert groups.status_code == 200
    payload = groups.json()
    assert [item["id"] for item in payload] == ["fmp", "sec", "cn_news", "crypto", "search"]
    fmp = next(item for item in payload if item["id"] == "fmp")
    cn_news = next(item for item in payload if item["id"] == "cn_news")
    search = next(item for item in payload if item["id"] == "search")
    assert [item["name"] for item in fmp["mcp_sources"]] == ["FMP"]
    assert [item["name"] for item in cn_news["mcp_sources"]] == ["金十数据"]
    assert [item["name"] for item in search["mcp_sources"]] == ["SearXNG", "DuckDuckGo"]
    assert "access_token" not in fmp["config"]


def test_jin10_seed_is_idempotent_and_preserves_existing_configuration(db):
    seed_integrations(db)
    source = db.scalar(select(McpSourceRow).where(McpSourceRow.name == "金十数据"))
    source.url = "https://custom.example.test/mcp"
    source.description = "customized"
    source.priority = 321
    source.enabled = True
    source.encrypted_secret = encrypt_secret("preserved-secret")
    source.tool_mappings = {"news_search": JIN10_TOOL_MAPPINGS["news_search"]}
    db.commit()

    seed_integrations(db)
    db.refresh(source)

    assert source.url == "https://custom.example.test/mcp"
    assert source.description == "customized"
    assert source.priority == 321
    assert source.enabled is True
    assert decrypt_secret(source.encrypted_secret) == "preserved-secret"
    assert source.tool_mappings == {"news_search": JIN10_TOOL_MAPPINGS["news_search"]}


def test_custom_mcp_group_is_persisted_and_other_is_a_safe_fallback(db):
    payload = {
        "name": "CN verifier",
        "url": "https://mcp.example.test/mcp",
        "description": "test",
        "priority": 70,
        "enabled": True,
        "auth_type": "none",
        "tool_mappings": {},
        "group_id": "cn_news",
    }
    with TestClient(app) as client:
        created = client.post("/api/v1/admin/mcp-sources", json=payload)
        assert created.json()["group_id"] == "cn_news"
        source_id = created.json()["id"]
        membership = db.get(IntegrationSettingRow, f"mcp-source-group:{source_id}")
        assert membership.payload == {"group_id": "cn_news"}
        db.delete(membership)
        db.commit()
        listed = client.get("/api/v1/admin/mcp-sources").json()
    assert next(item for item in listed if item["id"] == source_id)["group_id"] == "other"


def test_fmp_config_encrypts_masks_preserves_clears_and_resets(db):
    payload = {
        "base_url": "https://fmp.example.test/stable",
        "access_token": "rest-secret",
        "clear_access_token": False,
        "rate_limit_per_minute": 120,
        "news_lookback_hours": 24,
    }
    with TestClient(app) as client:
        saved = client.put("/api/v1/admin/fact-source-groups/fmp", json=payload)
        assert saved.status_code == 200
        assert "rest-secret" not in saved.text
        assert saved.json()["config"]["access_token_configured"] is True
        assert saved.json()["config"]["access_token_source"] == "database"

        db.expire_all()
        stored = db.get(IntegrationSettingRow, "fact-source:fmp").payload
        assert "rest-secret" not in str(stored)
        assert get_effective_settings(db=db).fmp_access_token == "rest-secret"

        payload["access_token"] = None
        preserved = client.put("/api/v1/admin/fact-source-groups/fmp", json=payload)
        assert preserved.json()["config"]["access_token_configured"] is True

        payload["clear_access_token"] = True
        cleared = client.put("/api/v1/admin/fact-source-groups/fmp", json=payload)
        assert cleared.json()["config"]["access_token_configured"] is False
        db.expire_all()
        assert get_effective_settings(db=db).fmp_access_token == ""

        reset = client.delete("/api/v1/admin/fact-source-groups/fmp")
        assert reset.status_code == 200
        assert reset.json()["config_source"] == "environment"


def test_missing_encryption_key_rejects_only_secret_updates(db):
    payload = {
        "base_url": "https://fmp.example.test/stable",
        "access_token": "secret",
        "clear_access_token": False,
        "rate_limit_per_minute": 120,
        "news_lookback_hours": 24,
    }
    with pytest.raises(RuntimeError, match="MCP_SECRET_KEY"):
        save_native_group_config(db, "fmp", payload, Settings(mcp_secret_key=""))


def test_provider_registry_reads_database_config_for_the_next_task(db):
    before = ProviderRegistry(Settings(coingecko_base_url="https://old.example.test"))
    save_native_group_config(
        db,
        "crypto",
        {
            "coingecko_base_url": "https://new.example.test/api/v3",
            "defillama_base_url": "https://llama.example.test",
        },
    )
    after = ProviderRegistry()
    assert before.settings.coingecko_base_url == "https://old.example.test"
    assert after.settings.coingecko_base_url == "https://new.example.test/api/v3"


def test_fact_source_config_validation_reset_and_probe(monkeypatch):
    async def healthy_probe(_group_id, _settings):
        return {"ok": True, "status": "healthy"}

    monkeypatch.setattr("backend.app.api_integrations.probe_native_group", healthy_probe)
    with TestClient(app) as client:
        invalid_group = client.put("/api/v1/admin/fact-source-groups/unknown", json={})
        invalid_value = client.put(
            "/api/v1/admin/fact-source-groups/search", json={"timeout_seconds": 1}
        )
        invalid_field = client.put(
            "/api/v1/admin/fact-source-groups/search",
            json={"timeout_seconds": 20, "unexpected": True},
        )
        saved = client.put(
            "/api/v1/admin/fact-source-groups/sec", json={"identity": "Research research@example.com"}
        )
        tested = client.post("/api/v1/admin/fact-source-groups/sec/test")
        reset = client.delete("/api/v1/admin/fact-source-groups/sec")
    assert invalid_group.status_code == 422
    assert invalid_value.status_code == 422
    assert invalid_field.status_code == 422
    assert saved.json()["config"]["identity"] == "Research research@example.com"
    assert tested.json()["ok"] is True
    assert reset.json()["config_source"] == "environment"


def test_probe_unwraps_exception_group(monkeypatch, db):
    async def failing_discover(_row):
        raise ExceptionGroup("task group", [ConnectionError("service unavailable")])

    monkeypatch.setattr("backend.app.api_integrations.discover_source", failing_discover)
    with TestClient(app) as client:
        items = client.get("/api/v1/admin/mcp-sources").json()
        source = next(item for item in items if item["name"] == "FMP")
        tested = client.post(f"/api/v1/admin/mcp-sources/{source['id']}/test")
    assert tested.status_code == 200
    assert tested.json()["source"]["last_error"] == "ConnectionError: service unavailable"


def test_discover_records_tools_and_validates_mapping(monkeypatch):
    async def fake_discover(_row):
        return [
            {
                "name": "web_search",
                "description": "search",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "limit": {"type": "integer"},
                        "language": {"type": "string"},
                        "time_range": {"type": "string"},
                    },
                },
                "output_schema": {"type": "object"},
            }
        ]

    monkeypatch.setattr("backend.app.api_integrations.discover_source", fake_discover)
    with TestClient(app) as client:
        items = client.get("/api/v1/admin/mcp-sources").json()
        source = next(item for item in items if item["name"] == "SearXNG")
        discovered = client.post(f"/api/v1/admin/mcp-sources/{source['id']}/discover")
    assert discovered.status_code == 200
    assert discovered.json()["source"]["last_status"] == "discovered"
    assert discovered.json()["tools"][0]["name"] == "web_search"


def test_jin10_requires_credentials_discovery_and_test_before_enable(monkeypatch):
    calls: list[tuple[str, str]] = []

    async def fake_discover(_row):
        return [
            {
                "name": "list_flash",
                "input_schema": {
                    "type": "object",
                    "properties": {"cursor": {"type": "string"}},
                },
            },
            {
                "name": "search_flash",
                "input_schema": {
                    "type": "object",
                    "properties": {"keyword": {"type": "string"}},
                },
            },
        ]

    async def fake_call(row, purpose, _arguments):
        calls.append((row.name, purpose))
        return {
            "data": {
                "items": [
                    {
                        "content": "ETF市场快讯。",
                        "url": "https://jin10.example/flash/etf",
                    }
                ]
            }
        }

    monkeypatch.setattr("backend.app.api_integrations.discover_source", fake_discover)
    monkeypatch.setattr("backend.app.api_integrations.call_source_tool", fake_call)
    with TestClient(app) as client:
        source = next(
            item
            for item in client.get("/api/v1/admin/mcp-sources").json()
            if item["name"] == "金十数据"
        )
        source_id = source["id"]
        missing_secret = client.patch(
            f"/api/v1/admin/mcp-sources/{source_id}/enabled",
            json={"enabled": True},
        )
        source["secret"] = "rotated-token-for-test"
        source["clear_secret"] = False
        configured = client.put(f"/api/v1/admin/mcp-sources/{source_id}", json=source)
        discovered = client.post(f"/api/v1/admin/mcp-sources/{source_id}/discover")
        missing_test = client.patch(
            f"/api/v1/admin/mcp-sources/{source_id}/enabled",
            json={"enabled": True},
        )
        tested = client.post(f"/api/v1/admin/mcp-sources/{source_id}/test")
        enabled = client.patch(
            f"/api/v1/admin/mcp-sources/{source_id}/enabled",
            json={"enabled": True},
        )

    assert missing_secret.status_code == 409
    assert missing_secret.json()["detail"] == "请先配置 MCP 凭据"
    assert configured.status_code == 200
    assert configured.json()["enabled"] is False
    assert configured.json()["secret_configured"] is True
    assert "rotated-token-for-test" not in configured.text
    assert discovered.json()["source"]["last_status"] == "discovered"
    assert missing_test.status_code == 409
    assert missing_test.json()["detail"] == "请先完成 MCP 连接测试"
    assert tested.json()["source"]["last_status"] == "healthy"
    assert calls == [("金十数据", "news_search")]
    assert enabled.status_code == 200
    assert enabled.json()["enabled"] is True


def test_jin10_discovery_fails_closed_when_required_tool_is_missing(monkeypatch, db):
    seed_integrations(db)
    source = db.scalar(select(McpSourceRow).where(McpSourceRow.name == "金十数据"))
    source.encrypted_secret = encrypt_secret("rotated-token-for-test")
    db.commit()

    async def incomplete_discover(_row):
        return [
            {
                "name": "search_flash",
                "input_schema": {
                    "type": "object",
                    "properties": {"keyword": {"type": "string"}},
                },
            }
        ]

    monkeypatch.setattr("backend.app.api_integrations.discover_source", incomplete_discover)
    with TestClient(app) as client:
        discovered = client.post(f"/api/v1/admin/mcp-sources/{source.id}/discover")
        enabled = client.patch(
            f"/api/v1/admin/mcp-sources/{source.id}/enabled",
            json={"enabled": True},
        )

    assert discovered.status_code == 200
    assert discovered.json()["source"]["last_status"] == "failed"
    assert discovered.json()["source"]["discovered_tools"] == []
    assert "mapped tool was not discovered: list_flash" in discovered.json()["source"]["last_error"]
    assert enabled.status_code == 409
    assert enabled.json()["detail"] == "请先完成 MCP 工具发现"


def test_search_result_normalization_dedicated_shape():
    items = normalize_search_results(
        {
            "results": [
                {
                    "title": "Verified result",
                    "url": "https://Example.com/story?utm_source=x&id=2",
                    "content": "Independent summary",
                    "publishedDate": "2026-08-24T10:00:00Z",
                },
                {"title": "missing summary", "url": "https://example.com/empty"},
                {"title": "unsafe", "url": "javascript:alert(1)", "content": "bad"},
            ]
        },
        "SearXNG",
    )
    assert len(items) == 1
    assert items[0].url == "https://example.com/story?id=2"
    assert items[0].domain == "example.com"
    assert items[0].sources == ["SearXNG"]


def test_jin10_flash_adapter_builds_title_and_parses_nested_results():
    published = utc_now().replace(microsecond=0)
    payload = {
        "data": {
            "items": [
                {
                    "id": "flash-1",
                    "title": "",
                    "content": "沪深两市成交额突破一万亿元。市场交易保持活跃。",
                    "time": published.isoformat(),
                    "url": "https://www.jin10.com/example/?utm_source=test&id=1",
                },
                {
                    "content": "unsafe result",
                    "time": published.isoformat(),
                    "url": "javascript:alert(1)",
                },
            ],
            "next_cursor": "page-2",
            "has_more": True,
        }
    }

    search_items = normalize_search_results(payload, "金十", "jin10_flash_v1")
    news_items, cursor, has_more, reached_since = normalize_news_feed_page(
        payload,
        "金十",
        "jin10_flash_v1",
        published - timedelta(minutes=1),
    )

    assert flash_headline("第一句。第二句。") == "第一句。"
    assert len(flash_headline("A" * 200)) == 120
    assert len(search_items) == 1
    assert search_items[0].title == "沪深两市成交额突破一万亿元。"
    assert search_items[0].url == "https://www.jin10.com/example?id=1"
    assert search_items[0].published_at == published
    assert len(news_items) == 1
    assert news_items[0].source == "金十"
    assert news_items[0].source_quality == "professional"
    assert news_items[0].language == "zh"
    assert cursor == "page-2"
    assert has_more is True
    assert reached_since is False


def test_jin10_feed_paginates_deduplicates_and_stops_at_time_boundary(db, monkeypatch):
    source = McpSourceRow(
        name="金十",
        url="https://mcp.jin10.com/mcp",
        description="财经快讯",
        priority=80,
        enabled=True,
        tool_mappings={
            "news_feed": {
                "tool_name": "list_flash",
                "input_bindings": {"cursor": "cursor"},
                "defaults": {},
                "output_adapter": "jin10_flash_v1",
            }
        },
    )
    db.add(source)
    db.commit()
    now = utc_now().replace(microsecond=0)
    calls: list[str] = []

    async def fake_call(_row, purpose, arguments):
        assert purpose == "news_feed"
        cursor = str(arguments.get("cursor") or "")
        calls.append(cursor)
        if not cursor:
            return {
                "data": {
                    "items": [
                        {
                            "id": "1",
                            "content": "央行发布最新政策。",
                            "time": now.isoformat(),
                            "url": "https://jin10.example/flash/1?utm_source=a",
                        }
                    ],
                    "next_cursor": "page-2",
                    "has_more": True,
                }
            }
        return {
            "data": {
                "items": [
                    {
                        "id": "duplicate",
                        "content": "央行发布最新政策。",
                        "time": now.isoformat(),
                        "url": "https://jin10.example/flash/duplicate",
                    },
                    {
                        "id": "old",
                        "content": "边界之外的旧快讯。",
                        "time": (now - timedelta(hours=2)).isoformat(),
                        "url": "https://jin10.example/flash/old",
                    },
                ],
                "next_cursor": "page-3",
                "has_more": True,
            }
        }

    monkeypatch.setattr("backend.app.services.mcp_registry.call_source_tool", fake_call)
    items, errors = asyncio.run(fetch_enabled_news_feeds(now - timedelta(hours=1), 40))

    assert errors == []
    assert calls == ["", "page-2"]
    assert [item.title for item in items] == ["央行发布最新政策。"]


def test_news_feed_caps_pages_and_isolates_source_failures(db, monkeypatch):
    mapping = {
        "news_feed": {
            "tool_name": "list_flash",
            "input_bindings": {"cursor": "cursor"},
            "defaults": {},
            "output_adapter": "jin10_flash_v1",
        }
    }
    db.add_all(
        [
            McpSourceRow(
                name="金十",
                url="https://mcp.jin10.com/mcp",
                priority=80,
                enabled=True,
                tool_mappings=mapping,
            ),
            McpSourceRow(
                name="故障快讯",
                url="https://broken.example/mcp",
                priority=70,
                enabled=True,
                tool_mappings=mapping,
            ),
            McpSourceRow(
                name="已关闭快讯",
                url="https://disabled.example/mcp",
                priority=60,
                enabled=False,
                tool_mappings=mapping,
            ),
        ]
    )
    db.commit()
    now = utc_now().replace(microsecond=0)
    calls: list[tuple[str, str]] = []

    async def fake_call(row, _purpose, arguments):
        cursor = str(arguments.get("cursor") or "")
        calls.append((row.name, cursor))
        if row.name == "故障快讯":
            raise TimeoutError("upstream unavailable")
        page = len([name for name, _ in calls if name == row.name])
        return {
            "data": {
                "items": [
                    {
                        "id": str(page),
                        "content": f"第{page}页财经快讯。",
                        "time": now.isoformat(),
                        "url": f"https://jin10.example/flash/{page}",
                    }
                ],
                "next_cursor": f"page-{page + 1}",
                "has_more": True,
            }
        }

    monkeypatch.setattr("backend.app.services.mcp_registry.call_source_tool", fake_call)
    items, errors = asyncio.run(fetch_enabled_news_feeds(now - timedelta(hours=1), 40))

    assert len(items) == 3
    assert [cursor for name, cursor in calls if name == "金十"] == ["", "page-2", "page-3"]
    assert not any(name == "已关闭快讯" for name, _ in calls)
    assert errors == [{"source": "故障快讯", "error": "TimeoutError: upstream unavailable"}]


def test_search_sources_run_in_parallel_and_merge_balanced_results(monkeypatch):
    calls: list[tuple[str, int]] = []

    async def fake_call(row, _purpose, arguments):
        calls.append((row.name, arguments["limit"]))
        await asyncio.sleep(0)
        if row.name == "SearXNG":
            return {
                "results": [
                    {
                        "title": "Shared result",
                        "url": "https://example.com/shared?utm_source=searxng",
                        "snippet": "SearXNG summary",
                    },
                    {
                        "title": "SearXNG one",
                        "url": "https://searx.example/one",
                        "snippet": "SearXNG result one",
                    },
                    {
                        "title": "SearXNG two",
                        "url": "https://searx.example/two",
                        "snippet": "SearXNG result two",
                    },
                ]
            }
        return {
            "results": [
                {
                    "title": "Shared result",
                    "url": "https://example.com/shared",
                    "snippet": "DuckDuckGo summary",
                },
                {
                    "title": "DuckDuckGo one",
                    "url": "https://duck.example/one",
                    "snippet": "DuckDuckGo result one",
                },
                {
                    "title": "DuckDuckGo two",
                    "url": "https://duck.example/two",
                    "snippet": "DuckDuckGo result two",
                },
            ]
        }

    monkeypatch.setattr("backend.app.services.mcp_registry.call_source_tool", fake_call)
    with TestClient(app):
        items, errors = asyncio.run(
            search_enabled_sources(SearchRequest(query="market verification", limit=6))
        )

    assert errors == []
    assert calls == [("SearXNG", 5), ("DuckDuckGo", 5)]
    assert [item.source for item in items] == [
        "SearXNG",
        "SearXNG",
        "DuckDuckGo",
        "SearXNG",
        "DuckDuckGo",
    ]
    assert items[0].sources == ["SearXNG", "DuckDuckGo"]


def test_search_source_failure_does_not_hide_healthy_results(monkeypatch):
    async def fake_call(row, _purpose, _arguments):
        if row.name == "DuckDuckGo":
            raise TimeoutError("rate limited")
        return {
            "results": [
                {
                    "title": "Healthy result",
                    "url": "https://example.com/healthy",
                    "snippet": "Available from the healthy source",
                }
            ]
        }

    monkeypatch.setattr("backend.app.services.mcp_registry.call_source_tool", fake_call)
    with TestClient(app):
        items, errors = asyncio.run(
            search_enabled_sources(SearchRequest(query="market verification"))
        )

    assert [item.source for item in items] == ["SearXNG"]
    assert errors == [{"source": "DuckDuckGo", "error": "TimeoutError: rate limited"}]


def test_search_source_connection_test_calls_upstream(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_discover(_row):
        return [
            {
                "name": "web_search",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {},
                        "limit": {},
                        "language": {},
                        "time_range": {},
                    },
                },
            }
        ]

    async def fake_call(row, purpose, arguments):
        calls.append((row.name, {"purpose": purpose, **arguments}))
        return {"results": [{"title": "ok", "url": "https://example.com", "snippet": "ok"}]}

    monkeypatch.setattr("backend.app.api_integrations.discover_source", fake_discover)
    monkeypatch.setattr("backend.app.api_integrations.call_source_tool", fake_call)
    with TestClient(app) as client:
        sources = client.get("/api/v1/admin/mcp-sources").json()
        duckduckgo = next(item for item in sources if item["name"] == "DuckDuckGo")
        tested = client.post(f"/api/v1/admin/mcp-sources/{duckduckgo['id']}/test")

    assert tested.status_code == 200
    assert tested.json()["source"]["last_status"] == "healthy"
    assert calls == [
        (
            "DuckDuckGo",
            {
                "purpose": "web_search",
                "query": "latest market news",
                "limit": 1,
                "language": "en",
                "time_range": "day",
            },
        )
    ]


def test_jin10_connection_test_uses_financial_news_query_and_adapter(monkeypatch, db):
    calls: list[tuple[str, dict[str, object]]] = []
    source = McpSourceRow(
        name="金十",
        url="https://mcp.jin10.com/mcp",
        priority=80,
        enabled=True,
        tool_mappings={
            "news_search": {
                "tool_name": "search_flash",
                "input_bindings": {"query": "keyword"},
                "defaults": {},
                "output_adapter": "jin10_flash_v1",
            }
        },
    )
    db.add(source)
    db.commit()

    async def fake_discover(_row):
        return [
            {
                "name": "search_flash",
                "input_schema": {
                    "type": "object",
                    "properties": {"keyword": {}},
                },
            }
        ]

    async def fake_call(row, purpose, arguments):
        calls.append((row.name, {"purpose": purpose, **arguments}))
        return {
            "data": {
                "items": [
                    {
                        "content": "ETF市场快讯。",
                        "url": "https://jin10.example/flash/etf",
                    }
                ]
            }
        }

    monkeypatch.setattr("backend.app.api_integrations.discover_source", fake_discover)
    monkeypatch.setattr("backend.app.api_integrations.call_source_tool", fake_call)
    with TestClient(app) as client:
        tested = client.post(f"/api/v1/admin/mcp-sources/{source.id}/test")

    assert tested.status_code == 200
    assert tested.json()["source"]["last_status"] == "healthy"
    assert calls == [
        (
            "金十",
            {
                "purpose": "news_search",
                "query": "ETF",
                "limit": 1,
                "language": "zh-CN",
                "time_range": "day",
            },
        )
    ]


def test_search_source_connection_test_rejects_empty_upstream(monkeypatch):
    async def fake_discover(_row):
        return [
            {
                "name": "web_search",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {},
                        "limit": {},
                        "language": {},
                        "time_range": {},
                    },
                },
            }
        ]

    async def empty_search(_row, _purpose, _arguments):
        return {"results": []}

    monkeypatch.setattr("backend.app.api_integrations.discover_source", fake_discover)
    monkeypatch.setattr("backend.app.api_integrations.call_source_tool", empty_search)
    with TestClient(app) as client:
        sources = client.get("/api/v1/admin/mcp-sources").json()
        duckduckgo = next(item for item in sources if item["name"] == "DuckDuckGo")
        tested = client.post(f"/api/v1/admin/mcp-sources/{duckduckgo['id']}/test")

    assert tested.status_code == 200
    assert tested.json()["source"]["last_status"] == "failed"
    assert "search upstream returned no results" in tested.json()["source"]["last_error"]


def test_manual_search_is_open_without_admin_token(monkeypatch):
    async def fake_search(_payload):
        return [], []

    monkeypatch.setattr("backend.app.api_integrations.search_enabled_sources", fake_search)
    with TestClient(app) as client:
        response = client.post("/api/v1/admin/search", json={"query": "market verification"})
    assert response.status_code == 200
    assert response.json() == {"items": [], "errors": []}


def test_mapping_arguments_must_match_discovered_tool_schema():
    tools = [
        {
            "name": "web_search",
            "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}},
        }
    ]
    with pytest.raises(ValueError, match="absent"):
        validate_mappings(
            {
                "web_search": {
                    "tool_name": "web_search",
                    "input_bindings": {"query": "unknown_argument"},
                    "defaults": {},
                    "output_adapter": "search_results_v1",
                }
            },
            tools,
        )


def test_weknora_default_is_public_but_updates_require_admin():
    with TestClient(app) as client:
        public = client.get("/api/v1/integrations/weknora")
        denied = client.put("/api/v1/admin/integrations/weknora", json={"url": "http://10.0.0.1/"})
        updated = client.put(
            "/api/v1/admin/integrations/weknora",
            headers=ADMIN,
            json={"url": "http://10.0.0.1/"},
        )
    assert public.json()["url"] == "http://10.15.0.28/"
    assert denied.status_code == 401
    assert updated.json()["url"] == "http://10.0.0.1/"


def test_conclusions_list_filter_and_detail(db):
    asset = AssetRef(
        asset_id="equity:XNAS:TEST",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="TEST",
        name="Test Corp",
        exchange_or_provider="XNAS",
    )
    now = utc_now() - timedelta(minutes=1)
    run = ResearchRun(asset=asset, as_of=now)
    recommendation = Recommendation(
        run_id=run.id,
        asset=asset,
        score=65,
        rating="bullish",
        confidence=0.72,
        bull_probability=0.6,
        base_probability=0.3,
        bear_probability=0.1,
        thesis=Thesis(summary="Durable earnings growth"),
        as_of=now,
        evidence_complete=True,
    )
    run.recommendation = recommendation
    save_run(db, run)
    save_recommendation(db, recommendation)

    with TestClient(app) as client:
        listed = client.get(
            "/api/v1/conclusions?q=earnings&market=US&rating=bullish&evidence_status=complete"
        )
        detail = client.get(f"/api/v1/conclusions/{recommendation.id}")
    assert listed.status_code == 200
    assert listed.json()["items"][0]["id"] == str(recommendation.id)
    assert detail.status_code == 200
    assert detail.json()["recommendation"]["thesis"]["summary"] == "Durable earnings growth"
    assert detail.json()["run"]["id"] == str(run.id)


def test_conclusions_omit_all_scores_when_evidence_is_insufficient(db):
    asset = AssetRef(
        asset_id="equity:XNAS:NOSCORE",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="NOSCORE",
        name="No Score Corp",
        exchange_or_provider="XNAS",
    )
    now = utc_now() - timedelta(minutes=1)
    run = ResearchRun(asset=asset, as_of=now)
    run.analysis_steps.append(
        AnalysisStep(
            phase="report_drafting",
            executor="ollama",
            model="qwen2.5:7b",
            summary="7B draft completed",
            metrics={"confidence": 0.8},
        )
    )
    recommendation = Recommendation(
        run_id=run.id,
        asset=asset,
        score=0,
        model_score=-20,
        raw_score=-34,
        rating="watch",
        confidence=0,
        bull_probability=0.25,
        base_probability=0.5,
        bear_probability=0.25,
        thesis=Thesis(summary="Evidence is still incomplete"),
        as_of=now,
        evidence_complete=False,
        evidence_strength=0,
    )
    run.recommendation = recommendation
    save_run(db, run)
    save_recommendation(db, recommendation)

    with TestClient(app) as client:
        listed = client.get("/api/v1/conclusions?q=NOSCORE")
        detail = client.get(f"/api/v1/conclusions/{recommendation.id}")

    assert listed.status_code == 200
    assert detail.status_code == 200
    for payload in (listed.json()["items"][0], detail.json()["recommendation"]):
        assert payload["score_available"] is False
        assert payload["score"] is None
        assert payload["model_score"] is None
        assert payload["raw_score"] is None
        assert payload["model_direction"] == "bearish"
        assert payload["model_rating"] == "bearish"
        assert payload["model_confidence"] == 0.8
        assert payload["confidence"] == 0.16


def test_short_term_conclusion_keeps_score_visible_with_evidence_warnings(db):
    asset = AssetRef(
        asset_id="equity:XNAS:SHORT",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="SHORT",
        name="Short Term Corp",
        exchange_or_provider="XNAS",
    )
    now = utc_now() - timedelta(minutes=1)
    run = ResearchRun(asset=asset, as_of=now)
    recommendation = Recommendation(
        run_id=run.id,
        asset=asset,
        score=-27,
        raw_score=-27,
        model_score=-30,
        rating="bearish",
        confidence=0.82,
        bull_probability=0.115,
        base_probability=0.5,
        bear_probability=0.385,
        horizon_days=3,
        horizon_unit=HorizonUnit.TRADING_SESSIONS,
        impact_factors=ImpactFactors(
            direction=-1,
            magnitude=ScoringFactor(value=0.217, reason="small ETF outflow"),
            persistence=ScoringFactor(value=0.2, reason="one day only"),
            representativeness=ScoringFactor(value=0.8, reason="large ETF"),
            market_confirmation=ScoringFactor(value=0, reason="not available"),
        ),
        confidence_factors=ConfidenceFactors(),
        fact_confidence=0.92,
        evidence_warnings=["one official source or two independent sources"],
        thesis=Thesis(summary="A small ETF outflow is mildly bearish."),
        as_of=now,
        evidence_complete=False,
        direction_verified=False,
        scoring_version="short-term-impact-v1",
        calibration_version="component-confidence-v1",
    )
    run.recommendation = recommendation
    assert recommendation.signal_status is SignalStatus.DIRECTIONAL
    save_run(db, run)
    save_recommendation(db, recommendation)

    with TestClient(app) as client:
        detail = client.get(f"/api/v1/conclusions/{recommendation.id}")

    assert detail.status_code == 200
    payload = detail.json()["recommendation"]
    assert payload["score_available"] is True
    assert payload["score"] == -27
    assert payload["rating"] == "bearish"
    assert payload["horizon_unit"] == "trading_sessions"
    assert payload["impact_factors"]["magnitude"]["value"] == 0.217
    assert payload["fact_confidence"] == 0.92
    assert payload["evidence_warnings"]


def _save_target_history_item(
    db,
    asset: AssetRef,
    *,
    as_of,
    rating: str,
    signal_status: str,
) -> Recommendation:
    run = ResearchRun(asset=asset, as_of=as_of)
    recommendation = Recommendation(
        run_id=run.id,
        asset=asset,
        score=0,
        rating=rating,
        confidence=0.5,
        bull_probability=0.25,
        base_probability=0.5,
        bear_probability=0.25,
        thesis=Thesis(summary=f"{asset.symbol} target history"),
        as_of=as_of,
        evidence_complete=True,
        signal_status=signal_status,
    )
    save_recommendation(db, recommendation)
    return recommendation


def test_changed_targets_include_only_latest_rating_change_per_asset(db):
    now = utc_now()
    assets = {
        symbol: AssetRef(
            asset_id=f"equity:XNAS:{symbol}",
            asset_class=AssetClass.EQUITY,
            market=Market.US,
            symbol=symbol,
            name=f"{symbol} Corp",
            exchange_or_provider="XNAS",
        )
        for symbol in ("STATUS", "RATING", "BOTH", "INITIAL")
    }
    _save_target_history_item(
        db,
        assets["STATUS"],
        as_of=now - timedelta(minutes=8),
        rating="watch",
        signal_status="insufficient_evidence",
    )
    _save_target_history_item(
        db,
        assets["STATUS"],
        as_of=now - timedelta(minutes=3),
        rating="watch",
        signal_status="neutral",
    )
    _save_target_history_item(
        db,
        assets["RATING"],
        as_of=now - timedelta(minutes=7),
        rating="watch",
        signal_status="directional",
    )
    rating_latest = _save_target_history_item(
        db,
        assets["RATING"],
        as_of=now - timedelta(minutes=2),
        rating="bearish",
        signal_status="directional",
    )
    _save_target_history_item(
        db,
        assets["BOTH"],
        as_of=now - timedelta(minutes=9),
        rating="watch",
        signal_status="insufficient_evidence",
    )
    _save_target_history_item(
        db,
        assets["BOTH"],
        as_of=now - timedelta(minutes=6),
        rating="bullish",
        signal_status="directional",
    )
    both_latest = _save_target_history_item(
        db,
        assets["BOTH"],
        as_of=now - timedelta(minutes=1),
        rating="strongly_bearish",
        signal_status="neutral",
    )
    both_latest_research = _save_target_history_item(
        db,
        assets["BOTH"],
        as_of=now,
        rating="strongly_bearish",
        signal_status="directional",
    )
    _save_target_history_item(
        db,
        assets["INITIAL"],
        as_of=now + timedelta(minutes=1),
        rating="bullish",
        signal_status="directional",
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/changed-targets")

    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["asset"]["symbol"] for item in body["items"]] == [
        "BOTH",
        "RATING",
    ]
    items = {item["asset"]["symbol"]: item for item in body["items"]}
    assert "STATUS" not in items
    assert items["RATING"]["recommendation_id"] == str(rating_latest.id)
    assert items["RATING"]["latest_recommendation_id"] == str(rating_latest.id)
    assert items["RATING"]["status_changed"] is False
    assert items["RATING"]["rating_changed"] is True
    assert items["BOTH"]["recommendation_id"] == str(both_latest.id)
    assert items["BOTH"]["latest_recommendation_id"] == str(both_latest_research.id)
    assert datetime.fromisoformat(items["BOTH"]["latest_researched_at"]) == both_latest_research.as_of
    assert items["BOTH"]["previous"] == {
        "signal_status": "directional",
        "rating": "bullish",
    }
    assert items["BOTH"]["status_changed"] is True
    assert items["BOTH"]["rating_changed"] is True
    assert all(item["rating_changed"] is True for item in body["items"])

    with TestClient(app) as client:
        detail = client.get(
            f"/api/v1/conclusions/{items['BOTH']['latest_recommendation_id']}"
        )

    assert detail.status_code == 200
    assert detail.json()["recommendation"]["id"] == str(both_latest_research.id)


def test_changed_targets_cursor_paginates_unique_assets(db):
    now = utc_now()
    for index, symbol in enumerate(("PAGE1", "PAGE2", "PAGE3")):
        asset = AssetRef(
            asset_id=f"equity:XNAS:{symbol}",
            asset_class=AssetClass.EQUITY,
            market=Market.US,
            symbol=symbol,
            name=f"{symbol} Corp",
            exchange_or_provider="XNAS",
        )
        _save_target_history_item(
            db,
            asset,
            as_of=now - timedelta(hours=1, minutes=index),
            rating="watch",
            signal_status="directional",
        )
        _save_target_history_item(
            db,
            asset,
            as_of=now - timedelta(minutes=index),
            rating="bearish",
            signal_status="directional",
        )

    with TestClient(app) as client:
        first = client.get("/api/v1/changed-targets?limit=2")
        second = client.get(
            "/api/v1/changed-targets",
            params={"limit": 2, "cursor": first.json()["next_cursor"]},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert [item["asset"]["symbol"] for item in first.json()["items"]] == [
        "PAGE1",
        "PAGE2",
    ]
    assert first.json()["next_cursor"]
    assert [item["asset"]["symbol"] for item in second.json()["items"]] == ["PAGE3"]
    assert second.json()["next_cursor"] is None
