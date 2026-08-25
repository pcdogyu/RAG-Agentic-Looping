from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from backend.app.config import Settings
from backend.app.db import IntegrationSettingRow, McpSourceRow
from backend.app.domain import (
    AssetClass,
    AssetRef,
    Market,
    Recommendation,
    ResearchRun,
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
    SearchRequest,
    normalize_search_results,
    require_admin_token,
    search_enabled_sources,
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
        assert {"SearXNG", "DuckDuckGo", "FMP"} <= names
        searxng = next(item for item in items if item["name"] == "SearXNG")
        duckduckgo = next(item for item in items if item["name"] == "DuckDuckGo")
        fmp = next(item for item in items if item["name"] == "FMP")
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
    search = next(item for item in payload if item["id"] == "search")
    assert [item["name"] for item in fmp["mcp_sources"]] == ["FMP"]
    assert [item["name"] for item in search["mcp_sources"]] == ["SearXNG", "DuckDuckGo"]
    assert "access_token" not in fmp["config"]


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
                "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}},
                "output_schema": {"type": "object"},
            }
        ]

    monkeypatch.setattr("backend.app.api_integrations.discover_source", fake_discover)
    with TestClient(app) as client:
        items = client.get("/api/v1/admin/mcp-sources").json()
        source = next(item for item in items if item["name"] == "SearXNG")
        discovered = client.post(f"/api/v1/admin/mcp-sources/{source['id']}/discover")
    assert discovered.status_code == 200
    assert discovered.json()["source"]["last_status"] == "healthy"
    assert discovered.json()["tools"][0]["name"] == "web_search"


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
        return [{"name": "web_search", "input_schema": {"type": "object"}}]

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


def test_search_source_connection_test_rejects_empty_upstream(monkeypatch):
    async def fake_discover(_row):
        return [{"name": "web_search", "input_schema": {"type": "object"}}]

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
