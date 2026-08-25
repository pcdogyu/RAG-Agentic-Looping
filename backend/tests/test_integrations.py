from __future__ import annotations

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from backend.app.db import McpSourceRow
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
from backend.app.services.mcp_registry import (
    decrypt_secret,
    encrypt_secret,
    normalize_search_results,
    require_admin_token,
    validate_mappings,
)
from backend.app.storage import save_recommendation, save_run

ADMIN = {"X-Admin-Token": "test-admin-token"}


def test_admin_auth_and_secret_round_trip_are_safe():
    require_admin_token("test-admin-token")
    ciphertext = encrypt_secret("very-secret")
    assert "very-secret" not in ciphertext
    assert decrypt_secret(ciphertext) == "very-secret"

    with TestClient(app) as client:
        missing = client.get("/api/v1/admin/mcp-sources")
        wrong = client.get("/api/v1/admin/mcp-sources", headers={"X-Admin-Token": "wrong"})
    assert missing.status_code == 401
    assert wrong.status_code == 401


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
    }
    with TestClient(app) as client:
        created = client.post("/api/v1/admin/mcp-sources", headers=ADMIN, json=payload)
        assert created.status_code == 201
        body = created.json()
        assert body["secret_configured"] is True
        assert "secret" not in body and "encrypted_secret" not in body

        source_id = body["id"]
        payload["description"] = "updated"
        payload["secret"] = None
        updated = client.put(f"/api/v1/admin/mcp-sources/{source_id}", headers=ADMIN, json=payload)
        assert updated.status_code == 200
        assert updated.json()["secret_configured"] is True

        payload["clear_secret"] = True
        cleared = client.put(f"/api/v1/admin/mcp-sources/{source_id}", headers=ADMIN, json=payload)
        assert cleared.json()["secret_configured"] is False

        disabled = client.patch(
            f"/api/v1/admin/mcp-sources/{source_id}/enabled",
            headers=ADMIN,
            json={"enabled": False},
        )
        assert disabled.json()["enabled"] is False
        assert db.get(McpSourceRow, source_id).enabled is False

        deleted = client.delete(f"/api/v1/admin/mcp-sources/{source_id}", headers=ADMIN)
        assert deleted.status_code == 200


def test_managed_sources_seed_and_cannot_be_deleted():
    with TestClient(app) as client:
        items = client.get("/api/v1/admin/mcp-sources", headers=ADMIN).json()
        names = {item["name"] for item in items}
        assert {"SearXNG", "FMP"} <= names
        searxng = next(item for item in items if item["name"] == "SearXNG")
        fmp = next(item for item in items if item["name"] == "FMP")
        assert "web_search" in searxng["tool_mappings"]
        assert fmp["url"] == "http://fmp-mcp:8080/mcp"
        assert set(fmp["tool_mappings"]) == {"quote", "fundamentals", "filings"}
        response = client.delete(f"/api/v1/admin/mcp-sources/{searxng['id']}", headers=ADMIN)
    assert response.status_code == 409


def test_probe_unwraps_exception_group(monkeypatch, db):
    async def failing_discover(_row):
        raise ExceptionGroup("task group", [ConnectionError("service unavailable")])

    monkeypatch.setattr("backend.app.api_integrations.discover_source", failing_discover)
    with TestClient(app) as client:
        items = client.get("/api/v1/admin/mcp-sources", headers=ADMIN).json()
        source = next(item for item in items if item["name"] == "FMP")
        tested = client.post(f"/api/v1/admin/mcp-sources/{source['id']}/test", headers=ADMIN)
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
        items = client.get("/api/v1/admin/mcp-sources", headers=ADMIN).json()
        source = next(item for item in items if item["name"] == "SearXNG")
        discovered = client.post(
            f"/api/v1/admin/mcp-sources/{source['id']}/discover", headers=ADMIN
        )
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
