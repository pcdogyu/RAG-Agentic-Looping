from fastapi.testclient import TestClient

from backend.app.main import app


def test_health_and_asset_endpoints():
    with TestClient(app) as client:
        health = client.get("/health")
        assets = client.get("/api/v1/assets")

    assert health.status_code == 200
    assert health.json()["database"] is True
    assert assets.status_code == 200
    assert any(item["asset_id"] == "crypto:coingecko:bitcoin" for item in assets.json())
