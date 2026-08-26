from sqlalchemy import create_engine, inspect, text

from backend.app.db import ensure_asset_identity_columns
from backend.app.domain import AssetClass, AssetRef, Market
from backend.app.storage import get_asset, upsert_asset


def test_asset_identity_fields_round_trip(db):
    asset = AssetRef(
        asset_id="equity:OTC:MOPHY",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="MOPHY",
        name="Monadelphous Group Limited Sponsored ADR",
        exchange_or_provider="OTC",
        issuer_id="fmp:monadelphous-group",
        primary_listing_asset_id="equity:ASX:MND.AX",
    )

    upsert_asset(db, asset)

    stored = get_asset(db, asset.asset_id)
    assert stored is not None
    assert stored.issuer_id == "fmp:monadelphous-group"
    assert stored.primary_listing_asset_id == "equity:ASX:MND.AX"


def test_legacy_sqlite_asset_table_upgrade_is_idempotent():
    legacy_engine = create_engine("sqlite:///:memory:")
    with legacy_engine.begin() as connection:
        connection.execute(text("CREATE TABLE assets (id VARCHAR(160) PRIMARY KEY)"))
        connection.execute(text("INSERT INTO assets (id) VALUES ('legacy-asset')"))

    ensure_asset_identity_columns(legacy_engine)
    ensure_asset_identity_columns(legacy_engine)

    columns = {column["name"] for column in inspect(legacy_engine).get_columns("assets")}
    assert {"issuer_id", "primary_listing_asset_id"} <= columns
    with legacy_engine.connect() as connection:
        assert connection.execute(text("SELECT id FROM assets")).scalar_one() == "legacy-asset"
