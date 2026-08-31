from sqlalchemy import create_engine, inspect, text

from backend.app.db import ensure_asset_identity_columns
from backend.app.domain import AssetClass, AssetRef, Market, as_utc, utc_now
from backend.app.storage import ensure_asset, get_asset, upsert_asset


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


def test_ensure_asset_does_not_reactivate_or_replace_master_identity(db):
    synced_at = utc_now()
    canonical = AssetRef(
        asset_id="equity:OTC:EADSY",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="EADSY",
        name="Airbus SE Sponsored ADR",
        exchange_or_provider="OTC",
        instrument_type="adr",
        last_synced_at=synced_at,
        active=False,
    )
    upsert_asset(db, canonical)

    stale_research_snapshot = canonical.model_copy(
        update={
            "name": "Stale Airbus Snapshot",
            "exchange_or_provider": "CRYPTO",
            "instrument_type": "",
            "last_synced_at": None,
            "active": True,
        }
    )
    resolved = ensure_asset(db, stale_research_snapshot)

    stored = get_asset(db, canonical.asset_id)
    assert stored is not None
    assert resolved == stored
    assert stored.name == canonical.name
    assert stored.exchange_or_provider == "OTC"
    assert stored.instrument_type == "adr"
    assert as_utc(stored.last_synced_at) == synced_at
    assert stored.active is False


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
