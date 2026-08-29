from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.app.db import AssetRow, AssetUniverseSyncRow, IndustryRow
from backend.app.domain import AssetRef, Market, utc_now
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.industry_taxonomy import all_industries
from backend.app.storage import asset_from_row

SYNC_MARKETS = (Market.CN, Market.HK, Market.US, Market.CRYPTO)


class AssetUniverseService:
    def __init__(self, db: Session, registry: ProviderRegistry | None = None) -> None:
        self.db = db
        self.registry = registry or ProviderRegistry()

    def seed_industries(self) -> None:
        for item in all_industries():
            row = self.db.get(IndustryRow, item.industry_id) or IndustryRow(id=item.industry_id)
            row.parent_id = item.parent_id
            row.level = item.level
            row.name_zh = item.name_zh
            row.name_en = item.name_en
            row.aliases = item.aliases
            row.active = item.active
            self.db.add(row)
        self.db.commit()

    def sync(self, markets: Iterable[Market] | None = None) -> dict[str, object]:
        self.seed_industries()
        selected = tuple(markets or SYNC_MARKETS)
        listed: list[AssetRef] | None = None
        results: dict[str, dict[str, object]] = {}
        for market in selected:
            status = self._start_status(market)
            try:
                if market in {Market.CN, Market.HK}:
                    if listed is None:
                        listed = self.registry.akshare.list_equity_universe()
                    assets = [item for item in listed if item.market is market]
                elif market is Market.US:
                    assets = self.registry.fmp.list_equity_universe()
                elif market is Market.CRYPTO:
                    assets = self.registry.crypto.all_assets()
                else:
                    assets = []
                if not assets:
                    raise RuntimeError(f"{market.value} provider returned an empty universe")
                results[market.value] = self._persist_market(market, assets, status)
            except Exception as exc:
                status.status = "failed"
                status.last_error = f"{type(exc).__name__}: {exc}"[:500]
                status.completed_at = utc_now()
                self.db.add(status)
                self.db.commit()
                results[market.value] = {
                    "status": "failed",
                    "error": status.last_error,
                    "assets": status.asset_count,
                }
        return {
            "markets": results,
            "status": "completed_with_errors"
            if any(item["status"] == "failed" for item in results.values())
            else "completed",
        }

    def _start_status(self, market: Market) -> AssetUniverseSyncRow:
        row = self.db.get(AssetUniverseSyncRow, market.value) or AssetUniverseSyncRow(
            market=market.value
        )
        row.status = "running"
        row.started_at = utc_now()
        row.completed_at = None
        row.last_error = None
        row.added_count = 0
        row.updated_count = 0
        row.deactivated_count = 0
        self.db.add(row)
        self.db.commit()
        return row

    def _persist_market(
        self,
        market: Market,
        assets: list[AssetRef],
        status: AssetUniverseSyncRow,
    ) -> dict[str, object]:
        now = utc_now()
        existing = {
            row.id: row
            for row in self.db.scalars(select(AssetRow).where(AssetRow.market == market.value))
        }
        received: set[str] = set()
        added = updated = 0
        for asset in assets:
            received.add(asset.asset_id)
            row = existing.get(asset.asset_id)
            if row is None:
                row = AssetRow(id=asset.asset_id)
                added += 1
            else:
                updated += 1
            merged_aliases = list(dict.fromkeys([*(row.aliases or []), *asset.aliases]))
            row.asset_class = asset.asset_class.value
            row.market = asset.market.value
            row.symbol = asset.symbol
            row.name = asset.name
            row.exchange_or_provider = asset.exchange_or_provider
            row.currency = asset.currency
            row.aliases = merged_aliases
            row.products = list(dict.fromkeys([*(row.products or []), *asset.products]))
            row.competitors = list(dict.fromkeys([*(row.competitors or []), *asset.competitors]))
            if row.manual_industry_id is not None:
                row.industry_id = row.manual_industry_id
                manual_industry = (
                    self.db.get(IndustryRow, row.manual_industry_id)
                    if row.manual_industry_id
                    else None
                )
                row.sector_id = (
                    manual_industry.parent_id
                    if manual_industry and manual_industry.parent_id
                    else ""
                )
            else:
                row.sector_id = asset.sector_id or row.sector_id or ""
                row.industry_id = asset.industry_id or row.industry_id or ""
            row.raw_sector = asset.raw_sector or row.raw_sector or ""
            row.raw_industry = asset.raw_industry or row.raw_industry or ""
            row.instrument_type = asset.instrument_type or row.instrument_type or ""
            row.market_cap = asset.market_cap if asset.market_cap is not None else row.market_cap
            row.market_cap_rank = (
                asset.market_cap_rank if asset.market_cap_rank is not None else row.market_cap_rank
            )
            row.issuer_id = asset.issuer_id or row.issuer_id
            row.primary_listing_asset_id = (
                asset.primary_listing_asset_id or row.primary_listing_asset_id
            )
            row.lot_size = asset.lot_size
            row.active = row.manual_active if row.manual_active is not None else True
            row.last_synced_at = now
            self.db.add(row)
        deactivated = 0
        for asset_id, row in existing.items():
            if asset_id in received or (row.issuer_id or "").startswith("curated:"):
                continue
            row.active = row.manual_active if row.manual_active is not None else False
            row.last_synced_at = now
            self.db.add(row)
            deactivated += 1
        status.status = "completed"
        status.asset_count = len(received)
        status.industry_count = len({item.industry_id for item in assets if item.industry_id})
        status.added_count = added
        status.updated_count = updated
        status.deactivated_count = deactivated
        status.completed_at = now
        self.db.add(status)
        self.db.commit()
        return {
            "status": "completed",
            "assets": len(received),
            "added": added,
            "updated": updated,
            "deactivated": deactivated,
        }


def universe_status(db: Session) -> dict[str, object]:
    rows = db.scalars(select(AssetUniverseSyncRow).order_by(AssetUniverseSyncRow.market)).all()
    classification_counts = {
        market: (int(count), int(classified))
        for market, count, classified in db.execute(
            select(
                AssetRow.market,
                func.count(AssetRow.id),
                func.count(func.nullif(AssetRow.industry_id, "")),
            )
            .where(AssetRow.active.is_(True))
            .group_by(AssetRow.market)
        )
    }
    counts = {market: count for market, (count, _classified) in classification_counts.items()}
    return {
        "markets": [
            {
                "market": row.market,
                "status": row.status,
                "asset_count": row.asset_count,
                "industry_count": row.industry_count,
                "added_count": row.added_count,
                "updated_count": row.updated_count,
                "deactivated_count": row.deactivated_count,
                "classified_count": classification_counts.get(row.market, (0, 0))[1],
                "unclassified_count": (
                    classification_counts.get(row.market, (0, 0))[0]
                    - classification_counts.get(row.market, (0, 0))[1]
                ),
                "classification_rate": round(
                    classification_counts.get(row.market, (0, 0))[1]
                    / classification_counts.get(row.market, (1, 0))[0],
                    4,
                )
                if classification_counts.get(row.market, (0, 0))[0]
                else 0.0,
                "last_error": row.last_error,
                "started_at": row.started_at,
                "completed_at": row.completed_at,
            }
            for row in rows
        ],
        "active_counts": counts,
    }


def active_assets(db: Session) -> list[AssetRef]:
    return [
        asset_from_row(row)
        for row in db.scalars(select(AssetRow).where(AssetRow.active.is_(True))).all()
    ]
