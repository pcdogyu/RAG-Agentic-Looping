from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.orm import Session

from backend.app.db import AssetRow, IndustryRow, get_db
from backend.app.services.asset_universe import universe_status
from backend.app.services.mcp_registry import require_admin_token
from backend.app.storage import asset_from_row

router = APIRouter()
Db = Annotated[Session, Depends(get_db)]


def require_admin(x_admin_token: Annotated[str | None, Header()] = None) -> None:
    try:
        require_admin_token(x_admin_token)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail="administrator token required") from exc


class AssetEditInput(BaseModel):
    aliases: list[str] | None = Field(default=None, max_length=50)
    industry_id: str | None = None
    active: bool | None = None


@router.get("/api/v1/asset-universe")
def asset_universe(
    db: Db,
    q: str = "",
    market: str = "",
    sector_id: str = "",
    industry_id: str = "",
    active: bool | None = True,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
):
    statement = select(AssetRow)
    count_statement = select(func.count(AssetRow.id))
    filters = []
    if q.strip():
        term = f"%{q.strip()}%"
        filters.append(
            or_(
                AssetRow.symbol.ilike(term),
                AssetRow.name.ilike(term),
                cast(AssetRow.aliases, String).ilike(term),
            )
        )
    if market:
        filters.append(AssetRow.market == market.upper())
    if sector_id:
        filters.append(AssetRow.sector_id == sector_id)
    if industry_id:
        filters.append(AssetRow.industry_id == industry_id)
    if active is not None:
        filters.append(AssetRow.active.is_(active))
    if filters:
        statement = statement.where(*filters)
        count_statement = count_statement.where(*filters)
    rows = db.scalars(
        statement.order_by(AssetRow.market, AssetRow.symbol).offset(offset).limit(limit)
    ).all()
    return {
        "items": [asset_from_row(row).model_dump(mode="json") for row in rows],
        "total": int(db.scalar(count_statement) or 0),
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/v1/industries")
def industries(db: Db, market: str = ""):
    rows = db.scalars(
        select(IndustryRow)
        .where(IndustryRow.active.is_(True))
        .order_by(IndustryRow.level, IndustryRow.parent_id, IndustryRow.name_zh)
    ).all()
    count_statement = (
        select(AssetRow.industry_id, func.count(AssetRow.id))
        .where(AssetRow.active.is_(True), AssetRow.industry_id != "")
        .group_by(AssetRow.industry_id)
    )
    if market:
        count_statement = count_statement.where(AssetRow.market == market.upper())
    counts = {industry_id: count for industry_id, count in db.execute(count_statement)}
    return [
        {
            "industry_id": row.id,
            "parent_id": row.parent_id,
            "level": row.level,
            "name_zh": row.name_zh,
            "name_en": row.name_en,
            "aliases": row.aliases or [],
            "asset_count": counts.get(row.id, 0),
        }
        for row in rows
    ]


@router.get("/api/v1/asset-universe/status")
def asset_universe_status(db: Db):
    return universe_status(db)


@router.post(
    "/api/v1/admin/asset-universe/refresh",
    status_code=202,
    dependencies=[Depends(require_admin)],
)
def refresh_asset_universe():
    from backend.app.worker import refresh_asset_universe as refresh_task

    task = refresh_task.apply_async(queue="io")
    return {"task_id": task.id, "status": "queued"}


@router.post(
    "/api/v1/admin/asset-universe/backfill",
    status_code=202,
    dependencies=[Depends(require_admin)],
)
def backfill_asset_mappings(days: int = Query(default=7, ge=1, le=30)):
    from backend.app.worker import backfill_asset_mappings as backfill_task

    task = backfill_task.apply_async(kwargs={"days": days}, queue="io")
    return {"task_id": task.id, "status": "queued", "days": days}


@router.patch(
    "/api/v1/admin/assets/{asset_id}",
    dependencies=[Depends(require_admin)],
)
def edit_asset(asset_id: str, payload: AssetEditInput, db: Db):
    row = db.get(AssetRow, asset_id)
    if row is None:
        raise HTTPException(404, "asset not found")
    if payload.aliases is not None:
        row.aliases = list(dict.fromkeys(item.strip() for item in payload.aliases if item.strip()))
    if payload.industry_id is not None:
        if payload.industry_id and db.get(IndustryRow, payload.industry_id) is None:
            raise HTTPException(422, "unknown industry_id")
        row.manual_industry_id = payload.industry_id
        row.industry_id = payload.industry_id
        industry = db.get(IndustryRow, payload.industry_id) if payload.industry_id else None
        row.sector_id = industry.parent_id if industry and industry.parent_id else ""
    if payload.active is not None:
        row.manual_active = payload.active
        row.active = payload.active
    db.add(row)
    db.commit()
    db.refresh(row)
    return asset_from_row(row)
