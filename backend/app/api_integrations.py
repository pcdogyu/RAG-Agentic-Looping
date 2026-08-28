from __future__ import annotations

import base64
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, HttpUrl, ValidationError
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.db import (
    IntegrationSettingRow,
    McpSourceRow,
    RecommendationRow,
    ResearchRunRow,
    get_db,
)
from backend.app.domain import Recommendation, ResearchRun, SignalStatus, utc_now
from backend.app.services.fact_sources import (
    BUILTIN_SOURCE_GROUPS,
    FACT_SOURCE_GROUPS,
    OTHER_GROUP,
    delete_source_group,
    get_effective_settings,
    native_group_config,
    probe_native_group,
    reset_native_group_config,
    save_native_group_config,
    set_source_group,
    source_group_id,
)
from backend.app.services.mcp_registry import (
    SearchRequest,
    SourceInput,
    call_source_tool,
    discover_source,
    normalize_search_results,
    require_admin_token,
    search_enabled_sources,
    source_public,
    validate_mappings,
)
from backend.app.services.secret_store import encrypt_secret
from backend.app.services.source_filter import (
    SourceFilterConfig,
    list_filter_logs,
    reset_source_filter,
    save_source_filter,
    source_filter_payload,
)
from backend.app.storage import get_event, get_news, get_run

router = APIRouter()


def require_admin(x_admin_token: Annotated[str | None, Header()] = None) -> None:
    try:
        require_admin_token(x_admin_token)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail="administrator token required") from exc


Admin = Annotated[None, Depends(require_admin)]
Db = Annotated[Session, Depends(get_db)]


class EnabledInput(BaseModel):
    enabled: bool


class WeknoraInput(BaseModel):
    url: HttpUrl


@router.get("/api/v1/source-filter")
def get_source_filter_config(db: Db) -> dict[str, Any]:
    return source_filter_payload(db)


@router.put("/api/v1/source-filter")
def update_source_filter_config(payload: SourceFilterConfig, db: Db) -> dict[str, Any]:
    save_source_filter(db, payload)
    return source_filter_payload(db)


@router.delete("/api/v1/source-filter")
def reset_source_filter_config(db: Db) -> dict[str, Any]:
    reset_source_filter(db)
    return source_filter_payload(db)


@router.get("/api/v1/source-filter/logs")
def get_source_filter_logs(
    db: Db, limit: int = Query(default=100, ge=1, le=500)
) -> dict[str, Any]:
    return {"items": list_filter_logs(db, limit=limit)}


def _apply_source(row: McpSourceRow, payload: SourceInput, *, creating: bool = False) -> None:
    row.name = payload.name.strip()
    row.url = str(payload.url)
    row.description = payload.description.strip()
    row.priority = payload.priority
    row.enabled = payload.enabled
    row.auth_type = payload.auth_type
    row.auth_header_name = payload.auth_header_name
    validate_mappings(payload.tool_mappings, row.discovered_tools or [])
    row.tool_mappings = payload.tool_mappings
    if payload.clear_secret or payload.auth_type == "none":
        row.encrypted_secret = None
    elif payload.secret:
        row.encrypted_secret = encrypt_secret(payload.secret)
    elif creating and payload.auth_type != "none":
        raise ValueError("credential is required for the selected auth type")
    row.updated_at = utc_now()


def _source_payload(row: McpSourceRow, db: Session) -> dict[str, Any]:
    return source_public(row, source_group_id(db, row.id, row.name))


@router.get("/api/v1/admin/mcp-sources")
def list_mcp_sources(db: Db) -> list[dict[str, Any]]:
    rows = db.scalars(select(McpSourceRow).order_by(desc(McpSourceRow.priority))).all()
    return [_source_payload(row, db) for row in rows]


@router.post("/api/v1/admin/mcp-sources", status_code=201)
def create_mcp_source(payload: SourceInput, db: Db) -> dict[str, Any]:
    row = McpSourceRow(name=payload.name, url=str(payload.url))
    try:
        _apply_source(row, payload, creating=True)
        db.add(row)
        db.flush()
        set_source_group(db, row.id, payload.group_id)
        db.commit()
        db.refresh(row)
    except (ValueError, RuntimeError) as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="source name already exists") from exc
    return _source_payload(row, db)


@router.put("/api/v1/admin/mcp-sources/{source_id}")
def update_mcp_source(source_id: UUID, payload: SourceInput, db: Db) -> dict[str, Any]:
    row = db.get(McpSourceRow, source_id)
    if not row:
        raise HTTPException(status_code=404, detail="MCP source not found")
    if (
        row.managed
        and "group_id" in payload.model_fields_set
        and payload.group_id != BUILTIN_SOURCE_GROUPS.get(row.name)
    ):
        raise HTTPException(status_code=409, detail="managed source group cannot be changed")
    try:
        _apply_source(row, payload)
        if "group_id" in payload.model_fields_set:
            set_source_group(db, row.id, payload.group_id)
        db.commit()
        db.refresh(row)
    except (ValueError, RuntimeError) as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="source name already exists") from exc
    return _source_payload(row, db)


@router.delete("/api/v1/admin/mcp-sources/{source_id}")
def delete_mcp_source(source_id: UUID, db: Db) -> dict[str, bool]:
    row = db.get(McpSourceRow, source_id)
    if not row:
        raise HTTPException(status_code=404, detail="MCP source not found")
    if row.managed:
        raise HTTPException(status_code=409, detail="managed source cannot be deleted")
    delete_source_group(db, row.id)
    db.delete(row)
    db.commit()
    return {"deleted": True}


@router.patch("/api/v1/admin/mcp-sources/{source_id}/enabled")
def set_mcp_source_enabled(source_id: UUID, payload: EnabledInput, db: Db) -> dict[str, Any]:
    row = db.get(McpSourceRow, source_id)
    if not row:
        raise HTTPException(status_code=404, detail="MCP source not found")
    row.enabled = payload.enabled
    row.updated_at = utc_now()
    db.commit()
    db.refresh(row)
    return _source_payload(row, db)


async def _probe(row: McpSourceRow, db: Session, *, discover: bool) -> dict[str, Any]:
    try:
        tools = await discover_source(row)
        if discover:
            row.discovered_tools = tools
        elif {"web_search", "news_search"} & set(row.tool_mappings or {}):
            purpose = "web_search" if "web_search" in (row.tool_mappings or {}) else "news_search"
            probe_query = "latest market news" if purpose == "web_search" else "ETF"
            result = await call_source_tool(
                row,
                purpose,
                {
                    "query": probe_query,
                    "limit": 1,
                    "language": "en" if purpose == "web_search" else "zh-CN",
                    "time_range": "day",
                },
            )
            adapter = str(
                (row.tool_mappings or {})[purpose].get("output_adapter")
                or "search_results_v1"
            )
            if not normalize_search_results(result, row.name, adapter):
                raise RuntimeError("search upstream returned no results")
        row.last_status = "healthy"
        row.last_error = None
    except Exception as exc:
        row.last_status = "failed"
        leaf = exc
        while isinstance(leaf, BaseExceptionGroup) and leaf.exceptions:
            leaf = leaf.exceptions[0]
        row.last_error = f"{type(leaf).__name__}: {leaf}"[:1000]
        tools = []
    row.last_checked_at = utc_now()
    row.updated_at = utc_now()
    db.commit()
    db.refresh(row)
    return {"source": _source_payload(row, db), "tools": tools}


def _configured_count(config: dict[str, Any]) -> int:
    ignored = {"access_token_source", "mcp_upstream_token_management", "ccxt_exchange"}
    return sum(
        bool(value) for key, value in config.items() if key not in ignored
    )


def _group_status(group_id: str, config: dict[str, Any], sources: list[dict[str, Any]]) -> str:
    enabled = [item for item in sources if item["enabled"]]
    if any(item["last_status"] == "failed" or item["last_error"] for item in enabled):
        return "failed"
    native_ready = {
        "fmp": bool(config.get("access_token_configured")),
        "sec": bool(config.get("identity")),
        "cn_news": bool(
            config.get("akshare_asset_master_enabled")
            or config.get("rss_feed_urls")
            or config.get("official_rss_feed_urls")
        ),
        "crypto": bool(config.get("coingecko_base_url") and config.get("defillama_base_url")),
        "search": bool(config.get("timeout_seconds")),
        "other": True,
    }[group_id]
    if not native_ready or any(item["last_status"] != "healthy" for item in enabled):
        return "pending"
    return "healthy" if group_id != "other" or enabled else "pending"


def _fact_source_groups(db: Session) -> list[dict[str, Any]]:
    rows = db.scalars(select(McpSourceRow).order_by(desc(McpSourceRow.priority))).all()
    grouped: dict[str, list[dict[str, Any]]] = {item["id"]: [] for item in FACT_SOURCE_GROUPS}
    grouped["other"] = []
    for row in rows:
        payload = _source_payload(row, db)
        grouped[payload["group_id"]].append(payload)
    output: list[dict[str, Any]] = []
    for metadata in (*FACT_SOURCE_GROUPS, OTHER_GROUP):
        group_id = metadata["id"]
        if group_id == "other" and not grouped[group_id]:
            continue
        native = native_group_config(db, group_id)
        config = native["config"]
        sources = grouped[group_id]
        output.append(
            {
                **metadata,
                **native,
                "status": _group_status(group_id, config, sources),
                "configured_count": _configured_count(config),
                "mcp_count": len(sources),
                "mcp_sources": sources,
            }
        )
    return output


@router.get("/api/v1/admin/fact-source-groups")
def list_fact_source_groups(db: Db) -> list[dict[str, Any]]:
    return _fact_source_groups(db)


@router.put("/api/v1/admin/fact-source-groups/{group_id}")
def update_fact_source_group(
    group_id: str, payload: dict[str, Any], db: Db
) -> dict[str, Any]:
    try:
        save_native_group_config(db, group_id, payload)
    except (ValidationError, ValueError, RuntimeError) as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return next(item for item in _fact_source_groups(db) if item["id"] == group_id)


@router.delete("/api/v1/admin/fact-source-groups/{group_id}")
def reset_fact_source_group(group_id: str, db: Db) -> dict[str, Any]:
    try:
        reset_native_group_config(db, group_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return next(item for item in _fact_source_groups(db) if item["id"] == group_id)


@router.post("/api/v1/admin/fact-source-groups/{group_id}/test")
async def test_fact_source_group(group_id: str, db: Db) -> dict[str, Any]:
    try:
        settings = get_effective_settings(db=db)
        native = await probe_native_group(group_id, settings)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    mcp_results: list[dict[str, Any]] = []
    rows = db.scalars(select(McpSourceRow).where(McpSourceRow.enabled.is_(True))).all()
    for row in rows:
        if source_group_id(db, row.id, row.name) != group_id:
            continue
        result = await _probe(row, db, discover=False)
        mcp_results.append(
            {"id": str(row.id), "name": row.name, "status": result["source"]["last_status"]}
        )
    ok = bool(native.get("ok")) and all(item["status"] == "healthy" for item in mcp_results)
    return {"ok": ok, "native": native, "mcp_sources": mcp_results}


@router.post("/api/v1/admin/mcp-sources/{source_id}/discover")
async def discover_mcp_source(source_id: UUID, db: Db) -> dict[str, Any]:
    row = db.get(McpSourceRow, source_id)
    if not row:
        raise HTTPException(status_code=404, detail="MCP source not found")
    return await _probe(row, db, discover=True)


@router.post("/api/v1/admin/mcp-sources/{source_id}/test")
async def test_mcp_source(source_id: UUID, db: Db) -> dict[str, Any]:
    row = db.get(McpSourceRow, source_id)
    if not row:
        raise HTTPException(status_code=404, detail="MCP source not found")
    return await _probe(row, db, discover=False)


@router.post("/api/v1/admin/search")
async def admin_search(payload: SearchRequest) -> dict[str, Any]:
    results, errors = await search_enabled_sources(payload)
    return {"items": [item.model_dump(mode="json") for item in results], "errors": errors}


def _weknora_payload(db: Session) -> dict[str, str]:
    row = db.get(IntegrationSettingRow, "weknora")
    return row.payload if row else {"url": get_settings().weknora_default_url}


@router.get("/api/v1/integrations/weknora")
def get_weknora(db: Db) -> dict[str, str]:
    return _weknora_payload(db)


@router.put("/api/v1/admin/integrations/weknora", dependencies=[Depends(require_admin)])
def update_weknora(payload: WeknoraInput, db: Db) -> dict[str, str]:
    row = db.get(IntegrationSettingRow, "weknora") or IntegrationSettingRow(key="weknora")
    row.payload = {"url": str(payload.url)}
    row.updated_at = utc_now()
    db.add(row)
    db.commit()
    return row.payload


@router.post("/api/v1/admin/integrations/weknora/test", dependencies=[Depends(require_admin)])
async def test_weknora(payload: WeknoraInput) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=5) as client:
            response = await client.get(str(payload.url))
        return {"ok": response.status_code < 500, "status_code": response.status_code}
    except httpx.HTTPError as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:500]}


def _encode_cursor(as_of: datetime, recommendation_id: UUID) -> str:
    raw = f"{as_of.isoformat()}|{recommendation_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        stamp, item_id = base64.urlsafe_b64decode(padded).decode().split("|", 1)
        return datetime.fromisoformat(stamp), UUID(item_id)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc


def _model_confidence_from_run(run: ResearchRun | None) -> float | None:
    if run is None:
        return None
    for step in reversed(run.analysis_steps):
        if step.phase not in {"report_revision", "report_drafting"}:
            continue
        value = step.metrics.get("confidence")
        if isinstance(value, int | float) and 0 <= value <= 1:
            return float(value)
    return None


def _public_recommendation(
    recommendation: Recommendation,
    run: ResearchRun | None = None,
) -> dict[str, Any]:
    payload = recommendation.model_dump(mode="json")
    if payload["model_confidence"] is None:
        payload["model_confidence"] = _model_confidence_from_run(run)
    short_term_score = recommendation.scoring_version == "short-term-impact-v1"
    score_available = bool(
        recommendation.signal_status is not SignalStatus.TECHNICAL_FAILURE
        and (
            short_term_score
            or (
                recommendation.direction_verified
                and recommendation.signal_status is not SignalStatus.INSUFFICIENT_EVIDENCE
            )
        )
    )
    payload["score_available"] = score_available
    if not score_available:
        payload["score"] = None
        payload["model_score"] = None
        payload["raw_score"] = None
        if (
            recommendation.signal_status is SignalStatus.INSUFFICIENT_EVIDENCE
            and payload["confidence"] == 0
            and isinstance(payload["model_confidence"], int | float)
        ):
            payload["confidence"] = round(
                min(
                    payload["model_confidence"]
                    * max(0.2, recommendation.evidence_strength)
                    * max(0.5, recommendation.mapping_confidence),
                    0.54,
                ),
                4,
            )
    return payload


def _conclusion_detail(db: Session, recommendation: Recommendation) -> dict[str, Any]:
    run = get_run(db, recommendation.run_id)
    event = get_event(db, run.event_id) if run and run.event_id else None
    news = [get_news(db, item_id) for item_id in event.news_item_ids] if event else []
    return {
        "recommendation": _public_recommendation(recommendation, run),
        "run": run.model_dump(mode="json") if run else None,
        "event": event.model_dump(mode="json") if event else None,
        "news": [item.model_dump(mode="json") for item in news if item],
        "evidence": [item.model_dump(mode="json") for item in (run.evidence if run else [])],
    }


def _latest_changed_targets(db: Session) -> list[tuple[Recommendation, Recommendation]]:
    rows = db.scalars(
        select(RecommendationRow).order_by(
            RecommendationRow.asset_id,
            RecommendationRow.as_of,
            RecommendationRow.id,
        )
    ).all()
    previous_by_asset: dict[str, Recommendation] = {}
    latest_change_by_asset: dict[str, tuple[Recommendation, Recommendation]] = {}
    for row in rows:
        current = Recommendation.model_validate(row.payload)
        previous = previous_by_asset.get(row.asset_id)
        if previous and previous.rating != current.rating:
            latest_change_by_asset[row.asset_id] = (previous, current)
        previous_by_asset[row.asset_id] = current
    return sorted(
        latest_change_by_asset.values(),
        key=lambda pair: (pair[1].as_of, pair[1].id.int),
        reverse=True,
    )


@router.get("/api/v1/changed-targets")
def list_changed_targets(
    db: Db,
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, Any]:
    changes = _latest_changed_targets(db)
    if cursor:
        cursor_time, cursor_id = _decode_cursor(cursor)
        changes = [
            pair
            for pair in changes
            if pair[1].as_of < cursor_time
            or (pair[1].as_of == cursor_time and pair[1].id.int < cursor_id.int)
        ]
    has_more = len(changes) > limit
    visible = changes[:limit]
    items = []
    for previous, current in visible:
        items.append(
            {
                "asset": current.asset.model_dump(mode="json"),
                "recommendation_id": current.id,
                "changed_at": current.as_of,
                "previous": {
                    "signal_status": previous.signal_status.value,
                    "rating": previous.rating.value,
                },
                "current": {
                    "signal_status": current.signal_status.value,
                    "rating": current.rating.value,
                },
                "status_changed": previous.signal_status != current.signal_status,
                "rating_changed": previous.rating != current.rating,
            }
        )
    next_cursor = None
    if has_more and visible:
        next_cursor = _encode_cursor(visible[-1][1].as_of, visible[-1][1].id)
    return {"items": items, "next_cursor": next_cursor}


@router.get("/api/v1/conclusions")
def list_conclusions(
    db: Db,
    q: str = "",
    market: str = "",
    rating: str = "",
    evidence_status: str = "",
    cursor: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    statement = select(RecommendationRow)
    if rating:
        statement = statement.where(RecommendationRow.rating == rating)
    if cursor:
        cursor_time, cursor_id = _decode_cursor(cursor)
        statement = statement.where(
            or_(
                RecommendationRow.as_of < cursor_time,
                and_(RecommendationRow.as_of == cursor_time, RecommendationRow.id < cursor_id),
            )
        )
    rows = list(
        db.scalars(
            statement.order_by(desc(RecommendationRow.as_of), desc(RecommendationRow.id)).limit(
                limit + 1
            )
        ).all()
    )
    items: list[Recommendation] = []
    query_text = q.strip().lower()
    for row in rows:
        recommendation = Recommendation.model_validate(row.payload)
        if market and recommendation.asset.market.value.casefold() != market.casefold():
            continue
        if (
            query_text
            and query_text
            not in (
                f"{recommendation.asset.symbol} {recommendation.asset.name} "
                f"{recommendation.thesis.summary}"
            ).lower()
        ):
            continue
        if evidence_status == "complete" and not recommendation.evidence_complete:
            continue
        if evidence_status == "incomplete" and recommendation.evidence_complete:
            continue
        items.append(recommendation)
    has_more = len(rows) > limit
    items = items[:limit]
    run_ids = {item.run_id for item in items}
    run_rows = (
        db.scalars(select(ResearchRunRow).where(ResearchRunRow.id.in_(run_ids))).all()
        if run_ids
        else []
    )
    runs_by_id = {
        row.id: ResearchRun.model_validate(row.payload)
        for row in run_rows
    }
    next_cursor = _encode_cursor(items[-1].as_of, items[-1].id) if has_more and items else None
    return {
        "items": [
            _public_recommendation(item, runs_by_id.get(item.run_id))
            for item in items
        ],
        "next_cursor": next_cursor,
    }


@router.get("/api/v1/conclusions/{recommendation_id}")
def get_conclusion(recommendation_id: UUID, db: Db) -> dict[str, Any]:
    row = db.get(RecommendationRow, recommendation_id)
    if not row:
        raise HTTPException(status_code=404, detail="conclusion not found")
    return _conclusion_detail(db, Recommendation.model_validate(row.payload))
