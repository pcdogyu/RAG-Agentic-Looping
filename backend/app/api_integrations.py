from __future__ import annotations

import base64
import re
import unicodedata
from datetime import datetime
from hashlib import sha256
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, HttpUrl, ValidationError
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.db import (
    EventResearchRunRow,
    EventRow,
    IntegrationSettingRow,
    McpSourceRow,
    NewsFilterLogRow,
    RecommendationRow,
    ResearchRunRow,
    get_db,
)
from backend.app.domain import (
    AssetClass,
    EventReport,
    EventResearchRun,
    NewsItem,
    Recommendation,
    ResearchRun,
    RunStatus,
    SignalStatus,
    SourceQuality,
    TargetImpact,
    TargetType,
    as_utc,
    utc_now,
)
from backend.app.services.event_refresh import public_full_event_research
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
    WHITELIST_MISS_REASON,
    SourceFilterConfig,
    list_filter_logs,
    reset_source_filter,
    save_source_filter,
    source_filter_payload,
)
from backend.app.services.target_trends import (
    CanonicalTarget,
    TargetObservation,
    TargetTrend,
    aggregate_target_trend,
    canonicalize_target,
)
from backend.app.storage import (
    event_news_item_ids,
    get_event,
    get_news,
    get_news_by_content_hash,
    get_run,
    save_news,
)

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


def _rescan_source_quality(source: str) -> SourceQuality:
    normalized = source.casefold()
    if normalized == "sec" or normalized.startswith("sec ") or "sec.gov" in normalized:
        return SourceQuality.OFFICIAL
    if "fmp" in normalized:
        return SourceQuality.PROFESSIONAL
    return SourceQuality.AGGREGATOR


@router.post("/api/v1/source-filter/logs/{log_id}/rescan", status_code=202)
def rescan_source_filter_log(log_id: UUID, db: Db) -> dict[str, Any]:
    row = db.get(NewsFilterLogRow, log_id)
    if row is None:
        raise HTTPException(status_code=404, detail="filtered news record not found")
    if row.matched_keyword != WHITELIST_MISS_REASON:
        raise HTTPException(
            status_code=409,
            detail="only whitelist-miss records can be rescanned",
        )

    content_hash = row.content_hash
    news = get_news_by_content_hash(db, content_hash)
    if news is not None and news.id in event_news_item_ids(db):
        content_hash = sha256(f"{row.content_hash}:rescan:{row.id}".encode()).hexdigest()
        news = get_news_by_content_hash(db, content_hash)
    if news is None:
        now = utc_now()
        news = NewsItem(
            source=row.source,
            source_quality=_rescan_source_quality(row.source),
            title=row.title,
            url=row.url,
            published_at=row.published_at,
            observed_at=now,
            as_of=now,
            content_hash=content_hash,
            raw_metadata={
                "manual_source_filter_rescan": True,
                "source_filter_log_id": str(row.id),
                "original_filter_reason": row.matched_keyword,
                "original_content_hash": row.content_hash,
            },
        )
        if not save_news(db, news):
            news = get_news_by_content_hash(db, content_hash)
        if news is None:
            raise HTTPException(status_code=409, detail="filtered news could not be restored")

    log_snapshot = {
        "id": row.id,
        "content_hash": row.content_hash,
        "source": row.source,
        "title": row.title,
        "url": row.url,
        "matched_keyword": row.matched_keyword,
        "published_at": row.published_at,
        "first_filtered_at": row.first_filtered_at,
        "last_filtered_at": row.last_filtered_at,
        "hit_count": row.hit_count,
    }
    db.delete(row)
    db.commit()
    try:
        from backend.app.worker import enqueue_news_extraction_retry

        task_id = enqueue_news_extraction_retry(
            news,
            force_asset_mapping=True,
        )
    except Exception as exc:
        if db.get(NewsFilterLogRow, log_id) is None:
            db.add(NewsFilterLogRow(**log_snapshot))
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
        raise HTTPException(
            status_code=503,
            detail=f"news rescan could not be queued: {type(exc).__name__}",
        ) from exc

    return {
        "status": "queued",
        "task_id": task_id,
        "news_id": str(news.id),
        "title": news.title,
    }


@router.post("/api/v1/news/{news_id}/retry", status_code=202)
def retry_news_processing(news_id: UUID, db: Db) -> dict[str, str]:
    """Durably retry one orphaned or failed news item by its stable ID."""

    news = get_news(db, news_id)
    if news is None:
        raise HTTPException(status_code=404, detail="news item not found")
    try:
        from backend.app.worker import enqueue_durable_news_retry

        queued = enqueue_durable_news_retry(news_id, force_asset_mapping=True)
    except RuntimeError as exc:
        detail = str(exc)
        status_code = 409 if "already active" in detail else 503
        raise HTTPException(status_code=status_code, detail=detail) from exc
    return {
        "status": "queued",
        "task_id": queued["task_id"],
        "news_id": queued["news_id"],
        "title": news.title,
    }


def _apply_source(row: McpSourceRow, payload: SourceInput, *, creating: bool = False) -> None:
    was_enabled = bool(row.enabled)
    connection_changed = creating or any(
        (
            row.url != str(payload.url),
            row.auth_type != payload.auth_type,
            row.auth_header_name != payload.auth_header_name,
            bool(payload.secret),
            payload.clear_secret,
        )
    )
    mappings_changed = (row.tool_mappings or {}) != payload.tool_mappings
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
    if connection_changed:
        row.discovered_tools = []
        row.last_status = "unchecked"
        row.last_error = None
        row.last_checked_at = None
    elif mappings_changed:
        row.last_status = "discovered" if row.discovered_tools else "unchecked"
        row.last_error = None
        row.last_checked_at = None
    if row.auth_type != "none" and (
        connection_changed or mappings_changed or (payload.enabled and not was_enabled)
    ):
        row.enabled = False
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
    if payload.enabled and row.auth_type != "none":
        if not row.encrypted_secret:
            raise HTTPException(status_code=409, detail="请先配置 MCP 凭据")
        if not row.discovered_tools:
            raise HTTPException(status_code=409, detail="请先完成 MCP 工具发现")
        try:
            validate_mappings(row.tool_mappings or {}, row.discovered_tools)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if row.last_status != "healthy":
            raise HTTPException(status_code=409, detail="请先完成 MCP 连接测试")
    row.enabled = payload.enabled
    row.updated_at = utc_now()
    db.commit()
    db.refresh(row)
    return _source_payload(row, db)


async def _probe(row: McpSourceRow, db: Session, *, discover: bool) -> dict[str, Any]:
    try:
        tools = await discover_source(row)
        if not tools:
            raise RuntimeError("MCP tool discovery returned no tools")
        validate_mappings(row.tool_mappings or {}, tools)
        if discover:
            row.discovered_tools = tools
            row.last_status = "discovered"
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
            row.discovered_tools = tools
        if not discover:
            row.last_status = "healthy"
        row.last_error = None
    except Exception as exc:
        if discover:
            row.discovered_tools = []
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
    if (
        recommendation.scoring_version != "llm-direction-v3"
        and payload["model_confidence"] is None
    ):
        payload["model_confidence"] = _model_confidence_from_run(run)
    current_score = recommendation.scoring_version == "llm-direction-v3"
    short_term_score = recommendation.scoring_version == "short-term-impact-v1"
    score_available = bool(
        recommendation.signal_status is not SignalStatus.TECHNICAL_FAILURE
        and (
            current_score
            or short_term_score
            or (
                recommendation.direction_verified
                and recommendation.signal_status is not SignalStatus.INSUFFICIENT_EVIDENCE
            )
        )
    )
    payload["score_available"] = score_available
    if not score_available:
        payload["score"] = None
        payload["direction_score"] = None
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


def _latest_changed_targets(
    db: Session,
) -> list[tuple[Recommendation, Recommendation, Recommendation]]:
    rows = db.scalars(
        select(RecommendationRow).order_by(
            RecommendationRow.asset_id,
            RecommendationRow.as_of,
            RecommendationRow.id,
        )
    ).all()
    previous_by_asset: dict[str, Recommendation] = {}
    latest_by_asset: dict[str, Recommendation] = {}
    latest_change_by_asset: dict[str, tuple[Recommendation, Recommendation]] = {}
    for row in rows:
        current = Recommendation.model_validate(row.payload)
        previous = previous_by_asset.get(row.asset_id)
        if previous and previous.rating != current.rating:
            latest_change_by_asset[row.asset_id] = (previous, current)
        previous_by_asset[row.asset_id] = current
        latest_by_asset[row.asset_id] = current
    return sorted(
        [
            (previous, changed, latest_by_asset[asset_id])
            for asset_id, (previous, changed) in latest_change_by_asset.items()
        ],
        key=lambda item: (item[1].as_of, item[1].id.int),
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
            item
            for item in changes
            if item[1].as_of < cursor_time
            or (item[1].as_of == cursor_time and item[1].id.int < cursor_id.int)
        ]
    has_more = len(changes) > limit
    visible = changes[:limit]
    items = []
    for previous, current, latest in visible:
        items.append(
            {
                "asset": current.asset.model_dump(mode="json"),
                "recommendation_id": current.id,
                "latest_recommendation_id": latest.id,
                "latest_researched_at": latest.as_of,
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


_CONCLUSION_KIND_ORDER = {"event": 0, "asset": 1}
_VISIBLE_EVENT_STATUSES = {
    RunStatus.COMPLETED.value,
    RunStatus.INSUFFICIENT_EVIDENCE.value,
}
_EVENT_REFRESH_FEED_STATUSES = {
    *_VISIBLE_EVENT_STATUSES,
    RunStatus.QUEUED.value,
    RunStatus.RUNNING.value,
    RunStatus.VERIFYING.value,
    RunStatus.FAILED.value,
}
_MACRO_TARGET_TYPES = {
    TargetType.ECONOMY,
    TargetType.SUPPLY_VOLUME,
    TargetType.COMMODITY_PRICE,
    TargetType.FX_RATE,
    TargetType.INTEREST_RATE,
    TargetType.SECTOR,
    TargetType.RISK_ASSET,
    TargetType.SHIPPING,
    TargetType.OTHER,
}


def _encode_union_cursor(stamp: datetime, kind: str, item_id: UUID) -> str:
    raw = f"{as_utc(stamp).isoformat()}|{kind}|{item_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode_union_cursor(cursor: str) -> tuple[datetime, str, UUID]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        stamp, kind, item_id = base64.urlsafe_b64decode(padded).decode().split("|", 2)
        if kind not in _CONCLUSION_KIND_ORDER:
            raise ValueError("invalid conclusion kind")
        return as_utc(datetime.fromisoformat(stamp)), kind, UUID(item_id)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc


def _conclusion_sort_key(item: dict[str, Any]) -> tuple[datetime, int, int]:
    return (
        as_utc(datetime.fromisoformat(str(item["occurred_at"]))),
        _CONCLUSION_KIND_ORDER[str(item["kind"])],
        UUID(str(item["id"])).int,
    )


def _matches_evidence_status(complete: bool, evidence_status: str) -> bool:
    if evidence_status == "complete":
        return complete
    if evidence_status == "incomplete":
        return not complete
    return True


def _representative_event_impact(report: EventReport) -> TargetImpact | None:
    return max(
        report.impacts,
        key=lambda impact: abs(impact.direction_score or 0),
        default=None,
    )


@router.get("/api/v1/research-conclusions")
def list_research_conclusions(
    db: Db,
    kind: Literal["all", "event", "asset"] = "all",
    q: str = "",
    market: str = "",
    rating: str = "",
    evidence_status: Literal["", "complete", "incomplete"] = "",
    cursor: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    query_text = q.strip().casefold()
    feed: list[dict[str, Any]] = []

    if kind in {"all", "asset"}:
        recommendation_rows = list(db.scalars(select(RecommendationRow)).all())
        recommendations = [
            Recommendation.model_validate(row.payload) for row in recommendation_rows
        ]
        run_ids = {item.run_id for item in recommendations}
        run_rows = (
            db.scalars(select(ResearchRunRow).where(ResearchRunRow.id.in_(run_ids))).all()
            if run_ids
            else []
        )
        runs_by_id = {
            row.id: ResearchRun.model_validate(row.payload) for row in run_rows
        }
        for recommendation in recommendations:
            run = runs_by_id.get(recommendation.run_id)
            if run is not None and run.retryable_reason is not None:
                continue
            asset = recommendation.asset
            if market and asset.market.value.casefold() != market.casefold():
                continue
            if rating and recommendation.rating.value != rating:
                continue
            if not _matches_evidence_status(
                recommendation.evidence_complete, evidence_status
            ):
                continue
            searchable = (
                f"{asset.symbol} {asset.name} {recommendation.thesis.summary}"
            ).casefold()
            if query_text and query_text not in searchable:
                continue
            feed.append(
                {
                    "kind": "asset",
                    "id": recommendation.id,
                    "occurred_at": as_utc(recommendation.as_of).isoformat(),
                    "status": recommendation.signal_status.value,
                    "evidence_complete": recommendation.evidence_complete,
                    "title": f"{asset.symbol} · {asset.name}",
                    "summary": recommendation.thesis.summary,
                    "asset": asset.model_dump(mode="json"),
                    "event": None,
                    "recommendation": _public_recommendation(
                        recommendation, run
                    ),
                    "report": None,
                }
            )

    if kind in {"all", "event"} and not market and not rating:
        event_rows = list(
            db.scalars(
                select(EventResearchRunRow).where(
                    EventResearchRunRow.status.in_(_EVENT_REFRESH_FEED_STATUSES)
                )
            ).all()
        )
        event_ids = {row.event_id for row in event_rows}
        stored_events = (
            db.scalars(select(EventRow).where(EventRow.id.in_(event_ids))).all()
            if event_ids
            else []
        )
        events_by_id = {row.id: row.payload for row in stored_events}
        for row in event_rows:
            run = EventResearchRun.model_validate(row.payload)
            if run.retryable_reason is not None:
                continue
            report = run.report
            if report is None:
                continue
            refresh = public_full_event_research(run)
            if row.status not in _VISIBLE_EVENT_STATUSES and refresh is None:
                continue
            representative_impact = _representative_event_impact(report)
            if not _matches_evidence_status(report.evidence_complete, evidence_status):
                continue
            event = events_by_id.get(run.event_id)
            headline = str((event or {}).get("headline") or report.summary)
            searchable = " ".join(
                [
                    headline,
                    report.summary,
                    *report.affected_markets,
                    *report.affected_sectors,
                ]
            ).casefold()
            if query_text and query_text not in searchable:
                continue
            feed.append(
                {
                    "kind": "event",
                    "id": run.id,
                    "occurred_at": as_utc(row.updated_at).isoformat(),
                    "status": run.status.value,
                    "evidence_complete": report.evidence_complete,
                    "title": headline,
                    "summary": report.summary,
                    "asset": None,
                    "event": (
                        {
                            "id": str(run.event_id),
                            "headline": headline,
                            "event_type": (event or {}).get("event_type", "other"),
                        }
                    ),
                    "recommendation": None,
                    "refresh": refresh,
                    "report": {
                        "confidence": report.confidence,
                        "news_confidence": report.news_confidence,
                        "direction_score": (
                            representative_impact.direction_score
                            if representative_impact
                            else None
                        ),
                        "rating": (
                            representative_impact.rating.value
                            if representative_impact
                            else None
                        ),
                        "impact_count": len(report.impacts),
                        "affected_markets": report.affected_markets,
                        "affected_sectors": report.affected_sectors,
                        "scoring_version": report.scoring_version,
                    },
                }
            )

    feed.sort(key=_conclusion_sort_key, reverse=True)
    if cursor:
        cursor_time, cursor_kind, cursor_id = _decode_union_cursor(cursor)
        cursor_key = (
            cursor_time,
            _CONCLUSION_KIND_ORDER[cursor_kind],
            cursor_id.int,
        )
        feed = [item for item in feed if _conclusion_sort_key(item) < cursor_key]
    has_more = len(feed) > limit
    visible = feed[:limit]
    next_cursor = None
    if has_more and visible:
        last = visible[-1]
        next_cursor = _encode_union_cursor(
            datetime.fromisoformat(str(last["occurred_at"])),
            str(last["kind"]),
            UUID(str(last["id"])),
        )
    return {"items": visible, "next_cursor": next_cursor}


@router.get("/api/v1/event-conclusions/{run_id}")
def get_event_conclusion(run_id: UUID, db: Db) -> dict[str, Any]:
    row = db.get(EventResearchRunRow, run_id)
    if not row:
        raise HTTPException(status_code=404, detail="event conclusion not found")
    run = EventResearchRun.model_validate(row.payload)
    if run.report is None:
        raise HTTPException(status_code=404, detail="event conclusion has no report")
    event = get_event(db, run.event_id)
    news = [get_news(db, news_id) for news_id in (event.news_item_ids if event else [])]
    return {
        "run": run.model_dump(mode="json"),
        "refresh": public_full_event_research(run),
        "event": event.model_dump(mode="json") if event else None,
        "report": run.report.model_dump(mode="json"),
        "news": [item.model_dump(mode="json") for item in news if item],
        "evidence": [item.model_dump(mode="json") for item in run.evidence],
    }


def _normalized_target_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", normalized)


def _macro_target_base(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = re.sub(r"\([^)]*(?:[a-z]{1,8}|\d{4,8})[^)]*\)", " ", normalized)
    for phrase in (
        "continuous benchmark",
        "continuous contract",
        "continuous futures",
        "连续基准",
        "连续合约",
    ):
        normalized = normalized.replace(phrase, " ")
    return _normalized_target_text(normalized)


def _security_aliases(db: Session) -> tuple[set[str], set[str]]:
    names: set[str] = set()
    symbols: set[str] = set()
    for payload in db.scalars(select(RecommendationRow.payload)).all():
        try:
            asset = Recommendation.model_validate(payload).asset
        except (TypeError, ValueError):
            continue
        if asset.asset_class not in {AssetClass.EQUITY, AssetClass.CRYPTO}:
            continue
        names.add(_normalized_target_text(asset.name))
        symbols.add(asset.symbol.casefold())
    return names, symbols


def _resembles_security_target(
    impact: TargetImpact,
    security_names: set[str],
    security_symbols: set[str],
) -> bool:
    if impact.target_type is TargetType.TRADABLE_ASSET:
        return True
    if impact.asset and impact.asset.asset_class in {AssetClass.EQUITY, AssetClass.CRYPTO}:
        return True
    compact = _normalized_target_text(impact.target_name)
    if compact in security_names:
        return True
    tokens = {
        token.casefold()
        for token in re.findall(r"[A-Za-z0-9.\-]+", impact.target_name)
        if len(token) >= 2
    }
    return bool(tokens & security_symbols)


def _impact_state(impact: TargetImpact) -> dict[str, Any]:
    return {
        "rating": impact.rating.value,
        "direction_score": impact.direction_score,
        "rating_confidence": impact.rating_confidence,
    }


def _trend_state(value: Any) -> dict[str, Any]:
    return {
        "rating": value.rating.value,
        "direction_score": value.score,
        "rating_confidence": value.confidence,
        "provisional": value.provisional,
    }


def _public_target_trend(trend: TargetTrend) -> dict[str, Any]:
    return {
        "algorithm_version": "dual-horizon-v1",
        "short_term": _trend_state(trend.short_term),
        "long_term": _trend_state(trend.long_term),
        "composite": _trend_state(trend.combined),
        "event_count_90d": trend.long_term.event_count,
        "eligible_event_count_90d": trend.long_term.eligible_event_count,
        "ignored_event_count_90d": trend.long_term.ignored_event_count,
        "regime_break": trend.long_term.regime_break,
    }


def _representative_macro_impact(impacts: list[TargetImpact]) -> TargetImpact:
    return max(
        impacts,
        key=lambda impact: (
            impact.rating_confidence or 0,
            len(impact.evidence_ids),
            -len(impact.missing_information),
            _normalized_target_text(impact.target_name),
        ),
    )


def _canonical_observation(
    impacts: list[TargetImpact],
    *,
    occurred_at: datetime,
    news_confidence: float,
    provisional: bool,
) -> TargetObservation:
    weights = [max(0.05, impact.rating_confidence or 0) for impact in impacts]
    total_weight = sum(weights)

    def weighted(value: Any) -> float:
        return sum(
            float(value(impact)) * weight
            for impact, weight in zip(impacts, weights, strict=True)
        ) / total_weight

    scores = [impact.direction_score or 0 for impact in impacts]
    conflicting_aliases = max(scores) - min(scores) >= 30
    insufficient = provisional or conflicting_aliases or all(
        impact.technical_failure for impact in impacts
    )
    return TargetObservation(
        occurred_at=occurred_at,
        score=weighted(lambda impact: impact.direction_score or 0),
        rating_confidence=weighted(lambda impact: impact.rating_confidence or 0),
        news_confidence=news_confidence,
        persistence=weighted(lambda impact: impact.factors.persistence),
        realization_probability=weighted(
            lambda impact: impact.factors.realization_probability
        ),
        insufficient_evidence=insufficient,
        provisional=insufficient,
    )


def _macro_target_changes(db: Session) -> list[dict[str, Any]]:
    rows = list(
        db.scalars(
            select(EventResearchRunRow)
            .order_by(EventResearchRunRow.updated_at, EventResearchRunRow.id)
        ).all()
    )
    security_names, security_symbols = _security_aliases(db)
    parsed: list[tuple[EventResearchRunRow, EventResearchRun]] = []
    aliases: dict[tuple[str, str], Any] = {}
    for row in rows:
        run = EventResearchRun.model_validate(row.payload)
        if run.retryable_reason is not None or run.report is None or (
            row.status not in _VISIBLE_EVENT_STATUSES and not run.report_history
        ):
            continue
        parsed.append((row, run))
        for impact in run.report.impacts:
            if (
                impact.target_type in _MACRO_TARGET_TYPES
                and impact.asset
                and impact.asset.asset_class not in {AssetClass.EQUITY, AssetClass.CRYPTO}
            ):
                aliases[
                    (impact.target_type.value, _macro_target_base(impact.target_name))
                ] = impact.asset

    event_ids = {run.event_id for _, run in parsed}
    event_rows = (
        db.scalars(select(EventRow).where(EventRow.id.in_(event_ids))).all()
        if event_ids
        else []
    )
    event_times = {row.id: as_utc(row.published_at) for row in event_rows}
    snapshots_by_key: dict[str, list[dict[str, Any]]] = {}
    for row, run in parsed:
        report = run.report
        if report is None:
            continue
        grouped_impacts: dict[str, dict[str, Any]] = {}
        for impact in run.report.impacts if run.report else []:
            if impact.target_type not in _MACRO_TARGET_TYPES or _resembles_security_target(
                impact, security_names, security_symbols
            ):
                continue
            alias_key = (impact.target_type.value, _macro_target_base(impact.target_name))
            stable_asset = (
                impact.asset
                if impact.asset
                and impact.asset.asset_class not in {AssetClass.EQUITY, AssetClass.CRYPTO}
                else aliases.get(alias_key)
            )
            canonical = canonicalize_target(
                impact.target_name,
                impact.target_type,
                asset_id=stable_asset.asset_id if stable_asset else None,
                asset_class=stable_asset.asset_class if stable_asset else None,
            )
            group = grouped_impacts.setdefault(
                canonical.key,
                {"canonical": canonical, "impacts": [], "asset": stable_asset},
            )
            group["impacts"].append(impact)
            if group["asset"] is None and stable_asset is not None:
                group["asset"] = stable_asset

        published_at = event_times.get(run.event_id, as_utc(run.as_of))
        changed_at = as_utc(row.updated_at)
        provisional = (
            row.status == RunStatus.INSUFFICIENT_EVIDENCE.value
            or not report.evidence_complete
        )
        for key, group in grouped_impacts.items():
            impacts = group["impacts"]
            observation = _canonical_observation(
                impacts,
                occurred_at=published_at,
                news_confidence=report.news_confidence or report.fact_confidence,
                provisional=provisional,
            )
            snapshot = {
                "canonical": group["canonical"],
                "impact": _representative_macro_impact(impacts),
                "asset": group["asset"],
                "run": run,
                "changed_at": changed_at,
                "observation": observation,
                "provisional": observation.provisional,
            }
            snapshots_by_key.setdefault(key, []).append(snapshot)

    output: list[dict[str, Any]] = []
    now = utc_now()
    for key, snapshots in snapshots_by_key.items():
        snapshots.sort(
            key=lambda item: (item["changed_at"], item["run"].id.int)
        )
        previous_snapshot: dict[str, Any] | None = None
        latest_change: tuple[dict[str, Any], dict[str, Any]] | None = None
        for snapshot in snapshots:
            if (
                previous_snapshot is not None
                and previous_snapshot["impact"].rating != snapshot["impact"].rating
            ):
                latest_change = (previous_snapshot, snapshot)
            previous_snapshot = snapshot
        if latest_change is None:
            continue
        previous, current = latest_change
        latest = snapshots[-1]
        latest_impact = latest["impact"]
        latest_run = latest["run"]
        canonical: CanonicalTarget = latest["canonical"]
        display_asset = latest["asset"] or latest_impact.asset
        trend = aggregate_target_trend(
            [snapshot["observation"] for snapshot in snapshots], as_of=now
        )
        output.append(
            {
                "kind": "macro",
                "key": key,
                "label": canonical.label,
                "symbol": display_asset.symbol if display_asset else None,
                "market": display_asset.market.value if display_asset else None,
                "target_type": canonical.target_type,
                "changed_at": current["changed_at"],
                "previous": {
                    **_impact_state(previous["impact"]),
                    "provisional": previous["provisional"],
                },
                "current": {
                    **_impact_state(current["impact"]),
                    "provisional": current["provisional"],
                },
                "latest": {
                    **_impact_state(latest_impact),
                    "provisional": latest["provisional"],
                    "news_confidence": (
                        latest_run.report.news_confidence
                        if latest_run.report
                        else None
                    ),
                },
                "trend": _public_target_trend(trend),
                "latest_detail": {
                    "kind": "event",
                    "id": latest_run.id,
                    "researched_at": latest["changed_at"],
                },
                "change_detail_id": current["run"].id,
            }
        )
    return sorted(
        output,
        key=lambda item: (as_utc(item["changed_at"]), UUID(str(item["change_detail_id"])).int),
        reverse=True,
    )


def _asset_target_changes(db: Session) -> list[dict[str, Any]]:
    return [
        {
            "kind": "asset",
            "key": current.asset.asset_id,
            "label": current.asset.name,
            "symbol": current.asset.symbol,
            "market": current.asset.market.value,
            "target_type": TargetType.TRADABLE_ASSET.value,
            "changed_at": current.as_of,
            "previous": {
                "rating": previous.rating.value,
                "direction_score": previous.direction_score,
                "rating_confidence": previous.rating_confidence,
            },
            "current": {
                "rating": current.rating.value,
                "direction_score": current.direction_score,
                "rating_confidence": current.rating_confidence,
            },
            "latest": {
                "rating": latest.rating.value,
                "direction_score": latest.direction_score,
                "rating_confidence": latest.rating_confidence,
                "news_confidence": latest.news_confidence,
            },
            "latest_detail": {
                "kind": "asset",
                "id": latest.id,
                "researched_at": latest.as_of,
            },
            "change_detail_id": current.id,
        }
        for previous, current, latest in _latest_changed_targets(db)
    ]


@router.get("/api/v1/target-changes")
def list_target_changes(
    db: Db,
    kind: Literal["macro", "asset"],
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, Any]:
    changes = _macro_target_changes(db) if kind == "macro" else _asset_target_changes(db)
    if cursor:
        cursor_time, cursor_id = _decode_cursor(cursor)
        changes = [
            item
            for item in changes
            if as_utc(item["changed_at"]) < as_utc(cursor_time)
            or (
                as_utc(item["changed_at"]) == as_utc(cursor_time)
                and UUID(str(item["change_detail_id"])).int < cursor_id.int
            )
        ]
    has_more = len(changes) > limit
    visible = changes[:limit]
    next_cursor = None
    if has_more and visible:
        next_cursor = _encode_cursor(
            visible[-1]["changed_at"], UUID(str(visible[-1]["change_detail_id"]))
        )
    return {"items": visible, "next_cursor": next_cursor}
