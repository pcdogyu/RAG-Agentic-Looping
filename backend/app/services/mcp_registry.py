from __future__ import annotations

import asyncio
import hmac
import json
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID

import httpx2
from cryptography.fernet import Fernet, InvalidToken
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from pydantic import BaseModel, Field, HttpUrl, field_validator
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.db import IntegrationSettingRow, McpSourceRow, SessionLocal

MCP_PURPOSES = {
    "web_search",
    "news_search",
    "asset_search",
    "quote",
    "fundamentals",
    "filings",
}
OUTPUT_ADAPTERS = {
    "search_results_v1",
    "news_items_v1",
    "asset_list_v1",
    "raw_records_v1",
    "filings_v1",
}


class SourceInput(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    url: HttpUrl
    description: str = Field(default="", max_length=1000)
    priority: int = Field(default=50, ge=0, le=1000)
    enabled: bool = True
    auth_type: str = "none"
    auth_header_name: str | None = None
    secret: str | None = None
    clear_secret: bool = False
    tool_mappings: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @field_validator("auth_type")
    @classmethod
    def validate_auth_type(cls, value: str) -> str:
        if value not in {"none", "bearer", "api_key_header"}:
            raise ValueError("auth_type must be none, bearer, or api_key_header")
        return value


class SearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=500)
    source_id: UUID | None = None
    language: str = Field(default="zh-CN", max_length=20)
    time_range: str = Field(default="", max_length=30)
    limit: int = Field(default=10, ge=1, le=20)


class SearchResult(BaseModel):
    title: str
    url: str
    snippet: str
    source: str
    domain: str
    published_at: datetime | None = None


def require_admin_token(provided: str | None, settings: Settings | None = None) -> None:
    expected = (settings or get_settings()).admin_api_token
    if not expected:
        raise RuntimeError("ADMIN_API_TOKEN is not configured")
    if not provided or not hmac.compare_digest(provided.encode(), expected.encode()):
        raise PermissionError("invalid administrator token")


def _fernet(settings: Settings) -> Fernet:
    if not settings.mcp_secret_key:
        raise RuntimeError("MCP_SECRET_KEY is not configured")
    try:
        return Fernet(settings.mcp_secret_key.encode())
    except (ValueError, TypeError) as exc:
        raise RuntimeError("MCP_SECRET_KEY is not a valid Fernet key") from exc


def encrypt_secret(secret: str, settings: Settings | None = None) -> str:
    return _fernet(settings or get_settings()).encrypt(secret.encode()).decode()


def decrypt_secret(ciphertext: str, settings: Settings | None = None) -> str:
    try:
        return _fernet(settings or get_settings()).decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        raise RuntimeError("stored MCP credential cannot be decrypted") from exc


def source_public(row: McpSourceRow) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "name": row.name,
        "url": row.url,
        "description": row.description,
        "priority": row.priority,
        "enabled": row.enabled,
        "managed": row.managed,
        "auth_type": row.auth_type,
        "auth_header_name": row.auth_header_name,
        "secret_configured": bool(row.encrypted_secret),
        "discovered_tools": row.discovered_tools or [],
        "tool_mappings": row.tool_mappings or {},
        "last_status": row.last_status,
        "last_error": row.last_error,
        "last_checked_at": row.last_checked_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def validate_mappings(mappings: dict[str, dict[str, Any]], tools: list[dict[str, Any]]) -> None:
    names = {item.get("name") for item in tools}
    tool_by_name = {item.get("name"): item for item in tools}
    for purpose, mapping in mappings.items():
        if purpose not in MCP_PURPOSES:
            raise ValueError(f"unsupported MCP purpose: {purpose}")
        tool_name = mapping.get("tool_name")
        if not tool_name:
            raise ValueError(f"{purpose} mapping requires tool_name")
        if tools and tool_name not in names:
            raise ValueError(f"mapped tool was not discovered: {tool_name}")
        adapter = mapping.get("output_adapter", "raw_records_v1")
        if adapter not in OUTPUT_ADAPTERS:
            raise ValueError(f"unsupported output adapter: {adapter}")
        bindings = mapping.get("input_bindings", {})
        if not isinstance(bindings, dict) or not isinstance(mapping.get("defaults", {}), dict):
            raise ValueError("input_bindings and defaults must be objects")
        tool = tool_by_name.get(tool_name)
        if tool:
            schema = tool.get("input_schema") or {}
            properties = set(schema.get("properties", {}))
            unknown = (set(bindings.values()) | set(mapping.get("defaults", {}))) - properties
            if unknown:
                raise ValueError(
                    f"mapping references arguments absent from {tool_name} schema: {sorted(unknown)}"
                )


def seed_integrations(db: Session, settings: Settings | None = None) -> None:
    cfg = settings or get_settings()
    if not db.scalar(select(McpSourceRow).where(McpSourceRow.name == "SearXNG")):
        db.add(
            McpSourceRow(
                name="SearXNG",
                url=cfg.searxng_mcp_url,
                description="内置联网验证搜索服务",
                priority=50,
                enabled=True,
                managed=True,
                tool_mappings={
                    "web_search": {
                        "tool_name": "web_search",
                        "input_bindings": {
                            "query": "query",
                            "limit": "limit",
                            "language": "language",
                            "time_range": "time_range",
                        },
                        "defaults": {},
                        "output_adapter": "search_results_v1",
                    }
                },
            )
        )
    if not db.scalar(select(McpSourceRow).where(McpSourceRow.name == "FMP")):
        db.add(
            McpSourceRow(
                name="FMP",
                url=cfg.fmp_mcp_url or "http://fmp-mcp:8000/mcp",
                description="内置受管的 Financial Modeling Prep 来源",
                priority=100,
                enabled=cfg.fmp_enabled,
                managed=True,
            )
        )
    if not db.get(IntegrationSettingRow, "weknora"):
        db.add(IntegrationSettingRow(key="weknora", payload={"url": cfg.weknora_default_url}))
    db.commit()


def _headers(row: McpSourceRow, settings: Settings) -> dict[str, str]:
    if row.auth_type == "none" or not row.encrypted_secret:
        return {}
    secret = decrypt_secret(row.encrypted_secret, settings)
    if row.auth_type == "bearer":
        return {"Authorization": f"Bearer {secret}"}
    return {row.auth_header_name or "X-API-Key": secret}


async def discover_source(
    row: McpSourceRow, settings: Settings | None = None
) -> list[dict[str, Any]]:
    cfg = settings or get_settings()
    async with httpx2.AsyncClient(
        headers=_headers(row, cfg),
        timeout=httpx2.Timeout(cfg.web_search_timeout_seconds),
        follow_redirects=True,
    ) as client:
        transport = streamable_http_client(row.url, http_client=client)
        async with Client(transport) as session:
            response = await session.list_tools()
    return [
        {
            "name": tool.name,
            "description": tool.description or "",
            "input_schema": tool.input_schema,
            "output_schema": getattr(tool, "output_schema", None),
        }
        for tool in response.tools
    ]


def _extract_payload(result: Any) -> Any:
    structured = getattr(result, "structured_content", None)
    if structured is not None:
        return structured
    values: list[Any] = []
    for item in getattr(result, "content", []):
        text = getattr(item, "text", None)
        if text is None:
            continue
        try:
            values.append(json.loads(text))
        except json.JSONDecodeError:
            values.append(text)
    if len(values) == 1:
        return values[0]
    return values


async def call_source_tool(
    row: McpSourceRow,
    purpose: str,
    canonical_args: dict[str, Any],
    settings: Settings | None = None,
) -> Any:
    cfg = settings or get_settings()
    mapping = (row.tool_mappings or {}).get(purpose)
    if not mapping:
        raise ValueError(f"source has no {purpose} mapping")
    arguments = dict(mapping.get("defaults", {}))
    for canonical, target in mapping.get("input_bindings", {}).items():
        if canonical in canonical_args and canonical_args[canonical] not in (None, ""):
            arguments[target] = canonical_args[canonical]
    async with httpx2.AsyncClient(
        headers=_headers(row, cfg),
        timeout=httpx2.Timeout(cfg.web_search_timeout_seconds),
        follow_redirects=True,
    ) as client:
        transport = streamable_http_client(row.url, http_client=client)
        async with Client(transport) as session:
            result = await session.call_tool(mapping["tool_name"], arguments)
    if getattr(result, "is_error", False):
        raise RuntimeError("MCP tool returned an error")
    return _extract_payload(result)


def _canonical_url(value: str) -> str:
    split = urlsplit(value.strip())
    kept = [(k, v) for k, v in parse_qsl(split.query) if not k.lower().startswith("utm_")]
    return urlunsplit(
        (split.scheme.lower(), split.netloc.lower(), split.path.rstrip("/"), urlencode(kept), "")
    )


def normalize_search_results(payload: Any, source: str) -> list[SearchResult]:
    if isinstance(payload, dict):
        items = payload.get("results") or payload.get("items") or payload.get("data") or []
    else:
        items = payload
    if isinstance(items, dict):
        items = [items]
    normalized: list[SearchResult] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or item.get("link") or "").strip()
        snippet = str(
            item.get("snippet") or item.get("content") or item.get("summary") or ""
        ).strip()
        parsed = urlsplit(url)
        if not title or not snippet or parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        published = item.get("published_at") or item.get("publishedDate") or item.get("date")
        try:
            published_at = (
                datetime.fromisoformat(str(published).replace("Z", "+00:00")) if published else None
            )
        except ValueError:
            published_at = None
        normalized.append(
            SearchResult(
                title=title,
                url=_canonical_url(url),
                snippet=snippet[:2000],
                source=source,
                domain=parsed.netloc.lower(),
                published_at=published_at,
            )
        )
    return normalized


async def search_enabled_sources(
    request: SearchRequest,
) -> tuple[list[SearchResult], list[dict[str, str]]]:
    with SessionLocal() as db:
        query = select(McpSourceRow).where(McpSourceRow.enabled.is_(True))
        if request.source_id:
            query = query.where(McpSourceRow.id == request.source_id)
        rows = list(db.scalars(query.order_by(desc(McpSourceRow.priority))).all())
    rows = [row for row in rows if {"web_search", "news_search"} & set(row.tool_mappings or {})]
    results: list[SearchResult] = []
    errors: list[dict[str, str]] = []
    for row in rows:
        try:
            purpose = "web_search" if "web_search" in (row.tool_mappings or {}) else "news_search"
            payload = await call_source_tool(row, purpose, request.model_dump())
            results.extend(normalize_search_results(payload, row.name)[: request.limit])
        except Exception as exc:
            errors.append({"source": row.name, "error": f"{type(exc).__name__}: {exc}"[:500]})
    deduped: list[SearchResult] = []
    seen: set[str] = set()
    for item in results:
        if item.url in seen:
            continue
        seen.add(item.url)
        deduped.append(item)
        if len(deduped) >= request.limit:
            break
    return deduped, errors


def search_enabled_sources_sync(
    request: SearchRequest,
) -> tuple[list[SearchResult], list[dict[str, str]]]:
    return asyncio.run(search_enabled_sources(request))


async def call_enabled_purpose(
    purpose: str, canonical_args: dict[str, Any]
) -> tuple[list[tuple[str, Any]], list[dict[str, str]]]:
    if purpose not in MCP_PURPOSES:
        raise ValueError(f"unsupported MCP purpose: {purpose}")
    with SessionLocal() as db:
        rows = list(
            db.scalars(
                select(McpSourceRow)
                .where(McpSourceRow.enabled.is_(True))
                .order_by(desc(McpSourceRow.priority))
            ).all()
        )
    rows = [row for row in rows if purpose in (row.tool_mappings or {})]
    values: list[tuple[str, Any]] = []
    errors: list[dict[str, str]] = []
    for row in rows:
        try:
            values.append((row.name, await call_source_tool(row, purpose, canonical_args)))
            if purpose == "quote":
                break
        except Exception as exc:
            errors.append({"source": row.name, "error": f"{type(exc).__name__}: {exc}"[:500]})
    return values, errors


def call_enabled_purpose_sync(
    purpose: str, canonical_args: dict[str, Any]
) -> tuple[list[tuple[str, Any]], list[dict[str, str]]]:
    return asyncio.run(call_enabled_purpose(purpose, canonical_args))
