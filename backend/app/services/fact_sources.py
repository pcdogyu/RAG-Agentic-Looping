from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, HttpUrl
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.db import IntegrationSettingRow, SessionLocal
from backend.app.services.secret_store import decrypt_secret, encrypt_secret

FACT_SOURCE_GROUPS = (
    {
        "id": "fmp",
        "badge": "US",
        "name": "FMP 美股数据",
        "description": "美股行情、财务报表、估值指标与公司基础数据",
        "tone": "amber",
    },
    {
        "id": "sec",
        "badge": "OFFICIAL",
        "name": "SEC 官方文件",
        "description": "SEC EDGAR 监管文件与公司申报记录",
        "tone": "cyan",
    },
    {
        "id": "cn_news",
        "badge": "CN / NEWS",
        "name": "A股与新闻",
        "description": "AkShare 主数据、市场新闻、公告与 RSS 事实来源",
        "tone": "amber",
    },
    {
        "id": "crypto",
        "badge": "CRYPTO",
        "name": "数字资产",
        "description": "CoinGecko、DeFiLlama 与 CCXT Kraken 交叉验证",
        "tone": "cyan",
    },
    {
        "id": "search",
        "badge": "WEB / SEARCH",
        "name": "网络搜索与交叉验证",
        "description": "跨市场网页搜索、独立来源验证与实时补充证据",
        "tone": "mint",
    },
)
OTHER_GROUP = {
    "id": "other",
    "badge": "OTHER",
    "name": "其他数据源",
    "description": "尚未归入固定事实领域的自定义 MCP 来源",
    "tone": "neutral",
}
FACT_SOURCE_GROUP_IDS = {item["id"] for item in FACT_SOURCE_GROUPS} | {"other"}
BUILTIN_SOURCE_GROUPS = {"FMP": "fmp", "SearXNG": "search", "DuckDuckGo": "search"}


class StrictConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FmpConfigInput(StrictConfig):
    base_url: HttpUrl
    access_token: str | None = Field(default=None, max_length=1000)
    clear_access_token: bool = False
    rate_limit_per_minute: int = Field(ge=1, le=300)
    news_lookback_hours: int = Field(ge=1, le=168)


class SecConfigInput(StrictConfig):
    identity: str = Field(default="", max_length=500)


class CnNewsConfigInput(StrictConfig):
    akshare_asset_master_enabled: bool
    akshare_ipv4_only: bool
    rss_feed_urls: list[HttpUrl] = Field(default_factory=list, max_length=50)
    official_rss_feed_urls: list[HttpUrl] = Field(default_factory=list, max_length=50)


class CryptoConfigInput(StrictConfig):
    coingecko_base_url: HttpUrl
    defillama_base_url: HttpUrl


class SearchConfigInput(StrictConfig):
    timeout_seconds: int = Field(ge=2, le=120)


GROUP_INPUTS = {
    "fmp": FmpConfigInput,
    "sec": SecConfigInput,
    "cn_news": CnNewsConfigInput,
    "crypto": CryptoConfigInput,
    "search": SearchConfigInput,
}


def _config_key(group_id: str) -> str:
    return f"fact-source:{group_id}"


def _membership_key(source_id: UUID | str) -> str:
    return f"mcp-source-group:{source_id}"


def validate_group_id(group_id: str) -> str:
    if group_id not in FACT_SOURCE_GROUP_IDS:
        raise ValueError(f"unsupported fact source group: {group_id}")
    return group_id


def source_group_id(db: Session, source_id: UUID | str, source_name: str) -> str:
    row = db.get(IntegrationSettingRow, _membership_key(source_id))
    if row and row.payload.get("group_id") in FACT_SOURCE_GROUP_IDS:
        return str(row.payload["group_id"])
    return BUILTIN_SOURCE_GROUPS.get(source_name, "other")


def set_source_group(db: Session, source_id: UUID | str, group_id: str) -> None:
    validate_group_id(group_id)
    key = _membership_key(source_id)
    row = db.get(IntegrationSettingRow, key) or IntegrationSettingRow(key=key)
    row.payload = {"group_id": group_id}
    db.add(row)


def delete_source_group(db: Session, source_id: UUID | str) -> None:
    row = db.get(IntegrationSettingRow, _membership_key(source_id))
    if row:
        db.delete(row)


def ensure_builtin_source_group(db: Session, source_id: UUID | str, source_name: str) -> None:
    group_id = BUILTIN_SOURCE_GROUPS.get(source_name)
    if group_id:
        set_source_group(db, source_id, group_id)


def _read_payload(db: Session, group_id: str) -> dict[str, Any]:
    row = db.get(IntegrationSettingRow, _config_key(group_id))
    return dict(row.payload) if row else {}


def _effective_values(base: Settings, db: Session) -> tuple[dict[str, Any], dict[str, str]]:
    values: dict[str, Any] = {}
    sources: dict[str, str] = {}
    for group_id in GROUP_INPUTS:
        payload = _read_payload(db, group_id)
        if not payload:
            sources[group_id] = "environment"
            continue
        sources[group_id] = "database"
        if group_id == "fmp":
            values.update(
                fmp_base_url=payload.get("base_url", base.fmp_base_url),
                fmp_rate_limit_per_minute=payload.get(
                    "rate_limit_per_minute", base.fmp_rate_limit_per_minute
                ),
                fmp_news_lookback_hours=payload.get(
                    "news_lookback_hours", base.fmp_news_lookback_hours
                ),
            )
            if payload.get("access_token_disabled"):
                values["fmp_access_token"] = ""
            elif payload.get("encrypted_access_token"):
                values["fmp_access_token"] = decrypt_secret(
                    payload["encrypted_access_token"], base
                )
        elif group_id == "sec":
            values["sec_identity"] = payload.get("identity", base.sec_identity)
        elif group_id == "cn_news":
            values.update(
                akshare_asset_master_enabled=payload.get(
                    "akshare_asset_master_enabled", base.akshare_asset_master_enabled
                ),
                akshare_ipv4_only=payload.get("akshare_ipv4_only", base.akshare_ipv4_only),
                rss_feed_urls=",".join(payload.get("rss_feed_urls", base.rss_feeds)),
                official_rss_feed_urls=",".join(
                    payload.get("official_rss_feed_urls", base.official_rss_feeds)
                ),
            )
        elif group_id == "crypto":
            values.update(
                coingecko_base_url=payload.get("coingecko_base_url", base.coingecko_base_url),
                defillama_base_url=payload.get("defillama_base_url", base.defillama_base_url),
            )
        elif group_id == "search":
            values["web_search_timeout_seconds"] = payload.get(
                "timeout_seconds", base.web_search_timeout_seconds
            )
    return values, sources


def get_effective_settings(
    *, db: Session | None = None, base: Settings | None = None
) -> Settings:
    active_base = base or get_settings()
    try:
        if db is not None:
            values, _ = _effective_values(active_base, db)
        else:
            with SessionLocal() as session:
                values, _ = _effective_values(active_base, session)
    except Exception:
        return active_base
    return Settings(**{**active_base.model_dump(), **values})


def _safe_config(group_id: str, settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    if group_id == "fmp":
        if payload.get("access_token_disabled"):
            token_source = "disabled"
            token_configured = False
        elif payload.get("encrypted_access_token"):
            token_source = "database"
            token_configured = True
        else:
            token_source = "environment" if settings.fmp_access_token else "unconfigured"
            token_configured = bool(settings.fmp_access_token)
        return {
            "base_url": settings.fmp_base_url,
            "rate_limit_per_minute": settings.fmp_rate_limit_per_minute,
            "news_lookback_hours": settings.fmp_news_lookback_hours,
            "access_token_configured": token_configured,
            "access_token_source": token_source,
            "mcp_upstream_token_management": "environment",
        }
    if group_id == "sec":
        return {"identity": settings.sec_identity}
    if group_id == "cn_news":
        return {
            "akshare_asset_master_enabled": settings.akshare_asset_master_enabled,
            "akshare_ipv4_only": settings.akshare_ipv4_only,
            "rss_feed_urls": settings.rss_feeds,
            "official_rss_feed_urls": settings.official_rss_feeds,
        }
    if group_id == "crypto":
        return {
            "coingecko_base_url": settings.coingecko_base_url,
            "defillama_base_url": settings.defillama_base_url,
            "ccxt_exchange": "kraken",
        }
    if group_id == "search":
        return {"timeout_seconds": settings.web_search_timeout_seconds}
    return {}


def native_group_config(db: Session, group_id: str, base: Settings | None = None) -> dict[str, Any]:
    validate_group_id(group_id)
    active_base = base or get_settings()
    payload = _read_payload(db, group_id) if group_id != "other" else {}
    settings = get_effective_settings(db=db, base=active_base)
    return {
        "config": _safe_config(group_id, settings, payload),
        "config_source": "database" if payload else "environment",
    }


def save_native_group_config(
    db: Session, group_id: str, raw_payload: dict[str, Any], base: Settings | None = None
) -> dict[str, Any]:
    validate_group_id(group_id)
    model = GROUP_INPUTS.get(group_id)
    if model is None:
        raise ValueError("the other group has no native configuration")
    active_base = base or get_settings()
    data = model.model_validate(raw_payload).model_dump(mode="json")
    current = _read_payload(db, group_id)
    if group_id == "fmp":
        access_token = data.pop("access_token", None)
        clear_access_token = data.pop("clear_access_token", False)
        if access_token:
            data["encrypted_access_token"] = encrypt_secret(access_token, active_base)
            data["access_token_disabled"] = False
        elif clear_access_token:
            data["encrypted_access_token"] = ""
            data["access_token_disabled"] = True
        else:
            for key in ("encrypted_access_token", "access_token_disabled"):
                if key in current:
                    data[key] = current[key]
    row = db.get(IntegrationSettingRow, _config_key(group_id)) or IntegrationSettingRow(
        key=_config_key(group_id)
    )
    row.payload = data
    db.add(row)
    db.commit()
    return native_group_config(db, group_id, active_base)


def reset_native_group_config(db: Session, group_id: str, base: Settings | None = None) -> dict[str, Any]:
    validate_group_id(group_id)
    if group_id == "other":
        raise ValueError("the other group has no native configuration")
    row = db.get(IntegrationSettingRow, _config_key(group_id))
    if row:
        db.delete(row)
        db.commit()
    return native_group_config(db, group_id, base)


async def probe_native_group(group_id: str, settings: Settings) -> dict[str, Any]:
    validate_group_id(group_id)
    timeout = min(settings.web_search_timeout_seconds, 20)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            if group_id == "fmp":
                if not settings.fmp_access_token:
                    return {"ok": False, "status": "pending", "detail": "FMP REST Token 未配置"}
                response = await client.get(
                    f"{settings.fmp_base_url.rstrip('/')}/quote",
                    params={"symbol": "AAPL"},
                    headers={"apikey": settings.fmp_access_token},
                )
            elif group_id == "sec":
                if not settings.sec_identity:
                    return {"ok": False, "status": "pending", "detail": "SEC Identity 未配置"}
                response = await client.get(
                    "https://data.sec.gov/submissions/CIK0000320193.json",
                    headers={"User-Agent": settings.sec_identity},
                )
            elif group_id == "crypto":
                responses = await asyncio.gather(
                    client.get(f"{settings.coingecko_base_url.rstrip('/')}/ping"),
                    client.get(f"{settings.defillama_base_url.rstrip('/')}/protocols"),
                )
                for item in responses:
                    item.raise_for_status()
                return {
                    "ok": True,
                    "status": "healthy",
                    "status_codes": [item.status_code for item in responses],
                }
            elif group_id == "cn_news":
                urls = [*settings.official_rss_feeds, *settings.rss_feeds]
                if not urls:
                    return {
                        "ok": settings.akshare_asset_master_enabled,
                        "status": "healthy" if settings.akshare_asset_master_enabled else "pending",
                        "detail": "AkShare 已启用" if settings.akshare_asset_master_enabled else "未配置 RSS",
                    }
                response = await client.get(urls[0])
            else:
                return {"ok": True, "status": "healthy", "detail": "使用所属 MCP 执行连接测试"}
        response.raise_for_status()
        return {"ok": True, "status": "healthy", "status_code": response.status_code}
    except Exception as exc:
        return {"ok": False, "status": "failed", "detail": f"{type(exc).__name__}: {exc}"[:500]}
