from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import delete, desc, func, select
from sqlalchemy.orm import Session

from backend.app.db import IntegrationSettingRow, NewsFilterLogRow
from backend.app.domain import NewsItem, as_utc, utc_now

SOURCE_FILTER_KEY = "source-filter"
DEFAULT_BLACKLIST = ["天气"]
WHITELIST_MISS_REASON = "未命中白名单"
FILTER_LOG_RETENTION_DAYS = 30
FILTER_LOG_MAX_ROWS = 5000


def _normalize_keyword(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip()


def _match_text(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


class SourceFilterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    whitelist_keywords: list[str] = Field(default_factory=list, max_length=200)
    blacklist_keywords: list[str] = Field(
        default_factory=lambda: list(DEFAULT_BLACKLIST), max_length=200
    )

    @field_validator("whitelist_keywords", "blacklist_keywords")
    @classmethod
    def normalize_keywords(cls, values: list[str]) -> list[str]:
        output: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized = _normalize_keyword(value)
            if not normalized:
                continue
            if len(normalized) > 80:
                raise ValueError("keywords must not exceed 80 characters")
            key = normalized.casefold()
            if key not in seen:
                seen.add(key)
                output.append(normalized)
        return output


@dataclass(frozen=True)
class FilterDecision:
    allowed: bool
    matched_keyword: str | None = None


def get_source_filter(db: Session) -> SourceFilterConfig:
    row = db.get(IntegrationSettingRow, SOURCE_FILTER_KEY)
    if not row:
        return SourceFilterConfig()
    try:
        return SourceFilterConfig.model_validate(row.payload)
    except ValueError:
        return SourceFilterConfig()


def save_source_filter(db: Session, config: SourceFilterConfig) -> SourceFilterConfig:
    row = db.get(IntegrationSettingRow, SOURCE_FILTER_KEY) or IntegrationSettingRow(
        key=SOURCE_FILTER_KEY
    )
    row.payload = config.model_dump(mode="json")
    row.updated_at = utc_now()
    db.add(row)
    db.commit()
    return config


def reset_source_filter(db: Session) -> SourceFilterConfig:
    row = db.get(IntegrationSettingRow, SOURCE_FILTER_KEY)
    if row:
        db.delete(row)
        db.commit()
    return SourceFilterConfig()


def evaluate_title(title: str, config: SourceFilterConfig) -> FilterDecision:
    if not config.enabled:
        return FilterDecision(allowed=True)
    candidate = _match_text(title)
    whitelist_match: str | None = None
    for keyword in config.whitelist_keywords:
        if _match_text(keyword) in candidate:
            whitelist_match = keyword
            break
    for keyword in config.blacklist_keywords:
        if _match_text(keyword) in candidate:
            return FilterDecision(allowed=False, matched_keyword=keyword)
    if whitelist_match is not None:
        return FilterDecision(allowed=True, matched_keyword=whitelist_match)
    return FilterDecision(allowed=False, matched_keyword=WHITELIST_MISS_REASON)


def _record_filtered(db: Session, item: NewsItem, matched_keyword: str) -> None:
    now = utc_now()
    row = db.scalar(
        select(NewsFilterLogRow).where(NewsFilterLogRow.content_hash == item.content_hash)
    )
    if row:
        row.last_filtered_at = now
        row.hit_count += 1
        row.matched_keyword = matched_keyword
        return
    db.add(
        NewsFilterLogRow(
            content_hash=item.content_hash,
            source=item.source,
            title=item.title,
            url=item.url,
            matched_keyword=matched_keyword,
            published_at=item.published_at,
            first_filtered_at=now,
            last_filtered_at=now,
        )
    )


def _prune_filter_logs(db: Session) -> None:
    cutoff = utc_now() - timedelta(days=FILTER_LOG_RETENTION_DAYS)
    db.execute(delete(NewsFilterLogRow).where(NewsFilterLogRow.last_filtered_at < cutoff))
    db.flush()
    overflow_ids = list(
        db.scalars(
            select(NewsFilterLogRow.id)
            .order_by(desc(NewsFilterLogRow.last_filtered_at), desc(NewsFilterLogRow.id))
            .offset(FILTER_LOG_MAX_ROWS)
        ).all()
    )
    if overflow_ids:
        db.execute(delete(NewsFilterLogRow).where(NewsFilterLogRow.id.in_(overflow_ids)))


def filter_news_items(db: Session, items: list[NewsItem]) -> tuple[list[NewsItem], int]:
    config = get_source_filter(db)
    accepted: list[NewsItem] = []
    filtered = 0
    for item in items:
        decision = evaluate_title(item.title, config)
        if decision.allowed:
            accepted.append(item)
            continue
        filtered += 1
        _record_filtered(db, item, decision.matched_keyword or "")
    _prune_filter_logs(db)
    db.commit()
    return accepted, filtered


def _log_payload(row: NewsFilterLogRow) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "source": row.source,
        "title": row.title,
        "url": row.url,
        "matched_keyword": row.matched_keyword,
        "published_at": as_utc(row.published_at),
        "first_filtered_at": as_utc(row.first_filtered_at),
        "last_filtered_at": as_utc(row.last_filtered_at),
        "hit_count": row.hit_count,
        "rescan_allowed": row.matched_keyword == WHITELIST_MISS_REASON,
    }


def list_filter_logs(db: Session, limit: int = 100) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(NewsFilterLogRow)
        .order_by(desc(NewsFilterLogRow.last_filtered_at), desc(NewsFilterLogRow.id))
        .limit(limit)
    ).all()
    return [_log_payload(row) for row in rows]


def source_filter_payload(db: Session) -> dict[str, Any]:
    config = get_source_filter(db)
    row = db.get(IntegrationSettingRow, SOURCE_FILTER_KEY)
    total = db.scalar(select(func.count()).select_from(NewsFilterLogRow)) or 0
    last_filtered_at = db.scalar(select(func.max(NewsFilterLogRow.last_filtered_at)))
    return {
        **config.model_dump(mode="json"),
        "retained_log_count": total,
        "last_filtered_at": as_utc(last_filtered_at) if last_filtered_at else None,
        "updated_at": as_utc(row.updated_at) if row else None,
    }
