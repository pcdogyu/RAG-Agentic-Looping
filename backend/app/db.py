from __future__ import annotations

from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.types import TypeDecorator

from backend.app.config import get_settings
from backend.app.domain import utc_now


class GUID(TypeDecorator):
    impl = String(36)
    cache_ok = True

    def process_bind_param(self, value: UUID | str | None, dialect: Any) -> str | None:
        return str(value) if value is not None else None

    def process_result_value(self, value: str | None, dialect: Any) -> UUID | None:
        return UUID(value) if value is not None else None


class Base(DeclarativeBase):
    pass


class AssetRow(Base):
    __tablename__ = "assets"

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    asset_class: Mapped[str] = mapped_column(String(20), index=True)
    market: Mapped[str] = mapped_column(String(20), index=True)
    symbol: Mapped[str] = mapped_column(String(40), index=True)
    name: Mapped[str] = mapped_column(String(200), index=True)
    exchange_or_provider: Mapped[str] = mapped_column(String(80))
    currency: Mapped[str] = mapped_column(String(10), default="USD")
    aliases: Mapped[list[str]] = mapped_column(JSON, default=list)
    products: Mapped[list[str]] = mapped_column(JSON, default=list)
    competitors: Mapped[list[str]] = mapped_column(JSON, default=list)
    sector_id: Mapped[str] = mapped_column(String(120), default="", index=True)
    industry_id: Mapped[str] = mapped_column(String(160), default="", index=True)
    raw_sector: Mapped[str] = mapped_column(String(160), default="")
    raw_industry: Mapped[str] = mapped_column(String(200), default="")
    instrument_type: Mapped[str] = mapped_column(String(40), default="", index=True)
    market_cap: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)
    market_cap_rank: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    manual_industry_id: Mapped[str | None] = mapped_column(
        String(160), nullable=True, index=True
    )
    manual_active: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    issuer_id: Mapped[str | None] = mapped_column(String(240), nullable=True)
    primary_listing_asset_id: Mapped[str | None] = mapped_column(
        String(160), nullable=True
    )
    lot_size: Mapped[int] = mapped_column(Integer, default=1)
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class IndustryRow(Base):
    __tablename__ = "industries"

    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    parent_id: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    level: Mapped[int] = mapped_column(Integer, index=True)
    name_zh: Mapped[str] = mapped_column(String(120), index=True)
    name_en: Mapped[str] = mapped_column(String(160), index=True)
    aliases: Mapped[list[str]] = mapped_column(JSON, default=list)
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class AssetUniverseSyncRow(Base):
    __tablename__ = "asset_universe_sync"

    market: Mapped[str] = mapped_column(String(20), primary_key=True)
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    asset_count: Mapped[int] = mapped_column(Integer, default=0)
    industry_count: Mapped[int] = mapped_column(Integer, default=0)
    added_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, default=0)
    deactivated_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class NewsRow(Base):
    __tablename__ = "news_items"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(120), index=True)
    source_quality: Mapped[str] = mapped_column(String(30))
    title: Mapped[str] = mapped_column(Text)
    summary: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(Text)
    language: Mapped[str] = mapped_column(String(10), default="en")
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    content_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    symbols: Mapped[list[str]] = mapped_column(JSON, default=list)
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class NewsSourceStateRow(Base):
    """Durable discovery watermark and health for one visible news source."""

    __tablename__ = "news_source_states"

    source: Mapped[str] = mapped_column(String(120), primary_key=True)
    provider: Mapped[str] = mapped_column(String(80), default="")
    status: Mapped[str] = mapped_column(String(30), default="unchecked", index=True)
    watermark_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_discovered_count: Mapped[int] = mapped_column(Integer, default=0)
    last_new_count: Mapped[int] = mapped_column(Integer, default=0)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, index=True
    )


class NewsProcessingRow(Base):
    """Durable lifecycle for one news item's extraction work."""

    __tablename__ = "news_processing"

    news_id: Mapped[UUID] = mapped_column(GUID(), primary_key=True)
    status: Mapped[str] = mapped_column(String(40), index=True)
    scan_task_id: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    celery_task_id: Mapped[str | None] = mapped_column(
        String(160), nullable=True, index=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    queued_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, index=True
    )


class NewsProcessingOutboxRow(Base):
    """Transactional intent to publish one standalone extraction task."""

    __tablename__ = "news_processing_outbox"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    news_id: Mapped[UUID] = mapped_column(GUID(), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(40), index=True, default="pending")
    force_asset_mapping: Mapped[bool] = mapped_column(Boolean, default=False)
    dispatch_attempts: Mapped[int] = mapped_column(Integer, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, index=True
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, index=True
    )


class NewsFilterLogRow(Base):
    __tablename__ = "news_filter_logs"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    content_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    source: Mapped[str] = mapped_column(String(120), index=True)
    title: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    matched_keyword: Mapped[str] = mapped_column(String(80), index=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    first_filtered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now
    )
    last_filtered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, index=True
    )
    hit_count: Mapped[int] = mapped_column(Integer, default=1)


class EventRow(Base):
    __tablename__ = "news_events"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    headline: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(String(40), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    priority: Mapped[float] = mapped_column(Float, default=0.5, index=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ResearchRunRow(Base):
    __tablename__ = "research_runs"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    event_id: Mapped[UUID | None] = mapped_column(GUID(), nullable=True, index=True)
    asset_id: Mapped[str] = mapped_column(String(160), index=True)
    status: Mapped[str] = mapped_column(String(40), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class EventResearchRunRow(Base):
    __tablename__ = "event_research_runs"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(GUID(), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(40), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class EvidenceRow(Base):
    __tablename__ = "evidence"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), index=True)
    claim: Mapped[str] = mapped_column(Text)
    source_url: Mapped[str] = mapped_column(Text)
    source_quality: Mapped[str] = mapped_column(String(30), index=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class DocumentChunkRow(Base):
    """Searchable evidence projection; original documents remain at their source URL."""

    __tablename__ = "document_chunks"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    evidence_id: Mapped[UUID] = mapped_column(GUID(), unique=True, index=True)
    run_id: Mapped[UUID] = mapped_column(GUID(), index=True)
    asset_id: Mapped[str] = mapped_column(String(160), index=True)
    text: Mapped[str] = mapped_column(Text)
    terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    embedding: Mapped[list[float]] = mapped_column(Vector(384).with_variant(JSON, "sqlite"))
    source_url: Mapped[str] = mapped_column(Text)
    source_quality: Mapped[str] = mapped_column(String(30), index=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class RecommendationRow(Base):
    __tablename__ = "recommendations"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), unique=True, index=True)
    asset_id: Mapped[str] = mapped_column(String(160), index=True)
    score: Mapped[int] = mapped_column(Integer)
    rating: Mapped[str] = mapped_column(String(40), index=True)
    confidence: Mapped[float] = mapped_column(Float)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class PaperOrderRow(Base):
    __tablename__ = "paper_orders"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    recommendation_id: Mapped[UUID] = mapped_column(GUID(), index=True)
    asset_id: Mapped[str] = mapped_column(String(160), index=True)
    side: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(10))
    fee: Mapped[float] = mapped_column(Float)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class OutcomeRow(Base):
    __tablename__ = "outcomes"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    recommendation_id: Mapped[UUID] = mapped_column(GUID(), index=True)
    horizon_days: Mapped[int] = mapped_column(Integer, index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class EvolutionRow(Base):
    __tablename__ = "evolution_candidates"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    branch: Mapped[str] = mapped_column(String(200), unique=True)
    status: Mapped[str] = mapped_column(String(30), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ModelCallAuditRow(Base):
    __tablename__ = "model_call_audits"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    logical_call_id: Mapped[UUID] = mapped_column(GUID(), index=True)
    source_key: Mapped[str | None] = mapped_column(String(500), unique=True, nullable=True)
    provider: Mapped[str] = mapped_column(String(40), index=True)
    model: Mapped[str] = mapped_column(String(160), index=True)
    operation: Mapped[str] = mapped_column(String(80), index=True)
    entity_type: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    entity_id: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(30), index=True)
    fidelity: Mapped[str] = mapped_column(String(30), default="exact", index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_language: Mapped[str] = mapped_column(String(20), default="other", index=True)
    output_language: Mapped[str] = mapped_column(String(20), default="other", index=True)
    messages: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    schema_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    raw_response: Mapped[str] = mapped_column(Text, default="")
    parsed_response: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class McpSourceRow(Base):
    __tablename__ = "mcp_sources"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text, default="")
    priority: Mapped[int] = mapped_column(Integer, default=50, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    managed: Mapped[bool] = mapped_column(Boolean, default=False)
    auth_type: Mapped[str] = mapped_column(String(30), default="none")
    auth_header_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    encrypted_secret: Mapped[str | None] = mapped_column(Text, nullable=True)
    discovered_tools: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    tool_mappings: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    last_status: Mapped[str] = mapped_column(String(30), default="unchecked")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class IntegrationSettingRow(Base):
    __tablename__ = "integration_settings"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


def _make_engine():
    settings = get_settings()
    if settings.database_url.startswith("sqlite"):
        Path("data").mkdir(exist_ok=True)
        return create_engine(
            settings.database_url,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
        )
    return create_engine(settings.database_url, pool_pre_ping=True, pool_recycle=1800)


engine = _make_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


_ASSET_IDENTITY_COLUMNS = {
    "issuer_id": "VARCHAR(240)",
    "primary_listing_asset_id": "VARCHAR(160)",
    "sector_id": "VARCHAR(120) DEFAULT ''",
    "industry_id": "VARCHAR(160) DEFAULT ''",
    "raw_sector": "VARCHAR(160) DEFAULT ''",
    "raw_industry": "VARCHAR(200) DEFAULT ''",
    "instrument_type": "VARCHAR(40) DEFAULT ''",
    "market_cap": "DOUBLE PRECISION",
    "market_cap_rank": "INTEGER",
    "last_synced_at": "TIMESTAMP WITH TIME ZONE",
    "manual_industry_id": "VARCHAR(160)",
    "manual_active": "BOOLEAN",
}


def _asset_column_names(bind: Engine) -> set[str]:
    inspector = inspect(bind)
    if "assets" not in inspector.get_table_names():
        return set()
    return {str(column["name"]) for column in inspector.get_columns("assets")}


def ensure_asset_identity_columns(bind: Engine) -> None:
    """Idempotently upgrade legacy asset tables without requiring a migration service."""

    existing = _asset_column_names(bind)
    if not existing:
        return
    for column_name, column_type in _ASSET_IDENTITY_COLUMNS.items():
        if column_name in existing:
            continue
        if bind.dialect.name == "postgresql":
            statement = text(
                f"ALTER TABLE assets ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            )
        else:
            statement = text(f"ALTER TABLE assets ADD COLUMN {column_name} {column_type}")
        try:
            with bind.begin() as connection:
                connection.execute(statement)
        except SQLAlchemyError:
            # SQLite has no portable ADD COLUMN IF NOT EXISTS. Another process
            # may have won the race between inspection and ALTER TABLE.
            if column_name not in _asset_column_names(bind):
                raise
        existing.add(column_name)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    ensure_asset_identity_columns(engine)


def get_db() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
