from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.db import (
    AssetRow,
    EventRow,
    EvidenceRow,
    EvolutionRow,
    NewsRow,
    OutcomeRow,
    PaperOrderRow,
    RecommendationRow,
    ResearchRunRow,
)
from backend.app.domain import (
    AssetRef,
    Evidence,
    EvolutionCandidate,
    NewsEvent,
    NewsItem,
    Outcome,
    PaperOrder,
    Recommendation,
    ResearchRun,
    as_utc,
    utc_now,
)


def upsert_asset(db: Session, asset: AssetRef) -> AssetRef:
    row = db.get(AssetRow, asset.asset_id) or AssetRow(id=asset.asset_id)
    payload = asset.model_dump(mode="json")
    row.asset_class = payload["asset_class"]
    row.market = payload["market"]
    row.symbol = asset.symbol
    row.name = asset.name
    row.exchange_or_provider = asset.exchange_or_provider
    row.currency = asset.currency
    row.aliases = asset.aliases
    row.products = asset.products
    row.competitors = asset.competitors
    row.lot_size = asset.lot_size
    row.active = asset.active
    db.add(row)
    db.commit()
    return asset


def asset_from_row(row: AssetRow) -> AssetRef:
    return AssetRef(
        asset_id=row.id,
        asset_class=row.asset_class,
        market=row.market,
        symbol=row.symbol,
        name=row.name,
        exchange_or_provider=row.exchange_or_provider,
        currency=row.currency,
        aliases=row.aliases or [],
        products=row.products or [],
        competitors=row.competitors or [],
        lot_size=row.lot_size or 1,
        active=row.active,
    )


def list_assets(db: Session) -> list[AssetRef]:
    return [asset_from_row(row) for row in db.scalars(select(AssetRow)).all()]


def get_asset(db: Session, asset_id: str) -> AssetRef | None:
    row = db.get(AssetRow, asset_id)
    return asset_from_row(row) if row else None


def save_news(db: Session, item: NewsItem) -> bool:
    payload = item.model_dump(mode="json")
    row = NewsRow(
        id=item.id,
        source=item.source,
        source_quality=item.source_quality.value,
        title=item.title,
        summary=item.summary,
        url=item.url,
        language=item.language,
        published_at=item.published_at,
        observed_at=item.observed_at,
        as_of=item.as_of,
        content_hash=item.content_hash,
        symbols=item.symbols,
        raw_metadata=payload["raw_metadata"],
    )
    db.add(row)
    try:
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        return False


def get_news_by_content_hash(db: Session, content_hash: str) -> NewsItem | None:
    row = db.scalar(select(NewsRow).where(NewsRow.content_hash == content_hash))
    return news_from_row(row) if row else None


def get_news(db: Session, news_id: UUID) -> NewsItem | None:
    row = db.get(NewsRow, news_id)
    return news_from_row(row) if row else None


def event_news_item_ids(db: Session) -> set[UUID]:
    """Return news IDs already attached to a durable event.

    A news row is committed before LLM extraction so a failed worker never loses
    the source item. This projection lets the next scan resume those orphaned
    rows without reprocessing items whose event was already saved.
    """

    processed: set[UUID] = set()
    for payload in db.scalars(select(EventRow.payload)).all():
        for value in (payload or {}).get("news_item_ids", []):
            try:
                processed.add(UUID(str(value)))
            except (TypeError, ValueError):
                continue
    return processed


def news_from_row(row: NewsRow) -> NewsItem:
    return NewsItem(
        id=row.id,
        source=row.source,
        source_quality=row.source_quality,
        title=row.title,
        summary=row.summary,
        url=row.url,
        language=row.language,
        published_at=as_utc(row.published_at),
        observed_at=as_utc(row.observed_at),
        as_of=as_utc(row.as_of),
        content_hash=row.content_hash,
        symbols=row.symbols or [],
        raw_metadata=row.raw_metadata or {},
    )


def list_news(db: Session, limit: int = 100, as_of: datetime | None = None) -> list[NewsItem]:
    statement = select(NewsRow)
    if as_of:
        statement = statement.where(NewsRow.observed_at <= as_of, NewsRow.published_at <= as_of)
    rows = db.scalars(statement.order_by(desc(NewsRow.published_at)).limit(limit)).all()
    return [news_from_row(row) for row in rows]


def save_event(db: Session, event: NewsEvent) -> None:
    row = db.get(EventRow, event.id) or EventRow(id=event.id)
    row.headline = event.headline
    row.event_type = event.event_type.value
    row.payload = event.model_dump(mode="json")
    row.priority = event.priority
    row.published_at = event.published_at
    row.observed_at = event.observed_at
    row.as_of = event.as_of
    db.add(row)
    db.commit()


def get_event(db: Session, event_id: UUID) -> NewsEvent | None:
    row = db.get(EventRow, event_id)
    return NewsEvent.model_validate(row.payload) if row else None


def list_events(db: Session, limit: int = 100, as_of: datetime | None = None) -> list[NewsEvent]:
    statement = select(EventRow)
    if as_of:
        statement = statement.where(EventRow.observed_at <= as_of, EventRow.published_at <= as_of)
    rows = db.scalars(
        statement.order_by(desc(EventRow.priority), desc(EventRow.published_at)).limit(limit)
    ).all()
    return [NewsEvent.model_validate(row.payload) for row in rows]


def list_recent_events(db: Session, limit: int = 100) -> list[NewsEvent]:
    rows = db.scalars(
        select(EventRow).order_by(desc(EventRow.observed_at)).limit(limit)
    ).all()
    return [NewsEvent.model_validate(row.payload) for row in rows]


def save_run(db: Session, run: ResearchRun) -> None:
    run.updated_at = utc_now()
    row = db.get(ResearchRunRow, run.id) or ResearchRunRow(id=run.id, created_at=run.created_at)
    row.event_id = run.event_id
    row.asset_id = run.asset.asset_id
    row.status = run.status.value
    row.payload = run.model_dump(mode="json")
    row.updated_at = run.updated_at
    db.add(row)
    for evidence in run.evidence:
        evidence_row = db.get(EvidenceRow, evidence.id) or EvidenceRow(id=evidence.id)
        evidence_row.run_id = evidence.run_id
        evidence_row.claim = evidence.claim
        evidence_row.source_url = evidence.source_url
        evidence_row.source_quality = evidence.source_quality.value
        evidence_row.published_at = evidence.published_at
        evidence_row.observed_at = evidence.observed_at
        evidence_row.as_of = evidence.as_of
        evidence_row.payload = evidence.model_dump(mode="json")
        db.add(evidence_row)
    db.commit()


def get_evidence(db: Session, evidence_id: UUID) -> Evidence | None:
    row = db.get(EvidenceRow, evidence_id)
    return Evidence.model_validate(row.payload) if row else None


def get_run(db: Session, run_id: UUID) -> ResearchRun | None:
    row = db.get(ResearchRunRow, run_id)
    return ResearchRun.model_validate(row.payload) if row else None


def list_runs(db: Session, limit: int = 100) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow).order_by(desc(ResearchRunRow.created_at)).limit(limit)
    ).all()
    return [ResearchRun.model_validate(row.payload) for row in rows]


def save_recommendation(db: Session, recommendation: Recommendation) -> None:
    row = RecommendationRow(
        id=recommendation.id,
        run_id=recommendation.run_id,
        asset_id=recommendation.asset.asset_id,
        score=recommendation.score,
        rating=recommendation.rating.value,
        confidence=recommendation.confidence,
        as_of=recommendation.as_of,
        payload=recommendation.model_dump(mode="json"),
    )
    db.add(row)
    db.commit()


def list_recommendations(db: Session, limit: int = 100) -> list[Recommendation]:
    rows = db.scalars(
        select(RecommendationRow).order_by(desc(RecommendationRow.as_of)).limit(limit)
    ).all()
    return [Recommendation.model_validate(row.payload) for row in rows]


def save_order(db: Session, order: PaperOrder) -> None:
    db.add(
        PaperOrderRow(
            id=order.id,
            recommendation_id=order.recommendation_id,
            asset_id=order.asset.asset_id,
            side=order.side.value,
            quantity=order.quantity,
            price=order.price,
            currency=order.currency,
            fee=order.fee,
            executed_at=order.executed_at,
            payload=order.model_dump(mode="json"),
        )
    )
    db.commit()


def list_orders(db: Session) -> list[PaperOrder]:
    rows = db.scalars(select(PaperOrderRow).order_by(PaperOrderRow.executed_at)).all()
    return [PaperOrder.model_validate(row.payload) for row in rows]


def save_outcome(db: Session, outcome: Outcome) -> None:
    db.add(
        OutcomeRow(
            id=outcome.id,
            recommendation_id=outcome.recommendation_id,
            horizon_days=outcome.horizon_days,
            observed_at=outcome.observed_at,
            payload=outcome.model_dump(mode="json"),
        )
    )
    db.commit()


def list_outcomes(db: Session) -> list[Outcome]:
    rows = db.scalars(select(OutcomeRow).order_by(desc(OutcomeRow.observed_at))).all()
    return [Outcome.model_validate(row.payload) for row in rows]


def save_evolution(db: Session, candidate: EvolutionCandidate) -> None:
    row = db.get(EvolutionRow, candidate.id) or EvolutionRow(id=candidate.id)
    row.branch = candidate.branch
    row.status = candidate.status.value
    row.payload = candidate.model_dump(mode="json")
    row.created_at = candidate.created_at
    db.add(row)
    db.commit()


def list_evolutions(db: Session) -> list[EvolutionCandidate]:
    rows = db.scalars(select(EvolutionRow).order_by(desc(EvolutionRow.created_at))).all()
    return [EvolutionCandidate.model_validate(row.payload) for row in rows]
