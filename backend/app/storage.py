from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import desc, exists, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.db import (
    AssetRow,
    EventResearchRunRow,
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
    EventResearchRun,
    Evidence,
    EvolutionCandidate,
    NewsEvent,
    NewsItem,
    Outcome,
    PaperOrder,
    Recommendation,
    ResearchRun,
    RunStatus,
    as_utc,
    utc_now,
)
from backend.app.services.source_lineage import enrich_news_lineage


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
    row.issuer_id = asset.issuer_id
    row.primary_listing_asset_id = asset.primary_listing_asset_id
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
        issuer_id=row.issuer_id,
        primary_listing_asset_id=row.primary_listing_asset_id,
        lot_size=row.lot_size or 1,
        active=row.active,
    )


def list_assets(db: Session) -> list[AssetRef]:
    return [asset_from_row(row) for row in db.scalars(select(AssetRow)).all()]


def get_asset(db: Session, asset_id: str) -> AssetRef | None:
    row = db.get(AssetRow, asset_id)
    return asset_from_row(row) if row else None


def news_row_from_item(item: NewsItem) -> NewsRow:
    item = enrich_news_lineage(item)
    payload = item.model_dump(mode="json")
    return NewsRow(
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


def save_news(db: Session, item: NewsItem) -> bool:
    row = news_row_from_item(item)
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
        statement.order_by(desc(EventRow.published_at), desc(EventRow.priority)).limit(limit)
    ).all()
    return [NewsEvent.model_validate(row.payload) for row in rows]


def list_events_without_downstream(
    db: Session,
    *,
    observed_after: datetime,
    limit: int = 100,
) -> list[NewsEvent]:
    """Return recent events that never reached mapping/research follow-up."""

    rows = db.scalars(
        select(EventRow)
        .where(
            EventRow.observed_at >= observed_after,
            ~exists(
                select(EventResearchRunRow.id).where(
                    EventResearchRunRow.event_id == EventRow.id
                )
            ),
            ~exists(
                select(ResearchRunRow.id).where(ResearchRunRow.event_id == EventRow.id)
            ),
        )
        .order_by(EventRow.observed_at, EventRow.id)
        .limit(limit)
    ).all()
    return [NewsEvent.model_validate(row.payload) for row in rows]


def normalize_legacy_akshare_timestamps(db: Session) -> dict[str, int]:
    """One-time correction for AkShare local timestamps previously tagged as UTC."""

    marker = "Asia/Shanghai->UTC:v1"
    correction = timedelta(hours=8)
    corrected_news: dict[UUID, NewsRow] = {}
    rows = db.scalars(select(NewsRow).where(NewsRow.source.like("%AkShare%"))).all()
    for row in rows:
        metadata = dict(row.raw_metadata or {})
        if metadata.get("time_normalization") == marker:
            continue
        row.published_at = as_utc(row.published_at) - correction
        row.as_of = as_utc(row.as_of) - correction
        metadata["time_normalization"] = marker
        row.raw_metadata = metadata
        corrected_news[row.id] = row
        db.add(row)

    corrected_events = 0
    if corrected_news:
        for event_row in db.scalars(select(EventRow)).all():
            payload = dict(event_row.payload or {})
            news_ids = {
                UUID(str(value))
                for value in payload.get("news_item_ids", [])
                if value
            }
            if not any(item_id in corrected_news for item_id in news_ids):
                continue
            related = [db.get(NewsRow, item_id) for item_id in news_ids]
            related = [item for item in related if item is not None]
            if not related:
                continue
            published_at = max(as_utc(item.published_at) for item in related)
            as_of = max(as_utc(item.as_of) for item in related)
            event_row.published_at = published_at
            event_row.as_of = as_of
            payload["published_at"] = published_at.isoformat()
            payload["as_of"] = as_of.isoformat()
            event_row.payload = payload
            db.add(event_row)
            corrected_events += 1

    if corrected_news:
        db.commit()
    return {"news": len(corrected_news), "events": corrected_events}


def list_recent_events(db: Session, limit: int = 100) -> list[NewsEvent]:
    rows = db.scalars(
        select(EventRow).order_by(desc(EventRow.observed_at)).limit(limit)
    ).all()
    return [NewsEvent.model_validate(row.payload) for row in rows]


def save_run(db: Session, run: ResearchRun) -> None:
    run.updated_at = utc_now()
    row = db.get(ResearchRunRow, run.id)
    if row is not None:
        db.refresh(row, attribute_names=["status"])
    else:
        row = ResearchRunRow(id=run.id, created_at=run.created_at)
    if row.status == RunStatus.CANCELLED.value and run.status is not RunStatus.CANCELLED:
        db.rollback()
        return
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


def list_active_runs(db: Session) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow)
        .where(ResearchRunRow.status.in_(("queued", "running", "verifying")))
        .order_by(ResearchRunRow.created_at)
    ).all()
    return [ResearchRun.model_validate(row.payload) for row in rows]


def list_active_runs_for_asset(db: Session, asset_id: str) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow)
        .where(
            ResearchRunRow.asset_id == asset_id,
            ResearchRunRow.status.in_(
                (
                    RunStatus.QUEUED.value,
                    RunStatus.RUNNING.value,
                    RunStatus.VERIFYING.value,
                )
            ),
        )
        .order_by(ResearchRunRow.created_at)
    ).all()
    return [ResearchRun.model_validate(row.payload) for row in rows]


def get_latest_cooldown_run(
    db: Session,
    asset_id: str,
    *,
    completed_after: datetime,
) -> ResearchRun | None:
    """Return the latest real completed research inside an asset cooldown window."""

    rows = db.scalars(
        select(ResearchRunRow)
        .where(
            ResearchRunRow.asset_id == asset_id,
            ResearchRunRow.status.in_(
                (
                    RunStatus.COMPLETED.value,
                    RunStatus.INSUFFICIENT_EVIDENCE.value,
                )
            ),
        )
        .order_by(desc(ResearchRunRow.updated_at))
    ).all()
    cutoff = as_utc(completed_after)
    eligible = []
    for row in rows:
        run = ResearchRun.model_validate(row.payload)
        if run.historical_replay or run.completed_at is None:
            continue
        if as_utc(run.completed_at) > cutoff:
            eligible.append(run)
    return max(eligible, key=lambda item: as_utc(item.completed_at)) if eligible else None


def list_queued_runs(db: Session) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow)
        .where(ResearchRunRow.status == RunStatus.QUEUED.value)
        .order_by(ResearchRunRow.created_at)
    ).all()
    return [ResearchRun.model_validate(row.payload) for row in rows]


def get_mergeable_queued_run(
    db: Session,
    asset_id: str,
    created_after: datetime,
) -> ResearchRun | None:
    rows = db.scalars(
        select(ResearchRunRow)
        .where(
            ResearchRunRow.asset_id == asset_id,
            ResearchRunRow.status == RunStatus.QUEUED.value,
            ResearchRunRow.created_at >= created_after,
        )
        .order_by(ResearchRunRow.created_at)
    ).all()
    for row in rows:
        run = ResearchRun.model_validate(row.payload)
        if not run.historical_replay and run.retry_of_run_id is None:
            return run
    return None


def list_failed_runs(db: Session, limit: int = 100) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow)
        .where(ResearchRunRow.status == "failed")
        .order_by(desc(ResearchRunRow.updated_at))
        .limit(limit)
    ).all()
    return [ResearchRun.model_validate(row.payload) for row in rows]


def list_retryable_runs(db: Session, limit: int = 100) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow)
        .where(
            ResearchRunRow.status.in_(
                (RunStatus.FAILED.value, RunStatus.INSUFFICIENT_EVIDENCE.value)
            )
        )
        .order_by(desc(ResearchRunRow.updated_at))
        .limit(limit * 4)
    ).all()
    values = [ResearchRun.model_validate(row.payload) for row in rows]
    return [
        run
        for run in values
        if run.status is RunStatus.FAILED or run.retryable_reason is not None
    ][:limit]


def list_retries_for_run(db: Session, run_id: UUID) -> list[ResearchRun]:
    rows = db.scalars(
        select(ResearchRunRow).order_by(desc(ResearchRunRow.created_at))
    ).all()
    retries = [
        ResearchRun.model_validate(row.payload)
        for row in rows
        if str((row.payload or {}).get("retry_of_run_id") or "") == str(run_id)
    ]
    return retries


def list_retryable_run_lineages(
    db: Session,
) -> tuple[list[ResearchRun], dict[UUID, list[ResearchRun]]]:
    """Load retryable asset runs and all retry children in one database scan."""

    rows = db.scalars(
        select(ResearchRunRow).order_by(desc(ResearchRunRow.created_at))
    ).all()
    originals: list[ResearchRun] = []
    retries: dict[UUID, list[ResearchRun]] = {}
    for row in rows:
        run = ResearchRun.model_validate(row.payload)
        if run.retry_of_run_id is not None:
            retries.setdefault(run.retry_of_run_id, []).append(run)
        elif run.status is RunStatus.FAILED or run.retryable_reason is not None:
            originals.append(run)
    return originals, retries


def get_run_for_event_asset(
    db: Session, event_id: UUID, asset_id: str
) -> ResearchRun | None:
    row = db.scalar(
        select(ResearchRunRow).where(
            ResearchRunRow.event_id == event_id,
            ResearchRunRow.asset_id == asset_id,
        ).order_by(desc(ResearchRunRow.created_at))
    )
    return ResearchRun.model_validate(row.payload) if row else None


def save_event_research_run(db: Session, run: EventResearchRun) -> None:
    run.updated_at = utc_now()
    row = db.get(EventResearchRunRow, run.id)
    if row is not None:
        db.refresh(row, attribute_names=["status"])
    else:
        row = EventResearchRunRow(
            id=run.id,
            event_id=run.event_id,
            created_at=run.created_at,
        )
    if row.status == RunStatus.CANCELLED.value and run.status is not RunStatus.CANCELLED:
        db.rollback()
        return
    row.event_id = run.event_id
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


def get_event_research_run(db: Session, run_id: UUID) -> EventResearchRun | None:
    row = db.get(EventResearchRunRow, run_id)
    return EventResearchRun.model_validate(row.payload) if row else None


def get_event_research_for_event(db: Session, event_id: UUID) -> EventResearchRun | None:
    row = db.scalar(
        select(EventResearchRunRow).where(EventResearchRunRow.event_id == event_id)
    )
    return EventResearchRun.model_validate(row.payload) if row else None


def list_event_research_runs(db: Session, limit: int = 100) -> list[EventResearchRun]:
    rows = db.scalars(
        select(EventResearchRunRow)
        .order_by(desc(EventResearchRunRow.created_at))
        .limit(limit)
    ).all()
    return [EventResearchRun.model_validate(row.payload) for row in rows]


def list_failed_event_research_runs(
    db: Session, limit: int = 100
) -> list[EventResearchRun]:
    rows = db.scalars(
        select(EventResearchRunRow)
        .where(EventResearchRunRow.status == "failed")
        .order_by(desc(EventResearchRunRow.updated_at))
        .limit(limit)
    ).all()
    return [EventResearchRun.model_validate(row.payload) for row in rows]


def list_retryable_event_research_runs(
    db: Session, limit: int | None = 100
) -> list[EventResearchRun]:
    query = (
        select(EventResearchRunRow)
        .where(
            EventResearchRunRow.status.in_(
                (RunStatus.FAILED.value, RunStatus.INSUFFICIENT_EVIDENCE.value)
            )
        )
        .order_by(desc(EventResearchRunRow.updated_at))
    )
    if limit is not None:
        query = query.limit(limit * 2)
    rows = db.scalars(query).all()
    values = [EventResearchRun.model_validate(row.payload) for row in rows]
    retryable = [
        run
        for run in values
        if run.status is RunStatus.FAILED or run.retryable_reason is not None
    ]
    return retryable[:limit] if limit is not None else retryable


def list_recommendation_run_ids(db: Session) -> set[UUID]:
    return set(db.scalars(select(RecommendationRow.run_id)).all())


def get_events_by_ids(db: Session, event_ids: set[UUID]) -> dict[UUID, NewsEvent]:
    if not event_ids:
        return {}
    rows = db.scalars(select(EventRow).where(EventRow.id.in_(event_ids))).all()
    return {row.id: NewsEvent.model_validate(row.payload) for row in rows}


def save_recommendation(db: Session, recommendation: Recommendation) -> None:
    row = db.scalar(
        select(RecommendationRow).where(RecommendationRow.run_id == recommendation.run_id)
    )
    if row is None:
        row = RecommendationRow(id=recommendation.id, run_id=recommendation.run_id)
    else:
        recommendation.id = row.id
    row.asset_id = recommendation.asset.asset_id
    row.score = recommendation.score
    row.rating = recommendation.rating.value
    row.confidence = recommendation.confidence
    row.as_of = recommendation.as_of
    row.payload = recommendation.model_dump(mode="json")
    db.add(row)
    db.commit()


def get_recommendation_for_run(db: Session, run_id: UUID) -> Recommendation | None:
    row = db.scalar(select(RecommendationRow).where(RecommendationRow.run_id == run_id))
    return Recommendation.model_validate(row.payload) if row else None


def get_recommendation(db: Session, recommendation_id: UUID) -> Recommendation | None:
    row = db.get(RecommendationRow, recommendation_id)
    return Recommendation.model_validate(row.payload) if row else None


def list_latest_legacy_gate_recommendations(
    db: Session,
    *,
    current_scoring_version: str,
) -> list[Recommendation]:
    """Return one latest legacy insufficient-evidence conclusion per asset."""

    rows = db.scalars(
        select(RecommendationRow).order_by(
            desc(RecommendationRow.as_of),
            desc(RecommendationRow.id),
        )
    ).all()
    selected: dict[str, Recommendation] = {}
    for row in rows:
        recommendation = Recommendation.model_validate(row.payload)
        status = getattr(recommendation.signal_status, "value", recommendation.signal_status)
        if (
            recommendation.scoring_version != current_scoring_version
            and status == "insufficient_evidence"
            and recommendation.asset.asset_id not in selected
        ):
            selected[recommendation.asset.asset_id] = recommendation
    return sorted(selected.values(), key=lambda item: (as_utc(item.as_of), str(item.id)))


def asset_has_scoring_version(
    db: Session,
    asset_id: str,
    scoring_version: str,
) -> bool:
    rows = db.scalars(
        select(RecommendationRow.payload).where(RecommendationRow.asset_id == asset_id)
    ).all()
    return any(
        str((payload or {}).get("scoring_version") or "") == scoring_version
        for payload in rows
    )


def asset_has_research_phase(db: Session, asset_id: str, phase: str) -> bool:
    rows = db.scalars(
        select(ResearchRunRow.payload).where(ResearchRunRow.asset_id == asset_id)
    ).all()
    return any(
        any(str(step.get("phase") or "") == phase for step in (payload or {}).get("analysis_steps", []))
        for payload in rows
    )


def list_recommendations(
    db: Session,
    limit: int = 100,
    *,
    offset: int = 0,
    oldest_first: bool = False,
) -> list[Recommendation]:
    ordering = RecommendationRow.as_of if oldest_first else desc(RecommendationRow.as_of)
    rows = db.scalars(
        select(RecommendationRow)
        .order_by(ordering, RecommendationRow.id)
        .offset(offset)
        .limit(limit)
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
