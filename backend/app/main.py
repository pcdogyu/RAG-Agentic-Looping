from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import UUID

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.db import ModelCallAuditRow, NewsRow, SessionLocal, engine, get_db, init_db
from backend.app.domain import (
    EventResearchRun,
    EvolutionCandidate,
    PaperOrder,
    PortfolioSnapshot,
    ResearchRun,
    utc_now,
)
from backend.app.model_audit import audit_detail, list_model_audits, model_usage
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService
from backend.app.services.evolution import EvolutionError, EvolutionService
from backend.app.services.notifications import notifier
from backend.app.services.portfolio import PortfolioError, PortfolioService
from backend.app.services.research import ResearchService
from backend.app.storage import (
    get_asset,
    get_event,
    get_event_research_run,
    get_news,
    get_run,
    list_assets,
    list_event_research_runs,
    list_events,
    list_evolutions,
    list_news,
    list_outcomes,
    list_recent_events,
    list_recommendations,
    list_runs,
    normalize_legacy_akshare_timestamps,
    save_evolution,
    upsert_asset,
)
from backend.app.worker import (
    celery_app,
    enqueue_research,
    enqueue_scan,
    evolve_failures,
    get_scan_status,
    request_scan_pause,
    resume_scan,
)
from backend.app.worker import execute_evolution as execute_evolution_task

settings = get_settings()
registry = ProviderRegistry(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    with SessionLocal() as db:
        normalize_legacy_akshare_timestamps(db)
        for asset in registry.all_assets():
            upsert_asset(db, asset)
        registry.add_assets(list_assets(db))
    yield


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Evidence-first cross-market research and paper portfolio API",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ResearchRequest(BaseModel):
    asset_id: str
    event_id: UUID | None = None
    as_of: datetime | None = None
    background: bool = True


class ScanRequest(BaseModel):
    background: bool = True


class PaperOrderRequest(BaseModel):
    recommendation_id: UUID
    price: float = Field(gt=0)
    target_weight: float | None = Field(default=None, gt=0, le=0.15)


class EvolutionRequest(BaseModel):
    failures: list[dict]
    background: bool = True


@app.get("/health")
def health() -> dict:
    database = False
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        database = True
    except Exception:
        pass
    latest_news = None
    try:
        with SessionLocal() as db:
            latest_news = db.scalar(select(func.max(NewsRow.observed_at)))
    except Exception:
        pass
    if latest_news and latest_news.tzinfo is None:
        latest_news = latest_news.replace(tzinfo=utc_now().tzinfo)
    news_age_seconds = (utc_now() - latest_news).total_seconds() if latest_news else None
    data_fresh = news_age_seconds is None or news_age_seconds <= settings.scan_interval_minutes * 180
    ollama = False
    models: list[str] = []
    try:
        response = httpx.get(f"{settings.ollama_base_url.rstrip('/')}/api/tags", timeout=2)
        response.raise_for_status()
        models = [item["name"] for item in response.json().get("models", [])]
        ollama = True
    except Exception:
        pass
    redis_ok = False
    task_failure_rate = None
    try:
        from redis import Redis

        redis_client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.5)
        redis_client.ping()
        successes = int(redis_client.get("market-loop:tasks:success") or 0)
        failures = int(redis_client.get("market-loop:tasks:failure") or 0)
        total = successes + failures
        task_failure_rate = failures / total if total else 0.0
        redis_ok = True
    except Exception:
        pass
    return {
        "status": "ok" if database and data_fresh else "degraded",
        "database": database,
        "redis": redis_ok,
        "task_failure_rate": task_failure_rate,
        "ollama": ollama,
        "models": models,
        "fmp_configured": settings.fmp_enabled,
        "fmp_mcp_configured": bool(settings.fmp_mcp_url),
        "latest_news_at": latest_news,
        "news_age_seconds": news_age_seconds,
        "data_fresh": data_fresh,
        "telegram_configured": bool(settings.telegram_bot_token and settings.telegram_chat_id),
        "evolution_enabled": settings.evolution_enabled,
        "as_of": utc_now(),
    }


@app.get("/api/v1/assets")
def assets(db: Session = Depends(get_db)):
    return list_assets(db)


@app.get("/api/v1/news")
def news(
    limit: int = Query(default=100, ge=1, le=500),
    as_of: datetime | None = None,
    db: Session = Depends(get_db),
):
    return list_news(db, limit, as_of)


@app.get("/api/v1/events")
def events(
    limit: int = Query(default=100, ge=1, le=500),
    as_of: datetime | None = None,
    db: Session = Depends(get_db),
):
    return list_events(db, limit, as_of)


@app.post("/api/v1/scan")
def start_scan(request: ScanRequest, db: Session = Depends(get_db)):
    if request.background:
        task_id, status = enqueue_scan()
        return {"task_id": task_id, "status": status, "scan": get_scan_status()}
    since = utc_now() - timedelta(minutes=settings.scan_interval_minutes * 2)
    items = registry.discover_news(since=since, limit=settings.scan_batch_size)
    created = EventService(registry).ingest(db, items)
    return {"news": len(items), "events": len(created)}


@app.get("/api/v1/scan/status")
def scan_status():
    return get_scan_status()


@app.post("/api/v1/scan/pause")
def pause_scan():
    try:
        request_scan_pause()
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"status": "paused", "scan": get_scan_status()}


@app.post("/api/v1/scan/resume")
def resume_paused_scan():
    try:
        resume_scan()
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"status": "running", "scan": get_scan_status()}


@app.get("/api/v1/tasks/{task_id}")
def task_status(task_id: str):
    task = celery_app.AsyncResult(task_id)
    payload: dict = {"task_id": task_id, "state": task.state}
    if task.state == "PROGRESS" and isinstance(task.info, dict):
        payload["progress"] = task.info
    elif task.successful():
        payload["result"] = task.result
    elif task.failed():
        payload["error"] = type(task.result).__name__
    return payload


@app.post("/api/v1/research", response_model=ResearchRun | dict)
def start_research(request: ResearchRequest, db: Session = Depends(get_db)):
    asset = get_asset(db, request.asset_id) or registry.get_asset(request.asset_id)
    if not asset:
        raise HTTPException(404, "asset not found")
    if request.background:
        event = get_event(db, request.event_id) if request.event_id else None
        task_id, run = enqueue_research(db, asset, event, request.as_of)
        return {"task_id": task_id, "run_id": str(run.id), "status": "queued"}
    event = get_event(db, request.event_id) if request.event_id else None
    # Release the read-only transaction before first-use checkpoint DDL.
    db.rollback()
    return ResearchService(registry, db).run(asset, event, request.as_of)


@app.get("/api/v1/research-runs")
def research_runs(limit: int = Query(default=100, ge=1, le=500), db: Session = Depends(get_db)):
    return list_runs(db, limit)


@app.get("/api/v1/research-runs/{run_id}")
def research_run(run_id: UUID, db: Session = Depends(get_db)):
    value = get_run(db, run_id)
    if not value:
        raise HTTPException(404, "run not found")
    return value


@app.get("/api/v1/event-research-runs", response_model=list[EventResearchRun])
def event_research_runs(
    limit: int = Query(default=100, ge=1, le=500), db: Session = Depends(get_db)
):
    return list_event_research_runs(db, limit)


@app.get("/api/v1/event-research-runs/{run_id}", response_model=EventResearchRun)
def event_research_run(run_id: UUID, db: Session = Depends(get_db)):
    value = get_event_research_run(db, run_id)
    if not value:
        raise HTTPException(404, "event research run not found")
    return value


@app.get("/api/v1/recommendations")
def recommendations(limit: int = Query(default=100, ge=1, le=500), db: Session = Depends(get_db)):
    return list_recommendations(db, limit)


def _analysis_logs(db: Session, limit: int) -> list[dict]:
    """Join event, source-news and research payloads into a UI audit view."""

    entries: list[tuple[datetime, dict]] = []
    seen_event_ids: set[UUID] = set()

    def make_entry(event, run=None, event_run=None) -> tuple[datetime, dict]:
        news_items = [
            item
            for news_id in (event.news_item_ids if event else [])
            if (item := get_news(db, news_id)) is not None
        ]
        steps = event_run.analysis_steps if event_run else (
            run.analysis_steps
            if run and run.analysis_steps
            else (event.analysis_steps if event else [])
        )
        model_names = list(dict.fromkeys(step.model for step in steps if step.model))
        recommendation = run.recommendation if run else None
        report = event_run.report if event_run else None
        asset = run.asset if run else (event.candidates[0].asset if event and event.candidates else None)
        status = run.status.value if run else (
            event_run.status.value if event_run else _event_mapping_status(event)
        )
        updated_at = run.updated_at if run else (
            event_run.updated_at if event_run else max(
                [event.observed_at, *[step.occurred_at for step in event.analysis_steps]]
            )
        )
        payload = {
            "id": str(run.id if run else event_run.id if event_run else event.id),
            "event_id": str(event.id) if event else None,
            "run_id": str(run.id) if run else None,
            "event_research_run_id": str(event_run.id) if event_run else None,
            "status": status,
            "updated_at": updated_at.isoformat(),
            "news": [
                {
                    "id": str(item.id),
                    "title": item.title,
                    "source": item.source,
                    "url": item.url,
                    "published_at": item.published_at.isoformat(),
                }
                for item in news_items
            ],
            "event": (
                {
                    "id": str(event.id),
                    "headline": event.headline,
                    "event_type": event.event_type.value,
                    "direct_impact": event.direct_impact,
                    "priority": event.priority,
                }
                if event
                else None
            ),
            "asset": asset.model_dump(mode="json") if asset else None,
            "models": model_names,
            "steps": [step.model_dump(mode="json") for step in steps],
            "result": (
                {
                    "kind": "asset_recommendation",
                    "rating": recommendation.rating.value,
                    "score": recommendation.score,
                    "confidence": recommendation.confidence,
                    "evidence_complete": recommendation.evidence_complete,
                    "summary": recommendation.thesis.summary,
                }
                if recommendation
                else (
                    {
                        "kind": "event_report",
                        "confidence": report.confidence,
                        "evidence_complete": report.evidence_complete,
                        "summary": report.summary,
                        "affected_markets": report.affected_markets,
                        "affected_sectors": report.affected_sectors,
                        "scenarios": report.scenarios,
                        "catalysts": report.catalysts,
                        "risks": report.risks,
                        "unresolved_questions": report.unresolved_questions,
                    }
                    if report
                    else None
                )
            ),
        }
        return updated_at, payload

    def _event_mapping_status(event) -> str:
        if not event:
            return "not_researched"
        for step in reversed(event.analysis_steps):
            if step.phase == "asset_mapping" and step.status in {
                "running",
                "retrying",
                "failed",
            }:
                return f"mapping_{step.status}"
            if step.phase == "asset_mapping_queue" and step.status == "queued":
                return "mapping_queued"
        return "unmapped" if not event.candidates else "not_researched"

    for run in list_runs(db, max(limit * 3, 30)):
        event = get_event(db, run.event_id) if run.event_id else None
        if run.event_id:
            seen_event_ids.add(run.event_id)
        entries.append(make_entry(event, run))
    for event_run in list_event_research_runs(db, max(limit * 3, 30)):
        event = get_event(db, event_run.event_id)
        seen_event_ids.add(event_run.event_id)
        entries.append(make_entry(event, event_run=event_run))
    for event in list_recent_events(db, max(limit * 3, 30)):
        if event.id not in seen_event_ids:
            entries.append(make_entry(event))
    entries.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in entries[:limit]]


@app.get("/api/v1/analysis-logs")
def analysis_logs(
    limit: int = Query(default=10, ge=1, le=50), db: Session = Depends(get_db)
):
    return _analysis_logs(db, limit)


@app.get("/api/v1/model-usage")
def model_usage_summary(
    start: datetime | None = None,
    end: datetime | None = None,
    model: str | None = None,
    provider: str | None = None,
    operation: str | None = None,
    status: str | None = None,
    language: str | None = None,
    fidelity: str | None = None,
    db: Session = Depends(get_db),
):
    return model_usage(
        db,
        start=start,
        end=end,
        model=model,
        provider=provider,
        operation=operation,
        status=status,
        language=language,
        fidelity=fidelity,
    )


@app.get("/api/v1/model-logs")
def model_logs(
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    model: str | None = None,
    provider: str | None = None,
    operation: str | None = None,
    status: str | None = None,
    language: str | None = None,
    fidelity: str | None = None,
    db: Session = Depends(get_db),
):
    try:
        return list_model_audits(
            db,
            limit=limit,
            cursor=cursor,
            start=start,
            end=end,
            model=model,
            provider=provider,
            operation=operation,
            status=status,
            language=language,
            fidelity=fidelity,
        )
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(400, "invalid model log cursor") from exc


@app.get("/api/v1/model-logs/{audit_id}")
def model_log(audit_id: UUID, db: Session = Depends(get_db)):
    row = db.get(ModelCallAuditRow, audit_id)
    if not row:
        raise HTTPException(404, "model log not found")
    return audit_detail(row)


@app.get("/api/v1/portfolio", response_model=PortfolioSnapshot)
def portfolio(db: Session = Depends(get_db)):
    return PortfolioService(registry).snapshot(db)


@app.post("/api/v1/paper-orders", response_model=PaperOrder)
def paper_order(request: PaperOrderRequest, db: Session = Depends(get_db)):
    recommendation = next(
        (item for item in list_recommendations(db, 1000) if item.id == request.recommendation_id),
        None,
    )
    if not recommendation:
        raise HTTPException(404, "recommendation not found")
    try:
        order = PortfolioService(registry).create_from_recommendation(
            db, recommendation, request.price, request.target_weight
        )
        notifier.send(
            f"模拟仓位变化：{order.side.value} {order.asset.symbol} "
            f"{order.quantity:g} @ {order.price:g} {order.currency}"
        )
        return order
    except PortfolioError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/v1/outcomes")
def outcomes(db: Session = Depends(get_db)):
    return list_outcomes(db)


@app.get("/api/v1/evolution")
def evolutions(db: Session = Depends(get_db)):
    return list_evolutions(db)


@app.post("/api/v1/evolution", response_model=EvolutionCandidate | dict)
def propose_evolution(request: EvolutionRequest, db: Session = Depends(get_db)):
    if request.background:
        if not settings.evolution_enabled:
            raise HTTPException(409, "EVOLUTION_ENABLED is false")
        task = evolve_failures.apply_async(args=[request.failures], queue="evolution")
        return {"task_id": task.id, "status": "queued"}
    try:
        return EvolutionService().propose(db, request.failures)
    except EvolutionError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/v1/evolution/{candidate_id}/execute", response_model=EvolutionCandidate | dict)
def execute_evolution(
    candidate_id: UUID, background: bool = True, db: Session = Depends(get_db)
):
    candidate = next((item for item in list_evolutions(db) if item.id == candidate_id), None)
    if not candidate:
        raise HTTPException(404, "evolution candidate not found")
    if background:
        task = execute_evolution_task.apply_async(args=[str(candidate_id)], queue="evolution")
        return {"task_id": task.id, "status": "queued"}
    try:
        result = EvolutionService().execute(db, candidate)
        save_evolution(db, result)
        return result
    except EvolutionError as exc:
        raise HTTPException(409, str(exc)) from exc


async def event_stream() -> AsyncIterator[str]:
    last_payload = ""
    while True:
        with SessionLocal() as db:
            payload = json.dumps(
                {
                    "events": [item.model_dump(mode="json") for item in list_events(db, 30)],
                    "runs": [item.model_dump(mode="json") for item in list_runs(db, 10)],
                    "recommendations": [
                        item.model_dump(mode="json") for item in list_recommendations(db, 10)
                    ],
                    "event_research_runs": [
                        item.model_dump(mode="json")
                        for item in list_event_research_runs(db, 10)
                    ],
                    "analysis_logs": _analysis_logs(db, 10),
                },
                ensure_ascii=False,
            )
        if payload != last_payload:
            yield f"event: snapshot\ndata: {payload}\n\n"
            last_payload = payload
        else:
            yield ": keepalive\n\n"
        await asyncio.sleep(3)


@app.get("/api/v1/stream")
async def stream():
    return StreamingResponse(event_stream(), media_type="text/event-stream")
