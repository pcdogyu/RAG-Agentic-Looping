from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from threading import Condition, Lock, Thread
from time import monotonic
from typing import Literal
from uuid import UUID, uuid4

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from redis import Redis
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from backend.app.api_assets import router as assets_router
from backend.app.api_integrations import router as integrations_router
from backend.app.config import get_settings
from backend.app.db import (
    ModelCallAuditRow,
    NewsRow,
    NewsSourceStateRow,
    SessionLocal,
    engine,
    get_db,
    init_db,
)
from backend.app.domain import (
    EventResearchRun,
    EvolutionCandidate,
    PaperOrder,
    PortfolioSnapshot,
    ResearchRun,
    RunStatus,
    utc_now,
)
from backend.app.llm import gateway
from backend.app.model_audit import audit_detail, list_model_audits, model_usage
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.asset_universe import AssetUniverseService
from backend.app.services.event_refresh import full_event_research_is_active
from backend.app.services.events import EventService
from backend.app.services.evolution import EvolutionError, EvolutionService
from backend.app.services.fact_sources import get_effective_settings
from backend.app.services.failed_research import failed_research_candidates
from backend.app.services.mcp_registry import seed_integrations
from backend.app.services.model_instances import (
    broker_queue_name,
    configured_model_instances,
    instance_health,
    lane_model,
    select_model_instance,
)
from backend.app.services.model_queue import (
    ModelQueueOverviewResponse,
    ModelQueueTask,
    build_model_queue_overview,
    cancel_model_task,
    cancel_model_tasks,
    record_model_task,
    update_model_task,
)
from backend.app.services.news_board import NewsBoardResponse, build_news_board
from backend.app.services.notifications import notifier
from backend.app.services.portfolio import PortfolioError, PortfolioService
from backend.app.services.research import ResearchService
from backend.app.services.research_admission import ResearchAdmissionError
from backend.app.services.research_cancellation import cancel_research_tasks
from backend.app.services.research_queue import (
    NewsExtractionQueueResponse,
    ResearchQueueResponse,
    build_research_queue,
)
from backend.app.services.source_filter import filter_news_items
from backend.app.storage import (
    ensure_assets,
    get_asset,
    get_event,
    get_event_research_run,
    get_news,
    get_recommendation,
    get_run,
    list_active_runs,
    list_assets,
    list_event_research_runs,
    list_events,
    list_evolutions,
    list_news,
    list_outcomes,
    list_recent_events,
    list_recommendations,
    list_retries_for_run,
    list_runs,
    normalize_legacy_akshare_timestamps,
    save_evolution,
    save_run,
)
from backend.app.worker import (
    DEFAULT_MODEL_TASK_PRIORITY,
    cancel_news_extraction_task,
    celery_app,
    clear_news_extraction_queue,
    enqueue_asset_mapping,
    enqueue_event_research_retry,
    enqueue_full_event_research,
    enqueue_news_extraction_retry,
    enqueue_research,
    enqueue_scan,
    evolve_failures,
    get_news_extraction_queue,
    get_scan_status,
    request_scan_pause,
    reserve_research_run,
    resume_scan,
)
from backend.app.worker import evolve_from_outcomes as evolve_from_outcomes_task
from backend.app.worker import execute_evolution as execute_evolution_task

settings = get_settings()
logger = logging.getLogger(__name__)

MODEL_QUEUE_SNAPSHOT_LIMIT = 500
MODEL_QUEUE_SNAPSHOT_TTL_SECONDS = 5.0
MODEL_QUEUE_SNAPSHOT_REDIS_KEY = "market-loop:model-queue-overview:snapshot:v3"
_model_queue_snapshot: tuple[float, ModelQueueOverviewResponse] | None = None
_model_queue_refreshing = False
_model_queue_snapshot_lock = Lock()
_model_queue_snapshot_ready = Condition(_model_queue_snapshot_lock)
_failed_research_retry_lock = Lock()


def _build_model_queue_snapshot() -> ModelQueueOverviewResponse:
    models = {
        "extract": settings.ollama_extract_model,
        "research": settings.ollama_research_model,
        "assist": settings.ollama_assist_model,
        "code": settings.ollama_code_model,
    }
    inference_statuses = {
        lane: gateway.queue_status(model, lane=lane) for lane, model in models.items()
    }
    threads = {
        lane: gateway.num_threads_for(model, lane=lane) for lane, model in models.items()
    }
    with SessionLocal() as db:
        result = build_model_queue_overview(
            db,
            extraction_queue=get_news_extraction_queue(200),
            inference_statuses=inference_statuses,
            threads=threads,
            limit=MODEL_QUEUE_SNAPSHOT_LIMIT,
            settings=settings,
        )
    return ModelQueueOverviewResponse.model_validate(result)


def _refresh_model_queue_snapshot() -> ModelQueueOverviewResponse:
    global _model_queue_refreshing, _model_queue_snapshot
    try:
        snapshot = _build_model_queue_snapshot()
        with _model_queue_snapshot_ready:
            _model_queue_snapshot = (monotonic(), snapshot)
        if not settings.database_url.startswith("sqlite"):
            try:
                Redis.from_url(
                    settings.redis_url,
                    socket_connect_timeout=0.5,
                    socket_timeout=1,
                ).set(MODEL_QUEUE_SNAPSHOT_REDIS_KEY, snapshot.model_dump_json())
            except Exception:
                logger.warning("model queue snapshot persistence failed", exc_info=True)
        return snapshot
    finally:
        with _model_queue_snapshot_ready:
            _model_queue_refreshing = False
            _model_queue_snapshot_ready.notify_all()


def _refresh_model_queue_snapshot_in_background() -> None:
    try:
        _refresh_model_queue_snapshot()
    except Exception:
        logger.exception("model queue snapshot refresh failed")


def _cached_model_queue_snapshot() -> ModelQueueOverviewResponse:
    global _model_queue_refreshing
    with _model_queue_snapshot_ready:
        cached = _model_queue_snapshot
        if cached is None and _model_queue_refreshing:
            _model_queue_snapshot_ready.wait(timeout=5)
            cached = _model_queue_snapshot
        warming = cached is None and _model_queue_refreshing
        stale = cached is None or monotonic() - cached[0] >= MODEL_QUEUE_SNAPSHOT_TTL_SECONDS
        should_refresh = stale and not _model_queue_refreshing
        if should_refresh:
            _model_queue_refreshing = True
    if cached is None:
        if warming:
            raise HTTPException(503, "model queue snapshot is warming up")
        return _refresh_model_queue_snapshot()
    if should_refresh:
        Thread(
            target=_refresh_model_queue_snapshot_in_background,
            name="model-queue-snapshot",
            daemon=True,
        ).start()
    return cached[1]


def _model_queue_snapshot_for_limit(
    snapshot: ModelQueueOverviewResponse, limit: int
) -> ModelQueueOverviewResponse:
    if limit >= MODEL_QUEUE_SNAPSHOT_LIMIT:
        return snapshot
    queues = []
    for queue in snapshot.queues:
        visible_limit = min(limit, 200) if queue.id == "extract" else limit
        queues.append(
            queue.model_copy(
                update={
                    "tasks": queue.tasks[:visible_limit],
                    "truncated": queue.truncated or len(queue.tasks) > visible_limit,
                }
            )
        )
    return snapshot.model_copy(update={"queues": queues})


def _reset_model_queue_snapshot() -> None:
    global _model_queue_refreshing, _model_queue_snapshot
    with _model_queue_snapshot_lock:
        _model_queue_snapshot = None
        _model_queue_refreshing = False


def _load_persisted_model_queue_snapshot() -> None:
    global _model_queue_snapshot
    try:
        raw = Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=0.5,
            socket_timeout=1,
        ).get(MODEL_QUEUE_SNAPSHOT_REDIS_KEY)
        if raw:
            snapshot = ModelQueueOverviewResponse.model_validate_json(raw)
            with _model_queue_snapshot_lock:
                _model_queue_snapshot = (0.0, snapshot)
    except Exception:
        logger.warning("persisted model queue snapshot load failed", exc_info=True)


def _start_model_queue_snapshot_refresh() -> None:
    global _model_queue_refreshing
    with _model_queue_snapshot_lock:
        if _model_queue_refreshing:
            return
        _model_queue_refreshing = True
    Thread(
        target=_refresh_model_queue_snapshot_in_background,
        name="model-queue-snapshot",
        daemon=True,
    ).start()


def _provider_registry(db: Session | None = None) -> ProviderRegistry:
    active = ProviderRegistry()
    if db is not None:
        active.add_assets(list_assets(db))
    return active


@asynccontextmanager
async def lifespan(app: FastAPI):
    _reset_model_queue_snapshot()
    init_db()
    with SessionLocal() as db:
        normalize_legacy_akshare_timestamps(db)
        seed_integrations(db, settings)
        startup_registry = _provider_registry(db)
        AssetUniverseService(db, startup_registry).seed_industries()
        ensure_assets(db, startup_registry.all_assets(), commit=False)
        db.commit()
    if not settings.database_url.startswith("sqlite"):
        _load_persisted_model_queue_snapshot()
        _start_model_queue_snapshot_refresh()
    try:
        yield
    finally:
        _reset_model_queue_snapshot()


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
app.include_router(integrations_router)
app.include_router(assets_router)


class ResearchRequest(BaseModel):
    asset_id: str
    event_id: UUID | None = None
    as_of: datetime | None = None
    historical_replay: bool = False
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


class ResearchCancellationRequest(BaseModel):
    task_id: str
    kind: str
    entity_id: str | None = None


class ModelTaskRetryRequest(BaseModel):
    task_id: str
    kind: str
    entity_id: str | None = None
    instance_id: str | None = None


class FailedResearchRetryResult(BaseModel):
    kind: Literal["asset", "event"]
    source_run_id: UUID
    run_id: UUID | None = None
    task_id: str | None = None
    status: Literal["queued", "skipped", "failed"]
    detail: str | None = None


class FailedResearchRetryResponse(BaseModel):
    requested: int
    retried: int
    skipped: int
    failed: int
    results: list[FailedResearchRetryResult]


@app.get("/health")
def health() -> dict:
    provider_settings = get_effective_settings()
    database = False
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        database = True
    except Exception:
        pass
    latest_news = None
    latest_discovery = None
    try:
        with SessionLocal() as db:
            latest_news = db.scalar(select(func.max(NewsRow.observed_at)))
            latest_discovery = db.scalar(select(func.max(NewsSourceStateRow.last_success_at)))
    except Exception:
        pass
    if latest_news and latest_news.tzinfo is None:
        latest_news = latest_news.replace(tzinfo=utc_now().tzinfo)
    if latest_discovery and latest_discovery.tzinfo is None:
        latest_discovery = latest_discovery.replace(tzinfo=utc_now().tzinfo)
    freshness_at = max(
        (value for value in (latest_news, latest_discovery) if value is not None),
        default=None,
    )
    news_age_seconds = (utc_now() - freshness_at).total_seconds() if freshness_at else None
    data_fresh = bool(
        news_age_seconds is not None
        and news_age_seconds <= settings.scan_interval_minutes * 180
    )
    ollama_instances: list[dict] = []
    models: list[str] = []
    endpoints = [
        (instance.id, instance.url, lane_model(settings, lane))
        for lane in ("extract", "assist", "research", "code")
        for instance in configured_model_instances(lane, settings)
    ]
    seen_endpoints: set[tuple[str, str]] = set()
    for instance_id, url, required_model in endpoints:
        if (instance_id, url) in seen_endpoints:
            continue
        seen_endpoints.add((instance_id, url))
        healthy = False
        available_models: list[str] = []
        loaded_models: list[str] = []
        try:
            response = httpx.get(f"{url}/api/tags", timeout=2)
            response.raise_for_status()
            available_models = [item["name"] for item in response.json().get("models", [])]
            healthy = True
            try:
                running = httpx.get(f"{url}/api/ps", timeout=2)
                running.raise_for_status()
                loaded_models = [
                    item["name"] for item in running.json().get("models", [])
                ]
            except Exception:
                pass
        except Exception:
            pass
        models.extend(available_models)
        ollama_instances.append(
            {
                "id": instance_id,
                "healthy": healthy,
                "model_available": (
                    bool(available_models)
                    if required_model is None
                    else required_model in available_models
                ),
                "model_loaded": (
                    bool(loaded_models)
                    if required_model is None
                    else required_model in loaded_models
                ),
            }
        )
    models = list(dict.fromkeys(models))
    ollama = bool(ollama_instances and ollama_instances[0]["healthy"])
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
        "status": (
            "ok" if database and redis_ok and ollama and data_fresh else "degraded"
        ),
        "database": database,
        "redis": redis_ok,
        "task_failure_rate": task_failure_rate,
        "ollama": ollama,
        "models": models,
        "ollama_instances": ollama_instances,
        "fmp_configured": provider_settings.fmp_enabled,
        "fmp_mcp_configured": bool(settings.fmp_mcp_url),
        "latest_news_at": latest_news,
        "latest_news_discovery_at": latest_discovery,
        "news_age_seconds": news_age_seconds,
        "data_fresh": data_fresh,
        "telegram_configured": bool(settings.telegram_bot_token and settings.telegram_chat_id),
        "evolution_enabled": settings.evolution_enabled,
        "evolution_auto_merge": settings.evolution_auto_merge,
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


@app.get("/api/v1/news-board", response_model=NewsBoardResponse)
def news_board(
    per_source: int = Query(default=50, ge=1, le=50),
    db: Session = Depends(get_db),
):
    extraction_queue = get_news_extraction_queue(limit=200)
    extraction_items = (
        extraction_queue.get("items", [])
        if extraction_queue.get("state") in {"queued", "running", "paused"}
        else []
    )
    return build_news_board(
        db,
        per_source=per_source,
        extraction_items=extraction_items,
    )


@app.post("/api/v1/scan")
def start_scan(request: ScanRequest, db: Session = Depends(get_db)):
    if request.background:
        task_id, status = enqueue_scan()
        return {"task_id": task_id, "status": status, "scan": get_scan_status()}
    active_registry = _provider_registry(db)
    since = utc_now() - timedelta(minutes=settings.scan_interval_minutes * 2)
    items = active_registry.discover_news(since=since, limit=settings.scan_batch_size)
    accepted_items, filtered = filter_news_items(db, items)
    created = EventService(active_registry).ingest(db, accepted_items)
    return {
        "news": len(items),
        "accepted": len(accepted_items),
        "filtered": filtered,
        "events": len(created),
    }


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
    active_registry = _provider_registry(db)
    asset = get_asset(db, request.asset_id) or active_registry.get_asset(request.asset_id)
    if not asset:
        raise HTTPException(404, "asset not found")
    if request.background:
        event = get_event(db, request.event_id) if request.event_id else None
        try:
            task_id, run = enqueue_research(
                db,
                asset,
                event,
                request.as_of,
                historical_replay=request.historical_replay,
            )
        except ResearchAdmissionError as exc:
            raise HTTPException(409, exc.detail()) from exc
        return {"task_id": task_id, "run_id": str(run.id), "status": "queued"}
    event = get_event(db, request.event_id) if request.event_id else None
    # Release the read-only transaction before first-use checkpoint DDL. Build
    # the service before reserving the visible run, so constructor failures
    # cannot leave a permanently queued reservation behind.
    db.rollback()
    service = ResearchService(active_registry, db)
    try:
        queued_run = reserve_research_run(
            db,
            asset,
            event,
            request.as_of,
            historical_replay=request.historical_replay,
        )
    except ResearchAdmissionError as exc:
        raise HTTPException(409, exc.detail()) from exc
    try:
        return service.run(
            asset,
            event,
            request.as_of,
            historical_replay=request.historical_replay,
            queued_run=queued_run,
        )
    except Exception as exc:
        db.rollback()
        queued_run.status = RunStatus.FAILED
        queued_run.error = f"{type(exc).__name__}: synchronous research failed"
        queued_run.completed_at = utc_now()
        try:
            save_run(db, queued_run)
        except Exception:
            logger.exception("synchronous research failure state could not be saved")
        raise


@app.get("/api/v1/research-runs")
def research_runs(limit: int = Query(default=100, ge=1, le=500), db: Session = Depends(get_db)):
    return list_runs(db, limit)


@app.get("/api/v1/research-queue", response_model=ResearchQueueResponse)
def research_queue(
    limit: int = Query(default=500, ge=1, le=1000), db: Session = Depends(get_db)
):
    return build_research_queue(list_active_runs(db), limit, settings.ollama_research_model)


@app.get("/api/v1/news-extraction-queue", response_model=NewsExtractionQueueResponse)
def news_extraction_queue(limit: int = Query(default=200, ge=1, le=200)):
    return get_news_extraction_queue(limit)


@app.get("/api/v1/model-inference-queues")
def model_inference_queues():
    model_specs = (
        {
            "lane": "assist",
            "model": settings.ollama_assist_model,
            "purpose": "股票映射",
            "binding": "新闻事件二次股票映射",
            "task_enabled": True,
        },
        {
            "lane": "code",
            "model": settings.ollama_code_model,
            "purpose": "代码演进",
            "binding": (
                "代码演进任务 · 自动合并开启"
                if settings.evolution_auto_merge
                else "代码演进任务 · 自动合并关闭"
            ),
            "task_enabled": settings.evolution_enabled,
        },
    )
    items = []
    for spec in model_specs:
        lane = str(spec["lane"])
        model = str(spec["model"])
        status = gateway.queue_status(model, lane=lane)
        if not status["observable"]:
            state = "unavailable"
        elif status["queued"]:
            state = "queued"
        elif status["running"]:
            state = "running"
        else:
            state = "idle"
        items.append(
            {
                **spec,
                **status,
                "threads": gateway.num_threads_for(model, lane=lane),
                "state": state,
            }
        )
    return {"generated_at": utc_now(), "items": items}


@app.get("/api/v1/model-queue-overview", response_model=ModelQueueOverviewResponse)
def model_queue_overview(
    limit: int = Query(default=500, ge=1, le=500),
):
    return _model_queue_snapshot_for_limit(_cached_model_queue_snapshot(), limit)


def _revoke_model_tasks(task_ids: list[str]) -> int:
    revoked = 0
    for task_id in task_ids:
        try:
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            revoked += 1
        except Exception:
            logger.warning("model task revoke failed: %s", task_id, exc_info=True)
    return revoked


def _purge_model_queue(queue_name: str) -> int:
    try:
        with celery_app.connection_for_write() as connection:
            return int(connection.default_channel.queue_purge(queue_name) or 0)
    except Exception:
        logger.warning("model queue purge failed: %s", queue_name, exc_info=True)
        return 0


def _revoke_research_tasks(task_ids: list[str]) -> int:
    return _revoke_model_tasks(task_ids)


def _purge_research_queue() -> int:
    return _purge_model_queue("research")


def _mark_model_queue_snapshot_stale() -> None:
    global _model_queue_refreshing, _model_queue_snapshot
    with _model_queue_snapshot_ready:
        if _model_queue_snapshot is not None:
            _model_queue_snapshot = (0.0, _model_queue_snapshot[1])
        if _model_queue_refreshing:
            return
        _model_queue_refreshing = True
    Thread(
        target=_refresh_model_queue_snapshot_in_background,
        name="model-queue-snapshot",
        daemon=True,
    ).start()


@app.post("/api/v1/model-queues/research/tasks/cancel", status_code=202)
def cancel_research_task(
    request: ResearchCancellationRequest,
    db: Session = Depends(get_db),
):
    if request.kind not in {"asset_research", "event_research"}:
        raise HTTPException(422, "only research tasks can be cancelled")
    try:
        result = cancel_research_tasks(
            db,
            kind=request.kind,
            entity_id=request.entity_id,
            task_id=request.task_id,
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if result.cancelled == 0:
        raise HTTPException(404, "active research task not found")
    revoked = _revoke_research_tasks(result.celery_task_ids)
    _mark_model_queue_snapshot_stale()
    return {
        **result.model_dump(exclude={"celery_task_ids"}),
        "revoked": revoked,
    }


@app.post("/api/v1/model-queues/assist/tasks/cancel", status_code=202)
def cancel_asset_mapping_task(request: ResearchCancellationRequest):
    if request.kind != "asset_mapping":
        raise HTTPException(422, "only asset mapping tasks can be cancelled")
    if not cancel_model_task("assist", request.task_id):
        raise HTTPException(404, "active asset mapping task not found")
    revoked = _revoke_model_tasks([request.task_id])
    _mark_model_queue_snapshot_stale()
    return {
        "queue_id": "assist",
        "cancelled": 1,
        "revoked": revoked,
    }


@app.post("/api/v1/model-queues/research/clear", status_code=202)
def clear_research_tasks(db: Session = Depends(get_db)):
    result = cancel_research_tasks(db, include_failed=True)
    purged = _purge_research_queue()
    revoked = _revoke_research_tasks(result.celery_task_ids)
    _mark_model_queue_snapshot_stale()
    return {
        **result.model_dump(exclude={"celery_task_ids"}),
        "purged": purged,
        "revoked": revoked,
    }


@app.post("/api/v1/model-queues/{queue_id}/clear", status_code=202)
def clear_model_queue(queue_id: str, db: Session = Depends(get_db)):
    queue_names = {
        "extract": "extract",
        "research": "research",
        "assist": "mapping",
        "code": "evolution",
    }
    if queue_id not in queue_names:
        raise HTTPException(422, "unknown model queue")
    if queue_id == "research":
        return clear_research_tasks(db)
    if queue_id == "extract":
        result = clear_news_extraction_queue()
        tracked = cancel_model_tasks("extract", include_failed=True)
        cancelled = int(result["cancelled"]) + tracked.cancelled
        task_ids = list(
            dict.fromkeys([*result["celery_task_ids"], *tracked.celery_task_ids])
        )
    else:
        result = cancel_model_tasks(
            "assist" if queue_id == "assist" else "code",
            include_failed=True,
        )
        cancelled = result.cancelled
        task_ids = result.celery_task_ids
    purged = _purge_model_queue(queue_names[queue_id])
    revoked = _revoke_model_tasks(task_ids)
    _mark_model_queue_snapshot_stale()
    return {
        "queue_id": queue_id,
        "cancelled": cancelled,
        "purged": purged,
        "revoked": revoked,
    }


RETRYABLE_MODEL_QUEUES = {"extract", "research", "assist", "code"}
MANUAL_RETRY_PRIORITY = 0
BULK_RETRY_PRIORITY = DEFAULT_MODEL_TASK_PRIORITY


def _retryable_model_queue_tasks(
    queue_id: str, instance_id: str | None = None
) -> list[ModelQueueTask]:
    overview = _build_model_queue_snapshot()
    queue = next((item for item in overview.queues if item.id == queue_id), None)
    if queue is None:
        return []
    tasks = queue.tasks
    if instance_id is not None:
        instance = next(
            (item for item in queue.instances if item.id == instance_id), None
        )
        if instance is None:
            return []
        tasks = instance.tasks
    return [task for task in tasks if task.error]


def _enqueue_model_task_retry(
    db: Session,
    *,
    queue_id: str,
    task: ModelQueueTask,
    priority: int,
) -> str:
    if queue_id == "extract":
        if not task.entity_id:
            raise HTTPException(409, "news extraction task has no durable news id")
        try:
            news_uuid = UUID(task.entity_id)
        except ValueError as exc:
            raise HTTPException(422, "invalid news extraction entity_id") from exc
        news = get_news(db, news_uuid)
        if news is None:
            raise HTTPException(409, "source news no longer exists")
        if cancel_model_task("extract", task.task_id):
            _revoke_model_tasks([task.task_id])
        cancel_news_extraction_task(
            task_id=task.task_id,
            news_id=task.entity_id,
        )
        return enqueue_news_extraction_retry(
            news,
            priority=priority,
            model_instance_id=task.instance_id,
        )

    if queue_id == "assist":
        if task.kind != "asset_mapping" or not task.entity_id:
            raise HTTPException(422, "only asset mapping tasks can be retried")
        try:
            event_uuid = UUID(task.entity_id)
        except ValueError as exc:
            raise HTTPException(422, "invalid asset mapping entity_id") from exc
        event = get_event(db, event_uuid)
        if event is None:
            raise HTTPException(409, "source event no longer exists")
        if cancel_model_task("assist", task.task_id):
            _revoke_model_tasks([task.task_id])
        task_id = enqueue_asset_mapping(
            db,
            event,
            force=True,
            priority=priority,
            model_instance_id=task.instance_id,
        )
        if task_id is None:
            raise HTTPException(409, "asset mapping retry was not queued")
        return task_id

    if queue_id == "code":
        candidate_id: UUID | None = None
        if task.entity_id:
            try:
                candidate_id = UUID(task.entity_id)
            except ValueError as exc:
                raise HTTPException(422, "invalid evolution candidate entity_id") from exc
            if not any(item.id == candidate_id for item in list_evolutions(db)):
                raise HTTPException(409, "evolution candidate no longer exists")
        if cancel_model_task("code", task.task_id):
            _revoke_model_tasks([task.task_id])
        task_id = str(uuid4())
        instance = select_model_instance(
            "code",
            task_id=task_id,
            preferred=task.instance_id,
            settings=settings,
            probe_health=True,
        )
        record_model_task(
            "code",
            task_id=task_id,
            kind="code_evolution",
            entity_id=str(candidate_id) if candidate_id else None,
            title=task.title,
            subtitle=task.subtitle,
            source="manual",
            instance_id=instance.id,
        )
        code_task = (
            execute_evolution_task if candidate_id else evolve_from_outcomes_task
        )
        try:
            published = code_task.apply_async(
                args=[str(candidate_id)] if candidate_id else [],
                kwargs={"model_instance_id": instance.id},
                queue=broker_queue_name("code", instance.id),
                task_id=task_id,
                priority=priority,
            )
        except Exception as exc:
            update_model_task(
                "code",
                task_id,
                status="failed",
                source="manual",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        return str(published.id)

    if task.kind == "asset_research":
        try:
            failed_run_id = UUID(task.task_id)
        except ValueError as exc:
            raise HTTPException(422, "invalid asset research task_id") from exc
        failed_run = get_run(db, failed_run_id)
        if failed_run is None:
            raise HTTPException(404, "asset research run not found")
        original = (
            get_run(db, failed_run.retry_of_run_id)
            if failed_run.retry_of_run_id is not None
            else failed_run
        )
        if original is None:
            raise HTTPException(409, "original asset research run no longer exists")
        if original.status is not RunStatus.FAILED and original.retryable_reason is None:
            raise HTTPException(409, "asset research run is no longer retryable")
        retries = list_retries_for_run(db, original.id)
        active_statuses = {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}
        if any(item.status in active_statuses for item in retries):
            raise HTTPException(409, "an asset research retry is already active")
        event = get_event(db, original.event_id) if original.event_id else None
        if original.event_id and event is None:
            raise HTTPException(409, "source event no longer exists")
        retry_attempt = max([item.retry_attempt for item in retries] or [0]) + 1
        try:
            task_id, _ = enqueue_research(
                db,
                original.asset,
                event,
                as_of=utc_now(),
                historical_replay=False,
                retry_of_run_id=original.id,
                retry_attempt=retry_attempt,
                priority=priority,
                model_instance_id=task.instance_id,
            )
        except ResearchAdmissionError as exc:
            raise HTTPException(409, exc.detail()) from exc
        return task_id

    if task.kind == "event_research":
        try:
            run_uuid = UUID(task.task_id)
        except ValueError as exc:
            raise HTTPException(422, "invalid event research task_id") from exc
        run = get_event_research_run(db, run_uuid)
        if run is None:
            raise HTTPException(404, "event research run not found")
        if run.status is not RunStatus.FAILED and run.retryable_reason is None:
            raise HTTPException(409, "event research run is no longer retryable")
        event = get_event(db, run.event_id)
        if event is None:
            raise HTTPException(409, "source event no longer exists")
        task_id, _ = enqueue_event_research_retry(
            db,
            event,
            run,
            priority=priority,
            model_instance_id=task.instance_id,
        )
        return task_id

    raise HTTPException(422, "only research tasks can be retried in this queue")


@app.post("/api/v1/model-queues/{queue_id}/tasks/retry", status_code=202)
def retry_model_queue_task(
    queue_id: str,
    request: ModelTaskRetryRequest,
    db: Session = Depends(get_db),
):
    if queue_id not in RETRYABLE_MODEL_QUEUES:
        raise HTTPException(422, "this model queue does not support manual retry")
    task = next(
        (
            item
            for item in _retryable_model_queue_tasks(queue_id)
            if item.task_id == request.task_id
            and item.kind == request.kind
            and item.entity_id == request.entity_id
            and (
                request.instance_id is None
                or item.instance_id == request.instance_id
            )
        ),
        None,
    )
    if task is None:
        raise HTTPException(404, "retryable model task not found")
    task_id = _enqueue_model_task_retry(
        db,
        queue_id=queue_id,
        task=task,
        priority=MANUAL_RETRY_PRIORITY,
    )
    _mark_model_queue_snapshot_stale()
    return {
        "queue_id": queue_id,
        "requested": 1,
        "retried": 1,
        "skipped": 0,
        "task_ids": [task_id],
        "priority": "highest",
    }


@app.post("/api/v1/model-queues/{queue_id}/retry", status_code=202)
def retry_model_queue_tasks(queue_id: str, db: Session = Depends(get_db)):
    if queue_id not in RETRYABLE_MODEL_QUEUES:
        raise HTTPException(422, "this model queue does not support bulk retry")
    tasks = _retryable_model_queue_tasks(queue_id)
    retried_task_ids: list[str] = []
    skipped = 0
    for task in tasks:
        try:
            retried_task_ids.append(
                _enqueue_model_task_retry(
                    db,
                    queue_id=queue_id,
                    task=task,
                    priority=BULK_RETRY_PRIORITY,
                )
            )
        except HTTPException:
            db.rollback()
            skipped += 1
    _mark_model_queue_snapshot_stale()
    return {
        "queue_id": queue_id,
        "requested": len(tasks),
        "retried": len(retried_task_ids),
        "skipped": skipped,
        "task_ids": retried_task_ids,
        "priority": "normal",
    }


@app.post(
    "/api/v1/model-queues/{queue_id}/instances/{instance_id}/retry",
    status_code=202,
)
def retry_model_instance_tasks(
    queue_id: str,
    instance_id: str,
    db: Session = Depends(get_db),
):
    if queue_id not in RETRYABLE_MODEL_QUEUES:
        raise HTTPException(422, "this model queue does not support bulk retry")
    tasks = _retryable_model_queue_tasks(queue_id, instance_id)
    if not tasks:
        overview = _build_model_queue_snapshot()
        queue = next((item for item in overview.queues if item.id == queue_id), None)
        if queue is None or not any(item.id == instance_id for item in queue.instances):
            raise HTTPException(404, "model instance queue not found")
    retried_task_ids: list[str] = []
    skipped = 0
    for task in tasks:
        try:
            retried_task_ids.append(
                _enqueue_model_task_retry(
                    db,
                    queue_id=queue_id,
                    task=task,
                    priority=BULK_RETRY_PRIORITY,
                )
            )
        except HTTPException:
            db.rollback()
            skipped += 1
    _mark_model_queue_snapshot_stale()
    return {
        "queue_id": queue_id,
        "instance_id": instance_id,
        "requested": len(tasks),
        "retried": len(retried_task_ids),
        "skipped": skipped,
        "task_ids": retried_task_ids,
        "priority": "normal",
    }


@app.post(
    "/api/v1/model-queues/{queue_id}/instances/{instance_id}/clear",
    status_code=202,
)
def clear_model_instance_queue(
    queue_id: str,
    instance_id: str,
    db: Session = Depends(get_db),
):
    lane_by_queue = {
        "extract": "extract",
        "research": "research",
        "assist": "assist",
        "code": "code",
    }
    lane = lane_by_queue.get(queue_id)
    if lane is None:
        raise HTTPException(422, "unknown model queue")
    overview = _build_model_queue_snapshot()
    queue = next((item for item in overview.queues if item.id == queue_id), None)
    if queue is None or not any(item.id == instance_id for item in queue.instances):
        raise HTTPException(404, "model instance queue not found")
    if queue_id == "research":
        result = cancel_research_tasks(
            db,
            include_failed=True,
            instance_id=instance_id,
        )
        cancelled = result.cancelled
        task_ids = result.celery_task_ids
    elif queue_id == "extract":
        extracted = clear_news_extraction_queue(instance_id=instance_id)
        tracked = cancel_model_tasks(
            "extract",
            include_failed=True,
            instance_id=instance_id,
        )
        cancelled = int(extracted["cancelled"]) + tracked.cancelled
        task_ids = list(
            dict.fromkeys([*extracted["celery_task_ids"], *tracked.celery_task_ids])
        )
    else:
        tracked = cancel_model_tasks(
            "assist" if queue_id == "assist" else "code",
            include_failed=True,
            instance_id=instance_id,
        )
        cancelled = tracked.cancelled
        task_ids = tracked.celery_task_ids
    purged = _purge_model_queue(broker_queue_name(lane, instance_id))
    revoked = _revoke_model_tasks(task_ids)
    _mark_model_queue_snapshot_stale()
    return {
        "queue_id": queue_id,
        "instance_id": instance_id,
        "cancelled": cancelled,
        "purged": purged,
        "revoked": revoked,
    }


@app.get("/api/v1/research-runs/{run_id}")
def research_run(run_id: UUID, db: Session = Depends(get_db)):
    value = get_run(db, run_id)
    if not value:
        raise HTTPException(404, "run not found")
    return value


@app.get("/api/v1/failed-research-runs")
def failed_research_runs(
    limit: int = Query(default=50, ge=1, le=200), db: Session = Depends(get_db)
):
    candidates = failed_research_candidates(db)
    return [item.api_item() for item in reversed(candidates)][:limit]


def _available_research_instance(instance_id: str | None) -> str | None:
    if instance_id is None:
        return None
    instance = next(
        (
            item
            for item in configured_model_instances("research", settings)
            if item.id == instance_id
        ),
        None,
    )
    if instance is None:
        raise HTTPException(404, "research instance not found")
    healthy, model_available = instance_health(
        instance,
        lane_model(settings, "research"),
    )
    if not healthy or not model_available:
        raise HTTPException(409, "research instance is unavailable")
    return instance.id


def _queue_asset_research_retry(
    run_id: UUID,
    db: Session,
    known_retries: tuple[ResearchRun, ...] | None = None,
    model_instance_id: str | None = None,
) -> dict:
    original = get_run(db, run_id)
    if not original:
        raise HTTPException(404, "run not found")
    if original.status is not RunStatus.FAILED and original.retryable_reason is None:
        raise HTTPException(409, "only failed or model-degraded research runs can be retried")
    if original.retry_of_run_id is not None:
        raise HTTPException(409, "retry the original failed research run")
    retries = (
        list_retries_for_run(db, original.id)
        if known_retries is None
        else known_retries
    )
    active_statuses = {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}
    if any(item.status in active_statuses for item in retries):
        raise HTTPException(409, "a retry is already queued or running")
    event = get_event(db, original.event_id) if original.event_id else None
    if original.event_id and event is None:
        raise HTTPException(409, "source event no longer exists")
    retry_attempt = max([item.retry_attempt for item in retries] or [0]) + 1
    try:
        task_id, run = enqueue_research(
            db,
            original.asset,
            event,
            as_of=utc_now(),
            historical_replay=False,
            retry_of_run_id=original.id,
            retry_attempt=retry_attempt,
            priority=MANUAL_RETRY_PRIORITY if model_instance_id else None,
            model_instance_id=model_instance_id,
        )
    except ResearchAdmissionError as exc:
        raise HTTPException(409, exc.detail()) from exc
    return {
        "task_id": task_id,
        "run_id": str(run.id),
        "retry_of_run_id": str(original.id),
        "retry_attempt": retry_attempt,
        "instance_id": run.model_instance_id,
        "status": "queued",
    }


def _queue_event_research_retry(
    run_id: UUID,
    db: Session,
    model_instance_id: str | None = None,
) -> dict:
    run = get_event_research_run(db, run_id)
    if not run:
        raise HTTPException(404, "event research run not found")
    if run.status is not RunStatus.FAILED and run.retryable_reason is None:
        raise HTTPException(
            409, "only failed or model-degraded event research runs can be retried"
        )
    event = get_event(db, run.event_id)
    if not event:
        raise HTTPException(409, "source event no longer exists")
    retry_options = (
        {
            "priority": MANUAL_RETRY_PRIORITY,
            "model_instance_id": model_instance_id,
        }
        if model_instance_id
        else {}
    )
    task_id, run = enqueue_event_research_retry(db, event, run, **retry_options)
    return {
        "task_id": task_id,
        "run_id": str(run.id),
        "retry_count": run.retry_count,
        "instance_id": run.model_instance_id,
        "status": "queued",
    }


@app.post(
    "/api/v1/failed-research-runs/retry",
    response_model=FailedResearchRetryResponse,
    status_code=202,
)
def retry_failed_research_runs(db: Session = Depends(get_db)):
    with _failed_research_retry_lock:
        candidates = failed_research_candidates(db)
        results: list[FailedResearchRetryResult] = []
        for candidate in candidates:
            try:
                queued = (
                    _queue_asset_research_retry(
                        candidate.source_run_id,
                        db,
                        candidate.retries,
                    )
                    if candidate.kind == "asset"
                    else _queue_event_research_retry(candidate.source_run_id, db)
                )
                results.append(
                    FailedResearchRetryResult(
                        kind=candidate.kind,
                        source_run_id=candidate.source_run_id,
                        run_id=UUID(queued["run_id"]),
                        task_id=queued["task_id"],
                        status="queued",
                    )
                )
            except HTTPException as exc:
                db.rollback()
                results.append(
                    FailedResearchRetryResult(
                        kind=candidate.kind,
                        source_run_id=candidate.source_run_id,
                        status="skipped",
                        detail=str(exc.detail),
                    )
                )
            except Exception as exc:
                db.rollback()
                logger.exception(
                    "bulk failed-research retry could not enqueue %s %s",
                    candidate.kind,
                    candidate.source_run_id,
                )
                results.append(
                    FailedResearchRetryResult(
                        kind=candidate.kind,
                        source_run_id=candidate.source_run_id,
                        status="failed",
                        detail=f"{type(exc).__name__}: research retry queue failed",
                    )
                )
        return FailedResearchRetryResponse(
            requested=len(candidates),
            retried=sum(item.status == "queued" for item in results),
            skipped=sum(item.status == "skipped" for item in results),
            failed=sum(item.status == "failed" for item in results),
            results=results,
        )


@app.post("/api/v1/conclusions/{recommendation_id}/research", status_code=202)
def research_conclusion_again(
    recommendation_id: UUID,
    db: Session = Depends(get_db),
):
    recommendation = get_recommendation(db, recommendation_id)
    if recommendation is None:
        raise HTTPException(404, "recommendation not found")
    source_run = get_run(db, recommendation.run_id)
    if source_run is None:
        raise HTTPException(409, "source research run no longer exists")
    event = get_event(db, source_run.event_id) if source_run.event_id else None
    if source_run.event_id is not None and event is None:
        raise HTTPException(409, "source event no longer exists")
    try:
        task_id, run = enqueue_research(
            db,
            recommendation.asset,
            event,
            as_of=utc_now(),
            historical_replay=False,
            force_research=True,
            queue_phase="forced_research_queue",
        )
    except ResearchAdmissionError as exc:
        raise HTTPException(409, exc.detail()) from exc
    return {
        "task_id": task_id,
        "run_id": str(run.id),
        "source_recommendation_id": str(recommendation.id),
        "status": "queued",
    }


@app.post("/api/v1/event-conclusions/{run_id}/research", status_code=202)
def research_event_conclusion_again(
    run_id: UUID,
    db: Session = Depends(get_db),
):
    with _failed_research_retry_lock:
        run = get_event_research_run(db, run_id)
        if run is None:
            raise HTTPException(404, "event conclusion not found")
        if full_event_research_is_active(run):
            raise HTTPException(
                409,
                {
                    "code": "event_research_already_active",
                    "message": "该事件已有完整重新研究任务正在执行。",
                    "active_run_id": str(run.id),
                },
            )
        if run.status in {
            RunStatus.QUEUED,
            RunStatus.RUNNING,
            RunStatus.VERIFYING,
        }:
            raise HTTPException(
                409,
                {
                    "code": "event_research_already_active",
                    "message": "该事件已有排队中或执行中的研究任务。",
                    "active_run_id": str(run.id),
                },
            )
        if run.status not in {
            RunStatus.COMPLETED,
            RunStatus.INSUFFICIENT_EVIDENCE,
            RunStatus.FAILED,
        }:
            raise HTTPException(409, "event conclusion is not refreshable")
        if run.report is None:
            raise HTTPException(409, "event conclusion has no report")
        event = get_event(db, run.event_id)
        if event is None:
            raise HTTPException(409, "source event no longer exists")
        available_news = [
            news_id for news_id in event.news_item_ids if get_news(db, news_id) is not None
        ]
        if not available_news:
            raise HTTPException(
                409,
                "该事件没有可用的关联原始新闻，无法执行完整重新研究。",
            )
        task_id, refreshed = enqueue_full_event_research(db, event, run)
        return {
            "task_id": task_id,
            "run_id": str(refreshed.id),
            "source_run_id": str(run_id),
            "status": "queued",
            "stage": "event_extraction",
        }


@app.post("/api/v1/research-runs/{run_id}/retry", status_code=202)
def retry_research_run(
    run_id: UUID,
    instance_id: str | None = Query(default=None, min_length=1, max_length=64),
    db: Session = Depends(get_db),
):
    selected_instance_id = _available_research_instance(instance_id)
    with _failed_research_retry_lock:
        return _queue_asset_research_retry(
            run_id,
            db,
            model_instance_id=selected_instance_id,
        )


@app.post("/api/v1/event-research-runs/{run_id}/retry", status_code=202)
def retry_event_research_run(
    run_id: UUID,
    instance_id: str | None = Query(default=None, min_length=1, max_length=64),
    db: Session = Depends(get_db),
):
    selected_instance_id = _available_research_instance(instance_id)
    with _failed_research_retry_lock:
        return _queue_event_research_retry(
            run_id,
            db,
            model_instance_id=selected_instance_id,
        )


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
        steps = (
            event_run.analysis_steps
            if event_run
            else (
                run.analysis_steps
                if run and run.analysis_steps
                else (event.analysis_steps if event else [])
            )
        )
        model_names = list(dict.fromkeys(step.model for step in steps if step.model))
        recommendation = run.recommendation if run else None
        report = event_run.report if event_run else None
        asset = (
            run.asset
            if run
            else (
                None
                if event_run
                else (event.candidates[0].asset if event and event.candidates else None)
            )
        )
        status = (
            run.status.value
            if run
            else (event_run.status.value if event_run else _event_mapping_status(event))
        )
        updated_at = (
            run.updated_at
            if run
            else (
                event_run.updated_at
                if event_run
                else max([event.observed_at, *[step.occurred_at for step in event.analysis_steps]])
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
                    "actions": [item.model_dump(mode="json") for item in event.actions],
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
                    "direction_score": recommendation.direction_score,
                    "raw_score": recommendation.raw_score,
                    "confidence": recommendation.confidence,
                    "rating_confidence": recommendation.rating_confidence,
                    "fact_confidence": recommendation.fact_confidence,
                    "news_confidence": recommendation.news_confidence,
                    "news_confidence_factors": (
                        recommendation.news_confidence_factors.model_dump(mode="json")
                        if recommendation.news_confidence_factors
                        else None
                    ),
                    "rating_confidence_factors": (
                        recommendation.rating_confidence_factors.model_dump(mode="json")
                        if recommendation.rating_confidence_factors
                        else None
                    ),
                    "mapping_distance": recommendation.mapping_distance,
                    "score_source": recommendation.score_source.value,
                    "scoring_version": recommendation.scoring_version,
                    "horizon_unit": recommendation.horizon_unit.value,
                    "horizon_days": recommendation.horizon_days,
                    "evidence_complete": recommendation.evidence_complete,
                    "directional_evidence_complete": (
                        recommendation.directional_evidence_complete
                    ),
                    "signal_status": recommendation.signal_status.value,
                    "summary": recommendation.thesis.summary,
                    "impact": (
                        recommendation.impact.model_dump(mode="json")
                        if recommendation.impact
                        else None
                    ),
                }
                if recommendation
                else (
                    {
                        "kind": "event_report",
                        "confidence": report.confidence,
                        "news_confidence": report.news_confidence,
                        "news_confidence_factors": (
                            report.news_confidence_factors.model_dump(mode="json")
                            if report.news_confidence_factors
                            else None
                        ),
                        "evidence_complete": report.evidence_complete,
                        "summary": report.summary,
                        "affected_markets": report.affected_markets,
                        "affected_sectors": report.affected_sectors,
                        "scenarios": report.scenarios,
                        "catalysts": report.catalysts,
                        "risks": report.risks,
                        "unresolved_questions": report.unresolved_questions,
                        "scoring_version": report.scoring_version,
                        "fact_confidence": report.fact_confidence,
                        "macro_factors": [
                            item.model_dump(mode="json") for item in report.macro_factors
                        ],
                        "impacts": [
                            item.model_dump(mode="json") for item in report.impacts
                        ],
                        "trade_status": report.trade_status.value,
                        "missing_information": report.missing_information,
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
def analysis_logs(limit: int = Query(default=10, ge=1, le=50), db: Session = Depends(get_db)):
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
    return PortfolioService(_provider_registry(db)).snapshot(db)


@app.post("/api/v1/paper-orders", response_model=PaperOrder)
def paper_order(request: PaperOrderRequest, db: Session = Depends(get_db)):
    recommendation = next(
        (item for item in list_recommendations(db, 1000) if item.id == request.recommendation_id),
        None,
    )
    if not recommendation:
        raise HTTPException(404, "recommendation not found")
    try:
        order = PortfolioService(_provider_registry(db)).create_from_recommendation(
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
        task_id = str(uuid4())
        instance = select_model_instance(
            "code", task_id=task_id, settings=settings, probe_health=True
        )
        record_model_task(
            "code",
            task_id=task_id,
            kind="code_evolution",
            title=f"失败案例代码演进（{len(request.failures)} 条）",
            subtitle="等待生成改进方案",
            source="manual",
            instance_id=instance.id,
        )
        try:
            task = evolve_failures.apply_async(
                args=[request.failures],
                kwargs={"model_instance_id": instance.id},
                queue=broker_queue_name("code", instance.id),
                task_id=task_id,
            )
        except Exception as exc:
            update_model_task(
                "code",
                task_id,
                status="failed",
                source="manual",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        return {"task_id": task.id, "status": "queued"}
    try:
        return EvolutionService().propose(db, request.failures)
    except EvolutionError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/v1/evolution/{candidate_id}/execute", response_model=EvolutionCandidate | dict)
def execute_evolution(candidate_id: UUID, background: bool = True, db: Session = Depends(get_db)):
    candidate = next((item for item in list_evolutions(db) if item.id == candidate_id), None)
    if not candidate:
        raise HTTPException(404, "evolution candidate not found")
    if background:
        task_id = str(uuid4())
        instance = select_model_instance(
            "code", task_id=task_id, settings=settings, probe_health=True
        )
        record_model_task(
            "code",
            task_id=task_id,
            kind="code_evolution",
            entity_id=str(candidate.id),
            title=candidate.hypothesis,
            subtitle=candidate.target_metric,
            source="manual",
            instance_id=instance.id,
        )
        try:
            task = execute_evolution_task.apply_async(
                args=[str(candidate_id)],
                kwargs={"model_instance_id": instance.id},
                queue=broker_queue_name("code", instance.id),
                task_id=task_id,
            )
        except Exception as exc:
            update_model_task(
                "code",
                task_id,
                status="failed",
                entity_id=str(candidate.id),
                source="manual",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
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
                        item.model_dump(mode="json") for item in list_event_research_runs(db, 10)
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
