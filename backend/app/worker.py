from __future__ import annotations

import json
from datetime import datetime, timedelta
from time import sleep
from typing import Any
from uuid import UUID, uuid4

from celery import Celery
from celery.signals import task_failure, task_success
from redis import Redis
from sqlalchemy import func, select

from backend.app.config import get_settings
from backend.app.db import NewsRow, SessionLocal, init_db
from backend.app.domain import (
    AnalysisStep,
    AssetRef,
    EventResearchRun,
    NewsEvent,
    Rating,
    ResearchRun,
    RunStatus,
    as_utc,
    utc_now,
)
from backend.app.model_audit import cleanup_model_audits
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.asset_mapping import AssetMappingService
from backend.app.services.event_research import EventResearchService
from backend.app.services.events import EventService
from backend.app.services.evolution import EvolutionService
from backend.app.services.notifications import notifier
from backend.app.services.outcomes import OutcomeService
from backend.app.services.portfolio import PortfolioService
from backend.app.services.research import ResearchService
from backend.app.services.source_filter import filter_news_items
from backend.app.services.source_lineage import canonicalize_url
from backend.app.storage import (
    get_asset,
    get_event,
    get_event_research_for_event,
    get_event_research_run,
    get_news,
    get_run,
    get_run_for_event_asset,
    list_assets,
    list_evolutions,
    list_outcomes,
    save_event,
    save_event_research_run,
    save_run,
    upsert_asset,
)

settings = get_settings()
SCAN_GATE_KEY = "market-loop:scan:active"
SCAN_LOCK_KEY = "market-loop:scan:lock"
SCAN_PAUSE_KEY = "market-loop:scan:pause"
SCAN_STATUS_KEY = "market-loop:scan:status"
SCAN_VISIBILITY_TIMEOUT_SECONDS = max(
    12 * 60 * 60,
    settings.scan_interval_minutes * 180,
)
SCAN_GATE_TTL_SECONDS = SCAN_VISIBILITY_TIMEOUT_SECONDS
celery_app = Celery("market-loop", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_transport_options={
        "visibility_timeout": SCAN_VISIBILITY_TIMEOUT_SECONDS,
    },
    result_backend_transport_options={
        "visibility_timeout": SCAN_VISIBILITY_TIMEOUT_SECONDS,
    },
    visibility_timeout=SCAN_VISIBILITY_TIMEOUT_SECONDS,
    beat_schedule={
        "ensure-news-scan-loop": {
            "task": "market_loop.ensure_scan_loop",
            "schedule": 5,
            "options": {"queue": "io"},
        },
        "refresh-crypto-universe": {
            "task": "market_loop.refresh_crypto_universe",
            "schedule": 6 * 60 * 60,
            "options": {"queue": "io"},
        },
        "evaluate-outcomes": {
            "task": "market_loop.evaluate_outcomes",
            "schedule": 24 * 60 * 60,
            "options": {"queue": "io"},
        },
        "cleanup-model-audits": {
            "task": "market_loop.cleanup_model_audits",
            "schedule": 24 * 60 * 60,
            "options": {"queue": "io"},
        },
        "evolve-from-failures": {
            "task": "market_loop.evolve_from_outcomes",
            "schedule": 7 * 24 * 60 * 60,
            "options": {"queue": "evolution"},
        },
        "system-monitor": {
            "task": "market_loop.monitor_health",
            "schedule": 5 * 60,
            "options": {"queue": "evolution"},
        },
    },
)


class ScanLeaseLost(RuntimeError):
    """The task is stale and must stop without changing the active scan state."""


if not settings.evolution_enabled:
    celery_app.conf.beat_schedule.pop("evolve-from-failures", None)
    celery_app.conf.beat_schedule.pop("system-monitor", None)


@celery_app.task(name="market_loop.cleanup_model_audits")
def cleanup_model_audit_records() -> dict[str, int]:
    init_db()
    with SessionLocal() as db:
        deleted = cleanup_model_audits(db, settings.model_audit_retention_days)
    return {"deleted": deleted, "retention_days": settings.model_audit_retention_days}


def _record_task_result(kind: str) -> None:
    try:
        client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.5)
        key = f"market-loop:tasks:{kind}"
        client.incr(key)
        client.expire(key, 3600)
    except Exception:
        pass


def _redis_client() -> Redis:
    return Redis.from_url(settings.redis_url, socket_connect_timeout=1)


def _default_scan_status() -> dict[str, Any]:
    return {
        "state": "idle",
        "task_id": None,
        "phase": None,
        "paused_from_phase": None,
        "current": 0,
        "total": 0,
        "started_at": None,
        "last_completed_at": None,
        "next_scan_at": None,
        "last_result": None,
        "last_error": None,
    }


def _decode(value: bytes | str | None) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else value


def _read_scan_status(client: Redis) -> dict[str, Any]:
    payload = _default_scan_status()
    raw = _decode(client.get(SCAN_STATUS_KEY))
    if raw:
        try:
            stored = json.loads(raw)
            if isinstance(stored, dict):
                payload.update(stored)
        except (TypeError, json.JSONDecodeError):
            pass
    return payload


def _update_scan_status(client: Redis, **updates: Any) -> dict[str, Any]:
    payload = _read_scan_status(client)
    payload.update(updates)
    client.set(SCAN_STATUS_KEY, json.dumps(payload, ensure_ascii=False, default=str))
    return payload


def _renew_scan_gate(client: Redis, task_id: str) -> bool:
    """Keep a live long-running scan from being duplicated after its lease expires."""

    if _decode(client.get(SCAN_GATE_KEY)) != task_id:
        return False
    return bool(client.expire(SCAN_GATE_KEY, SCAN_GATE_TTL_SECONDS))


def _claim_scan_gate(client: Redis, task_id: str) -> bool:
    """Claim an empty gate or renew a redelivered task's own existing gate."""

    current = _decode(client.get(SCAN_GATE_KEY))
    if current == task_id:
        return _renew_scan_gate(client, task_id)
    if current:
        return False
    if client.set(SCAN_GATE_KEY, task_id, nx=True, ex=SCAN_GATE_TTL_SECONDS):
        return True
    return _decode(client.get(SCAN_GATE_KEY)) == task_id and _renew_scan_gate(
        client, task_id
    )


def _require_scan_gate(client: Redis, task_id: str) -> None:
    if not _renew_scan_gate(client, task_id):
        raise ScanLeaseLost(f"scan lease no longer belongs to {task_id}")


def get_scan_status() -> dict[str, Any]:
    """Return the shared scan lifecycle with a server clock for UI countdowns."""

    now = utc_now()
    try:
        payload = _read_scan_status(_redis_client())
    except Exception as exc:
        payload = _default_scan_status()
        payload.update(
            state="failed",
            last_error=f"scan state unavailable: {type(exc).__name__}",
        )
    return {
        **payload,
        "interval_seconds": settings.scan_interval_minutes * 60,
        "server_time": now.isoformat(),
    }


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return as_utc(datetime.fromisoformat(value))
    except (TypeError, ValueError):
        return None


def _complete_scan(
    client: Redis,
    task_id: str,
    result: dict[str, Any],
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    if _decode(client.get(SCAN_GATE_KEY)) != task_id:
        return _read_scan_status(client)
    completed_at = completed_at or utc_now()
    payload = _update_scan_status(
        client,
        state="idle",
        task_id=task_id,
        phase="completed",
        paused_from_phase=None,
        current=int(result.get("discovered", 0)),
        total=int(result.get("discovered", 0)),
        last_completed_at=completed_at.isoformat(),
        next_scan_at=(
            completed_at + timedelta(minutes=settings.scan_interval_minutes)
        ).isoformat(),
        last_result=result,
        last_error=None,
    )
    _clear_scan_gate(client, task_id)
    _clear_scan_pause(client, task_id)
    return payload


def _clear_scan_gate(client: Redis, task_id: str) -> None:
    current = _decode(client.get(SCAN_GATE_KEY))
    if current == task_id:
        client.delete(SCAN_GATE_KEY)


def _clear_scan_pause(client: Redis, task_id: str) -> None:
    current = _decode(client.get(SCAN_PAUSE_KEY))
    if current == task_id:
        client.delete(SCAN_PAUSE_KEY)


def request_scan_pause() -> dict[str, Any]:
    """Request a cooperative pause at the next safe scan checkpoint."""

    client = _redis_client()
    task_id = _decode(client.get(SCAN_GATE_KEY))
    if not task_id:
        raise RuntimeError("no active scan")
    status = _read_scan_status(client)
    paused_from_phase = (
        status.get("paused_from_phase")
        if status.get("state") == "paused"
        else status.get("phase")
    )
    client.set(SCAN_PAUSE_KEY, task_id, ex=SCAN_GATE_TTL_SECONDS)
    return _update_scan_status(
        client,
        state="paused",
        phase="paused",
        paused_from_phase=paused_from_phase or "discovering",
        task_id=task_id,
        next_scan_at=None,
    )


def resume_scan() -> dict[str, Any]:
    """Resume a cooperatively paused scan."""

    client = _redis_client()
    task_id = _decode(client.get(SCAN_GATE_KEY))
    if not task_id:
        raise RuntimeError("no active scan")
    status = _read_scan_status(client)
    _clear_scan_pause(client, task_id)
    return _update_scan_status(
        client,
        state="running",
        phase=status.get("paused_from_phase") or "discovering",
        paused_from_phase=None,
        task_id=task_id,
        next_scan_at=None,
    )


def _wait_if_scan_paused(
    client: Redis,
    task_id: str,
    *,
    phase: str,
    current: int,
    total: int,
) -> None:
    """Block only between durable scan units, keeping the task lease alive."""

    _require_scan_gate(client, task_id)
    if _decode(client.get(SCAN_PAUSE_KEY)) != task_id:
        return
    _update_scan_status(
        client,
        state="paused",
        phase="paused",
        paused_from_phase=phase,
        current=current,
        total=total,
        next_scan_at=None,
    )
    while _decode(client.get(SCAN_PAUSE_KEY)) == task_id:
        _require_scan_gate(client, task_id)
        client.expire(SCAN_PAUSE_KEY, SCAN_GATE_TTL_SECONDS)
        sleep(0.25)
    _update_scan_status(
        client,
        state="running",
        phase=phase,
        paused_from_phase=None,
        current=current,
        total=total,
        next_scan_at=None,
    )


def enqueue_scan() -> tuple[str, str]:
    """Queue at most one manual/scheduled scan across API processes."""

    client = _redis_client()
    existing = _decode(client.get(SCAN_GATE_KEY))
    if existing:
        return existing, "already_queued"
    client.delete(SCAN_PAUSE_KEY)
    task_id = str(uuid4())
    claimed = client.set(SCAN_GATE_KEY, task_id, nx=True, ex=SCAN_GATE_TTL_SECONDS)
    if not claimed:
        existing = _decode(client.get(SCAN_GATE_KEY))
        return (existing or task_id), "already_queued"
    _update_scan_status(
        client,
        state="queued",
        task_id=task_id,
        phase="queued",
        paused_from_phase=None,
        current=0,
        total=0,
        started_at=None,
        next_scan_at=None,
        last_error=None,
    )
    try:
        scan_news.apply_async(queue="io", task_id=task_id)
    except Exception as exc:
        now = utc_now()
        _update_scan_status(
            client,
            state="failed",
            phase="queue_failed",
            next_scan_at=(now + timedelta(minutes=settings.scan_interval_minutes)).isoformat(),
            last_error=f"{type(exc).__name__}",
        )
        _clear_scan_gate(client, task_id)
        raise
    return task_id, "queued"


def enqueue_research(
    db,
    asset: AssetRef,
    event: NewsEvent | None = None,
    as_of: datetime | None = None,
    historical_replay: bool = False,
) -> tuple[str, ResearchRun]:
    """Persist a visible queued run before handing work to the LLM worker."""

    run = ResearchRun(
        event_id=event.id if event else None,
        asset=asset,
        as_of=as_of or utc_now(),
        historical_replay=historical_replay,
        analysis_steps=[
            *(event.analysis_steps if event else []),
            AnalysisStep(
                phase="research_queue",
                executor="celery",
                summary=f"已为主标的 {asset.symbol} 创建深度研究任务。",
            ),
        ],
    )
    save_run(db, run)
    try:
        task = research_asset.apply_async(
            args=[asset.asset_id, str(event.id) if event else None, str(run.id)],
            queue="llm",
        )
    except Exception as exc:
        run.status = RunStatus.FAILED
        run.error = f"{type(exc).__name__}: research queue failed"
        run.updated_at = utc_now()
        run.analysis_steps.append(
            AnalysisStep(
                phase="research_queue",
                status="failed",
                executor="celery",
                summary=f"研究任务入队失败（{type(exc).__name__}）。",
            )
        )
        save_run(db, run)
        raise
    return str(task.id), run


def enqueue_event_researches(
    db, event: NewsEvent, limit: int
) -> list[tuple[str, ResearchRun]]:
    """Queue distinct event-assets in relevance order, up to the requested limit."""

    queued: list[tuple[str, ResearchRun]] = []
    for candidate in event.candidates[:limit]:
        existing = get_run_for_event_asset(db, event.id, candidate.asset.asset_id)
        if existing:
            event_urls = {
                canonicalize_url(item.url)
                for news_id in event.news_item_ids
                if (item := get_news(db, news_id)) is not None
            }
            researched_urls = {
                canonicalize_url(item.source_url) for item in existing.evidence
            }
            has_new_cluster_evidence = bool(event_urls - researched_urls)
            if (
                existing.status is not RunStatus.INSUFFICIENT_EVIDENCE
                or not has_new_cluster_evidence
            ):
                continue
        queued.append(enqueue_research(db, candidate.asset, event))
    return queued


def enqueue_event_research(db, event: NewsEvent) -> tuple[str, ResearchRun] | None:
    """Queue exactly the highest-relevance mapped asset for one unique event."""

    queued = enqueue_event_researches(db, event, 1)
    return queued[0] if queued else None


def _replace_event_step(event: NewsEvent, step: AnalysisStep) -> None:
    for index in range(len(event.analysis_steps) - 1, -1, -1):
        if event.analysis_steps[index].phase == step.phase:
            event.analysis_steps[index] = step
            return
    event.analysis_steps.append(step)


def enqueue_asset_mapping(db, event: NewsEvent) -> str | None:
    """Queue one visible 7B mapping attempt for an otherwise unmapped event."""

    if event.candidates or any(
        step.phase == "asset_mapping_queue" and step.status in {"queued", "completed"}
        for step in event.analysis_steps
    ):
        return None
    _replace_event_step(
        event,
        AnalysisStep(
            phase="asset_mapping_queue",
            status="queued",
            executor="celery",
            model=settings.ollama_research_model,
            summary="确定性映射未找到标的，已创建 7B 二次标的发现任务。",
        ),
    )
    save_event(db, event)
    try:
        task = resolve_event_assets.apply_async(args=[str(event.id)], queue="llm")
    except Exception as exc:
        _replace_event_step(
            event,
            AnalysisStep(
                phase="asset_mapping_queue",
                status="failed",
                executor="celery",
                model=settings.ollama_research_model,
                summary=f"7B 标的发现任务入队失败（{type(exc).__name__}）。",
            ),
        )
        save_event(db, event)
        raise
    return str(task.id)


def enqueue_event_report(db, event: NewsEvent) -> tuple[str | None, EventResearchRun]:
    """Persist and queue one neutral report for an event with no verified asset."""

    existing = get_event_research_for_event(db, event.id)
    if existing:
        return None, existing
    run = EventResearchRun(
        event_id=event.id,
        as_of=max(event.as_of, event.observed_at),
        analysis_steps=[
            *event.analysis_steps,
            AnalysisStep(
                phase="event_research_queue",
                status="queued",
                executor="celery",
                model=settings.ollama_research_model,
                summary="未找到经主数据验证的证券标的，已创建中性事件研报任务。",
            ),
        ],
    )
    save_event_research_run(db, run)
    try:
        task = research_event.apply_async(
            args=[str(event.id), str(run.id)],
            queue="llm",
        )
    except Exception as exc:
        run.status = RunStatus.FAILED
        run.error = f"{type(exc).__name__}: event research queue failed"
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_research_queue",
                status="failed",
                executor="celery",
                summary=f"中性事件研报入队失败（{type(exc).__name__}）。",
            )
        )
        save_event_research_run(db, run)
        raise
    return str(task.id), run


@celery_app.task(name="market_loop.ensure_scan_loop")
def ensure_scan_loop() -> dict:
    """Start immediately when uninitialized, then ten minutes after completion."""

    client = _redis_client()
    active = _decode(client.get(SCAN_GATE_KEY))
    if active:
        return {"status": "active", "task_id": active}
    status = _read_scan_status(client)
    next_scan_at = _parse_timestamp(status.get("next_scan_at"))
    if next_scan_at and utc_now() < next_scan_at:
        return {"status": "waiting", "next_scan_at": next_scan_at.isoformat()}
    task_id, enqueue_status = enqueue_scan()
    return {"status": enqueue_status, "task_id": task_id}


@task_success.connect
def _task_succeeded(**kwargs) -> None:
    _record_task_result("success")


@task_failure.connect
def _task_failed(**kwargs) -> None:
    _record_task_result("failure")


@celery_app.task(bind=True, name="market_loop.scan_news", max_retries=3)
def scan_news(self) -> dict:
    task_id = str(self.request.id)
    client = _redis_client()
    if not _claim_scan_gate(client, task_id):
        return {"status": "already_running", "discovered": 0, "events": 0}
    lock = client.lock(
        SCAN_LOCK_KEY,
        timeout=SCAN_GATE_TTL_SECONDS,
        blocking_timeout=0,
    )
    if not lock.acquire(blocking=False):
        return {"status": "already_running", "discovered": 0, "events": 0}
    try:
        started_at = utc_now()
        _update_scan_status(
            client,
            state="running",
            task_id=task_id,
            phase="discovering",
            current=0,
            total=0,
            started_at=started_at.isoformat(),
            next_scan_at=None,
            last_error=None,
        )
        _wait_if_scan_paused(
            client,
            task_id,
            phase="discovering",
            current=0,
            total=0,
        )
        init_db()
        registry = ProviderRegistry()
        since = utc_now() - timedelta(minutes=settings.scan_interval_minutes * 2)
        items = registry.discover_news(since=since, limit=settings.scan_batch_size)
        with SessionLocal() as db:
            accepted_items, filtered_count = filter_news_items(db, items)
        _require_scan_gate(client, task_id)
        self.update_state(
            state="PROGRESS",
            meta={"phase": "extracting", "current": 0, "total": len(accepted_items)},
        )
        _update_scan_status(
            client,
            state="running",
            phase="extracting",
            current=0,
            total=len(accepted_items),
        )
        _wait_if_scan_paused(
            client,
            task_id,
            phase="extracting",
            current=0,
            total=len(accepted_items),
        )

        def update_progress(current: int, total: int) -> None:
            _require_scan_gate(client, task_id)
            self.update_state(
                state="PROGRESS",
                meta={"phase": "extracting", "current": current, "total": total},
            )
            _update_scan_status(
                client,
                state="running",
                phase="extracting",
                current=current,
                total=total,
            )
            _wait_if_scan_paused(
                client,
                task_id,
                phase="extracting",
                current=current,
                total=total,
            )

        with SessionLocal() as db:
            registry.add_assets(list_assets(db))
            service = EventService(registry)
            events = service.ingest(db, accepted_items, progress=update_progress)
            for error in registry.last_errors:
                notifier.send(f"数据源故障：{error}")
            for event in events:
                if event.priority >= 0.75:
                    notifier.send(
                        f"高优先级事件：{event.headline}\n类型：{event.event_type.value}\n"
                        f"候选标的：{', '.join(item.asset.symbol for item in event.candidates[:5]) or '待解析'}"
                    )
            queued = 0
            mapping_queued = 0
            if settings.auto_research:
                for event_index, event in enumerate(events):
                    _wait_if_scan_paused(
                        client,
                        task_id,
                        phase="queuing",
                        current=event_index,
                        total=len(events),
                    )
                    if event.candidates:
                        try:
                            queued += int(enqueue_event_research(db, event) is not None)
                        except Exception as exc:
                            notifier.send(
                                f"研究任务入队失败：{event.headline}\n错误：{type(exc).__name__}"
                            )
                    else:
                        try:
                            mapping_queued += int(enqueue_asset_mapping(db, event) is not None)
                        except Exception as exc:
                            notifier.send(
                                f"标的发现任务入队失败：{event.headline}\n错误：{type(exc).__name__}"
                            )
        result = {
            "status": "completed",
            "discovered": len(items),
            "accepted": len(accepted_items),
            "filtered": filtered_count,
            "events": len(events),
            "research_queued": queued,
            "asset_mapping_queued": mapping_queued,
        }
        _complete_scan(client, task_id, result)
        return result
    except ScanLeaseLost:
        return {"status": "superseded", "discovered": 0, "events": 0}
    except Exception as exc:
        if self.request.retries < self.max_retries:
            _update_scan_status(
                client,
                state="retrying",
                task_id=task_id,
                phase="retrying",
                last_error=f"{type(exc).__name__}",
            )
            raise self.retry(
                exc=exc, countdown=min(60, 2 ** (self.request.retries + 1))
            ) from exc
        failed_at = utc_now()
        _update_scan_status(
            client,
            state="failed",
            task_id=task_id,
            phase="failed",
            next_scan_at=(
                failed_at + timedelta(minutes=settings.scan_interval_minutes)
            ).isoformat(),
            last_error=f"{type(exc).__name__}",
        )
        _clear_scan_gate(client, task_id)
        _clear_scan_pause(client, task_id)
        raise
    finally:
        if lock.owned():
            lock.release()


@celery_app.task(bind=True, name="market_loop.resolve_event_assets", max_retries=2)
def resolve_event_assets(self, event_id: str) -> dict:
    init_db()
    with SessionLocal() as db:
        event = get_event(db, UUID(event_id))
        if not event:
            raise ValueError(f"unknown event: {event_id}")
        registry = ProviderRegistry(assets=list_assets(db))
        try:
            mapping_result = None
            if not event.candidates:
                _replace_event_step(
                    event,
                    AnalysisStep(
                        phase="asset_mapping",
                        status="running",
                        executor="ollama+provider-registry",
                        model=settings.ollama_research_model,
                        summary="7B 正在从原文提及中识别证券，并通过主数据验证代码。",
                    ),
                )
                save_event(db, event)
                news_items = [
                    item
                    for news_id in event.news_item_ids
                    if (item := get_news(db, news_id)) is not None
                ]
                mapping_result = AssetMappingService(registry).map_event(event, news_items)
                event.candidates = mapping_result.candidates
                for candidate in event.candidates:
                    upsert_asset(db, candidate.asset)
                _replace_event_step(
                    event,
                    AnalysisStep(
                        phase="asset_mapping",
                        status="completed" if event.candidates else "unmapped",
                        executor="ollama+provider-registry",
                        model=settings.ollama_research_model,
                        summary=(
                            f"7B 提出 {mapping_result.proposed_count} 个候选，"
                            f"主数据验证通过 {len(event.candidates)} 个、拒绝 "
                            f"{mapping_result.rejected_count} 个。"
                        ),
                        metrics={
                            "proposed_count": mapping_result.proposed_count,
                            "verified_count": len(event.candidates),
                            "rejected_count": mapping_result.rejected_count,
                            "provider_errors": registry.mapping_errors,
                            "no_asset_reason": mapping_result.no_asset_reason,
                        },
                    ),
                )
                _replace_event_step(
                    event,
                    AnalysisStep(
                        phase="asset_mapping_queue",
                        status="completed",
                        executor="celery",
                        model=settings.ollama_research_model,
                        summary="7B 二次标的发现任务已完成。",
                    ),
                )
                save_event(db, event)

            if event.candidates:
                queued = enqueue_event_researches(db, event, 3)
                return {
                    "status": "mapped",
                    "event_id": event_id,
                    "verified_assets": len(event.candidates),
                    "research_queued": len(queued),
                }

            task_id, run = enqueue_event_report(db, event)
            return {
                "status": "event_research_queued",
                "event_id": event_id,
                "event_research_run_id": str(run.id),
                "task_id": task_id,
            }
        except Exception as exc:
            db.rollback()
            event = get_event(db, UUID(event_id)) or event
            retrying = self.request.retries < self.max_retries
            _replace_event_step(
                event,
                AnalysisStep(
                    phase="asset_mapping",
                    status="retrying" if retrying else "failed",
                    executor="ollama+provider-registry",
                    model=settings.ollama_research_model,
                    summary=(
                        f"7B 标的发现{'暂时失败，等待重试' if retrying else '最终失败'}"
                        f"（{type(exc).__name__}）。"
                    ),
                ),
            )
            save_event(db, event)
            if retrying:
                raise self.retry(
                    exc=exc,
                    countdown=min(60, 2 ** (self.request.retries + 1)),
                ) from exc
            raise


@celery_app.task(bind=True, name="market_loop.research_event", max_retries=2)
def research_event(self, event_id: str, run_id: str) -> dict:
    init_db()
    with SessionLocal() as db:
        event = get_event(db, UUID(event_id))
        run = get_event_research_run(db, UUID(run_id))
        if not event or not run:
            raise ValueError(f"unknown event research run: {run_id}")
        if run.status in {RunStatus.COMPLETED, RunStatus.INSUFFICIENT_EVIDENCE}:
            return run.model_dump(mode="json")
        try:
            result = EventResearchService(db).run(event, run)
            notifier.send(
                f"事件研究完成：{event.headline}\n"
                f"证据{'完整' if result.report and result.report.evidence_complete else '不足'}"
            )
            return result.model_dump(mode="json")
        except Exception as exc:
            db.rollback()
            run = get_event_research_run(db, UUID(run_id)) or run
            retrying = self.request.retries < self.max_retries
            run.status = RunStatus.QUEUED if retrying else RunStatus.FAILED
            run.error = f"{type(exc).__name__}: {exc}"
            run.analysis_steps.append(
                AnalysisStep(
                    phase="event_research_failed",
                    status="retrying" if retrying else "failed",
                    executor="event-research",
                    model=settings.ollama_research_model,
                    summary=(
                        f"中性事件研报{'暂时失败，等待重试' if retrying else '最终失败'}"
                        f"（{type(exc).__name__}）。"
                    ),
                )
            )
            save_event_research_run(db, run)
            if retrying:
                raise self.retry(
                    exc=exc,
                    countdown=min(60, 2 ** (self.request.retries + 1)),
                ) from exc
            raise


@celery_app.task(
    name="market_loop.research_asset", autoretry_for=(Exception,), retry_backoff=True, max_retries=2
)
def research_asset(
    asset_id: str, event_id: str | None = None, run_id: str | None = None
) -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        registry.add_assets(list_assets(db))
        asset = get_asset(db, asset_id) or registry.get_asset(asset_id)
        if not asset:
            raise ValueError(f"unknown asset: {asset_id}")
        event = get_event(db, UUID(event_id)) if event_id else None
        queued_run = get_run(db, UUID(run_id)) if run_id else None
    # PostgresSaver.setup() may run concurrent index DDL on first use. Close the
    # read transaction above first so that DDL cannot wait on its own task.
    with SessionLocal() as db:
        run = ResearchService(registry, db).run(
            asset,
            event,
            as_of=queued_run.as_of if queued_run else None,
            historical_replay=queued_run.historical_replay if queued_run else False,
            queued_run=queued_run,
        )
        if run.recommendation:
            notifier.recommendation(run.recommendation)
            if settings.auto_paper_trade and run.recommendation.rating in {
                Rating.BULLISH,
                Rating.STRONGLY_BULLISH,
            }:
                portfolio = PortfolioService(registry)
                price = portfolio.current_price(run.recommendation)
                if price > 0:
                    order = portfolio.create_from_recommendation(db, run.recommendation, price)
                    notifier.send(
                        f"模拟仓位变化：{order.side.value} {order.asset.symbol} "
                        f"{order.quantity:g} @ {order.price:g} {order.currency}"
                    )
        return run.model_dump(mode="json")


@celery_app.task(name="market_loop.refresh_crypto_universe")
def refresh_crypto_universe() -> dict:
    init_db()
    registry = ProviderRegistry()
    assets = registry.refresh_crypto_universe()
    with SessionLocal() as db:
        for asset in assets:
            upsert_asset(db, asset)
    return {"assets": len(assets)}


@celery_app.task(name="market_loop.evaluate_outcomes")
def evaluate_outcomes() -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        outcomes = OutcomeService(registry).evaluate_due(db)
    return {"outcomes": len(outcomes)}


@celery_app.task(name="market_loop.seed_assets")
def seed_assets() -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        for asset in registry.all_assets():
            upsert_asset(db, asset)
        count = len(list_assets(db))
    return {"assets": count}


@celery_app.task(name="market_loop.evolve_from_outcomes")
def evolve_from_outcomes() -> dict:
    if not settings.evolution_enabled:
        return {"status": "disabled"}
    init_db()
    with SessionLocal() as db:
        failures = [
            item.model_dump(mode="json")
            for item in list_outcomes(db)
            if not item.direction_correct or item.alpha < 0
        ][:50]
        if not failures:
            return {"status": "no-failures"}
    return evolve_failures.run(failures)


@celery_app.task(name="market_loop.evolve_failures")
def evolve_failures(failures: list[dict]) -> dict:
    if not settings.evolution_enabled:
        return {"status": "disabled"}
    init_db()
    with SessionLocal() as db:
        service = EvolutionService()
        candidate = service.propose(db, failures)
        result = service.execute(db, candidate)
        notifier.send(f"演进结果：{result.status.value}\n分支：{result.branch}\n假设：{result.hypothesis}")
        return result.model_dump(mode="json")


@celery_app.task(name="market_loop.execute_evolution")
def execute_evolution(candidate_id: str) -> dict:
    init_db()
    with SessionLocal() as db:
        candidate = next(
            (item for item in list_evolutions(db) if str(item.id) == candidate_id), None
        )
        if not candidate:
            raise ValueError(f"unknown evolution candidate: {candidate_id}")
        result = EvolutionService().execute(db, candidate)
        notifier.send(f"演进结果：{result.status.value}\n分支：{result.branch}")
        return result.model_dump(mode="json")


@celery_app.task(name="market_loop.monitor_health")
def monitor_health() -> dict:
    init_db()
    client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.5)
    successes = int(client.get("market-loop:tasks:success") or 0)
    failures = int(client.get("market-loop:tasks:failure") or 0)
    total = successes + failures
    failure_rate = failures / total if total else 0.0
    with SessionLocal() as db:
        latest_news = db.scalar(select(func.max(NewsRow.observed_at)))
    stale = bool(
        latest_news
        and (utc_now() - as_utc(latest_news)).total_seconds()
        > settings.scan_interval_minutes * 180
    )
    unhealthy = (total >= 10 and failure_rate > 0.10) or stale
    rolled_back = False
    if unhealthy and settings.evolution_enabled and settings.evolution_auto_merge:
        try:
            EvolutionService().rollback()
            rolled_back = True
            notifier.send(
                f"系统已自动回滚：任务失败率 {failure_rate:.1%}，数据过期：{'是' if stale else '否'}"
            )
        except Exception:
            notifier.send("系统健康门禁触发，但自动回滚失败；请人工检查。")
    return {
        "failure_rate": failure_rate,
        "samples": total,
        "data_stale": stale,
        "rolled_back": rolled_back,
    }
