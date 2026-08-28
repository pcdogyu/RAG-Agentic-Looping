from __future__ import annotations

import json
from datetime import datetime, timedelta
from functools import wraps
from threading import Event, Thread
from time import sleep
from typing import Any
from uuid import UUID, uuid4

from celery import Celery, chord
from celery.exceptions import Ignore, Retry
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
    NewsItem,
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
from backend.app.services.model_instances import (
    ModelLane,
    broker_queue_name,
    instance_assignment,
    instance_health,
    lane_model,
    model_instance_affinity,
    select_model_instance,
    update_instance_assignment,
)
from backend.app.services.model_queue import (
    ACTIVE_STATUSES,
    RECENT_EXECUTION_WINDOW,
    cancel_model_task,
    list_model_task_records,
    model_task_is_cancelled,
    record_model_task,
    stale_model_task_records,
    touch_model_task,
    update_model_task,
)
from backend.app.services.notifications import notifier
from backend.app.services.outcomes import OutcomeService
from backend.app.services.portfolio import PortfolioService
from backend.app.services.research import ResearchService
from backend.app.services.research_admission import (
    ResearchAdmissionError,
    active_asset_research,
    asset_research_admission_lock,
    enforce_asset_research_admission,
)
from backend.app.services.research_lifecycle import (
    compact_queued_research_runs,
    reconcile_stale_research_runs,
    research_lease,
)
from backend.app.services.source_filter import filter_news_items
from backend.app.services.source_lineage import canonicalize_url, enrich_news_lineage
from backend.app.storage import (
    event_news_item_ids,
    get_asset,
    get_event,
    get_event_research_for_event,
    get_event_research_run,
    get_mergeable_queued_run,
    get_news,
    get_news_by_content_hash,
    get_run,
    get_run_for_event_asset,
    list_assets,
    list_event_research_runs,
    list_events,
    list_evolutions,
    list_outcomes,
    list_queued_runs,
    list_recommendations,
    list_runs,
    save_event,
    save_event_research_run,
    save_news,
    save_run,
    upsert_asset,
)

settings = get_settings()
DEFAULT_MODEL_TASK_PRIORITY = 5
ASSET_RESEARCH_PRIORITY = 3
EVENT_RESEARCH_PRIORITY = 7
SCAN_GATE_KEY = "market-loop:scan:active"
SCAN_LOCK_KEY = "market-loop:scan:lock"
SCAN_PAUSE_KEY = "market-loop:scan:pause"
SCAN_STATUS_KEY = "market-loop:scan:status"
NEWS_EXTRACTION_QUEUE_KEY = "market-loop:scan:news-extraction-queue"
NEWS_EXTRACTION_QUEUE_TTL_SECONDS = 12 * 60 * 60
RESEARCH_DISPATCH_PREFIX = "market-loop:research:dispatch:"
RESEARCH_DISPATCH_TTL_SECONDS = 30 * 24 * 60 * 60
SCAN_VISIBILITY_TIMEOUT_SECONDS = max(
    NEWS_EXTRACTION_QUEUE_TTL_SECONDS,
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
    task_default_priority=DEFAULT_MODEL_TASK_PRIORITY,
    task_queue_max_priority=9,
    worker_prefetch_multiplier=1,
    task_routes={
        "market_loop.extract_news_item": {"queue": "extract"},
        "market_loop.retry_news_item": {"queue": "extract"},
        "market_loop.finalize_news_extraction": {"queue": "extract"},
        "market_loop.resolve_event_assets": {"queue": "mapping"},
        "market_loop.research_event": {"queue": "research"},
        "market_loop.research_asset": {"queue": "research"},
    },
    broker_transport_options={
        "visibility_timeout": SCAN_VISIBILITY_TIMEOUT_SECONDS,
        "priority_steps": list(range(10)),
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
        "refresh-event-market-factors": {
            "task": "market_loop.refresh_event_market_factors",
            "schedule": 24 * 60 * 60,
            "options": {"queue": "io"},
        },
        "cleanup-model-audits": {
            "task": "market_loop.cleanup_model_audits",
            "schedule": 24 * 60 * 60,
            "options": {"queue": "io"},
        },
        "reconcile-research-leases": {
            "task": "market_loop.reconcile_research_leases",
            "schedule": 5 * 60,
            "options": {"queue": "io"},
        },
        "reconcile-asset-mapping-leases": {
            "task": "market_loop.reconcile_asset_mapping_leases",
            "schedule": 60,
            "options": {"queue": "io"},
        },
        "evolve-from-failures": {
            "task": "market_loop.dispatch_evolve_from_outcomes",
            "schedule": 7 * 24 * 60 * 60,
            "options": {"queue": "io"},
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


def model_instance_task(lane: ModelLane):
    """Bind a Celery business task to one durable model instance."""

    def decorate(function):
        @wraps(function)
        def wrapped(self, *args, model_instance_id: str | None = None, **kwargs):
            task_id = str(self.request.id or uuid4())
            preferred = model_instance_id or instance_assignment(lane, task_id)
            delivery_info = self.request.delivery_info or {}
            routing_key = str(delivery_info.get("routing_key") or "")
            delivery_priority = delivery_info.get("priority")
            rebalance_preferred = (
                lane == "research"
                and bool(routing_key)
                and getattr(self.request, "retries", 0) == 0
                and delivery_priority != 0
            )
            selected = select_model_instance(
                lane,
                task_id=task_id,
                preferred=preferred,
                settings=settings,
                probe_health=bool(routing_key),
                rebalance_preferred=rebalance_preferred,
            )
            target_queue = broker_queue_name(lane, selected.id)
            if routing_key and not all(instance_health(selected, lane_model(settings, lane))):
                update_instance_assignment(
                    lane,
                    task_id,
                    status="retrying",
                    instance_id=selected.id,
                )
                if lane in {"extract", "assist", "code"}:
                    update_model_task(
                        lane,
                        task_id,
                        status="retrying",
                        instance_id=selected.id,
                    )
                raise self.retry(
                    kwargs={**kwargs, "model_instance_id": selected.id},
                    queue=target_queue,
                    countdown=30,
                    max_retries=1_000_000,
                )
            if routing_key and routing_key != target_queue:
                raise self.retry(
                    kwargs={**kwargs, "model_instance_id": selected.id},
                    queue=target_queue,
                    countdown=1,
                )
            update_instance_assignment(
                lane,
                task_id,
                status="running",
                instance_id=selected.id,
            )
            heartbeat_stop = Event()
            heartbeat_thread = None
            if lane in {"extract", "assist", "code"}:
                update_model_task(
                    lane,
                    task_id,
                    status="running",
                    instance_id=selected.id,
                )

                def refresh_lease() -> None:
                    while not heartbeat_stop.wait(settings.model_task_heartbeat_seconds):
                        if not touch_model_task(lane, task_id, instance_id=selected.id):
                            break

                heartbeat_thread = Thread(
                    target=refresh_lease,
                    name=f"model-task-heartbeat-{lane}-{task_id}",
                    daemon=True,
                )
                heartbeat_thread.start()
            try:
                with model_instance_affinity(lane, selected.id, task_id=task_id):
                    result = function(
                        self,
                        *args,
                        model_instance_id=selected.id,
                        **kwargs,
                    )
            except Retry:
                update_instance_assignment(lane, task_id, status="retrying")
                raise
            except Exception:
                update_instance_assignment(lane, task_id, status="failed")
                raise
            finally:
                heartbeat_stop.set()
                if heartbeat_thread is not None:
                    heartbeat_thread.join(timeout=1)
            update_instance_assignment(lane, task_id, status="completed")
            return result

        return wrapped

    return decorate


@celery_app.task(name="market_loop.cleanup_model_audits")
def cleanup_model_audit_records() -> dict[str, int]:
    init_db()
    with SessionLocal() as db:
        deleted = cleanup_model_audits(db, settings.model_audit_retention_days)
    return {"deleted": deleted, "retention_days": settings.model_audit_retention_days}


@celery_app.task(name="market_loop.reconcile_research_leases")
def reconcile_research_leases() -> dict[str, int]:
    init_db()
    try:
        client = _redis_client()
        client.ping()
    except Exception:
        client = None
    with SessionLocal() as db:
        repaired = reconcile_stale_research_runs(db, client, settings)
        recovery = recover_orphaned_queued_research_runs(db, client, settings)
    return {"repaired": repaired, **recovery}


@celery_app.task(name="market_loop.compact_research_backlog")
def compact_research_backlog(dry_run: bool = True) -> dict[str, int]:
    init_db()
    with SessionLocal() as db:
        return compact_queued_research_runs(db, settings, dry_run=dry_run)


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


def _research_dispatch_key(run_id: str) -> str:
    return f"{RESEARCH_DISPATCH_PREFIX}{run_id}"


def _record_research_dispatch(
    run_id: str,
    task_id: str,
    redis_client: Redis | None = None,
) -> bool:
    try:
        client = redis_client or _redis_client()
        return bool(
            client.set(
                _research_dispatch_key(run_id),
                task_id,
                ex=RESEARCH_DISPATCH_TTL_SECONDS,
            )
        )
    except Exception:
        return False


def _research_dispatch_task_id(
    run_id: str,
    redis_client: Redis | None,
) -> str | None:
    if redis_client is None:
        return None
    try:
        value = redis_client.get(_research_dispatch_key(run_id))
    except Exception:
        return None
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _clear_research_dispatch(
    run_id: str,
    task_id: str,
    redis_client: Redis | None = None,
) -> None:
    try:
        client = redis_client or _redis_client()
        if _research_dispatch_task_id(run_id, client) == task_id:
            client.delete(_research_dispatch_key(run_id))
    except Exception:
        return


def recover_orphaned_queued_research_runs(
    db,
    redis_client: Redis | None,
    active_settings,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Republish durable queued runs whose Redis dispatch marker disappeared."""

    if redis_client is None:
        return {"queued_scanned": 0, "queued_recovered": 0, "recovery_failed": 0}

    current = as_utc(now or utc_now())
    grace_seconds = max(30, active_settings.research_lease_seconds)
    cutoff = current - timedelta(seconds=grace_seconds)
    candidates: list[tuple[str, ResearchRun | EventResearchRun]] = [
        *(('asset', run) for run in list_queued_runs(db)),
        *(
            ('event', run)
            for run in list_event_research_runs(db, 5_000)
            if run.status is RunStatus.QUEUED
        ),
    ]
    candidates.sort(key=lambda item: as_utc(item[1].created_at))

    scanned = recovered = failed = 0
    for kind, run in candidates:
        if as_utc(run.created_at) > cutoff:
            continue
        scanned += 1
        run_id = str(run.id)
        dispatched_task_id = _research_dispatch_task_id(run_id, redis_client)
        if dispatched_task_id is not None and dispatched_task_id == run.celery_task_id:
            continue

        task_id = str(uuid4())
        instance = select_model_instance(
            "research",
            task_id=task_id,
            preferred=run.model_instance_id,
            settings=active_settings,
            probe_health=True,
        )
        if not _record_research_dispatch(run_id, task_id, redis_client):
            failed += 1
            continue

        previous_task_id = run.celery_task_id
        previous_instance_id = run.model_instance_id
        previous_step_count = len(run.analysis_steps)
        run.celery_task_id = task_id
        run.model_instance_id = instance.id
        run.analysis_steps.append(
            AnalysisStep(
                phase="research_dispatch_recovery",
                status="queued",
                executor="research-lifecycle",
                summary="检测到消息队列中的派发记录已丢失，已自动重新派发研究任务。",
                metrics={
                    "previous_task_id": previous_task_id,
                    "instance_id": instance.id,
                    "priority": (
                        ASSET_RESEARCH_PRIORITY
                        if kind == "asset"
                        else EVENT_RESEARCH_PRIORITY
                    ),
                },
            )
        )
        if kind == "asset":
            save_run(db, run)
        else:
            save_event_research_run(db, run)

        try:
            if kind == "asset":
                research_asset.apply_async(
                    args=[
                        run.asset.asset_id,
                        str(run.event_id) if run.event_id else None,
                        run_id,
                    ],
                    kwargs={"model_instance_id": instance.id},
                    queue=broker_queue_name("research", instance.id),
                    task_id=task_id,
                    countdown=1,
                    priority=ASSET_RESEARCH_PRIORITY,
                )
            else:
                research_event.apply_async(
                    args=[str(run.event_id), run_id],
                    kwargs={"model_instance_id": instance.id},
                    queue=broker_queue_name("research", instance.id),
                    task_id=task_id,
                    countdown=1,
                    priority=EVENT_RESEARCH_PRIORITY,
                )
        except Exception:
            _clear_research_dispatch(run_id, task_id, redis_client)
            run.celery_task_id = previous_task_id
            run.model_instance_id = previous_instance_id
            run.analysis_steps = run.analysis_steps[:previous_step_count]
            if kind == "asset":
                save_run(db, run)
            else:
                save_event_research_run(db, run)
            failed += 1
            continue
        recovered += 1

    return {
        "queued_scanned": scanned,
        "queued_recovered": recovered,
        "recovery_failed": failed,
    }


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


def _default_news_extraction_queue() -> dict[str, Any]:
    return {
        "model": settings.ollama_extract_model,
        "scan_task_id": None,
        "state": "idle",
        "total_items": 0,
        "metadata": {},
        "items": [],
        "error": None,
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


def _read_news_extraction_queue(client: Redis) -> dict[str, Any]:
    payload = _default_news_extraction_queue()
    raw = _decode(client.get(NEWS_EXTRACTION_QUEUE_KEY))
    if raw:
        try:
            stored = json.loads(raw)
            if isinstance(stored, dict):
                payload.update(stored)
        except (TypeError, json.JSONDecodeError):
            pass
    return payload


def _write_news_extraction_queue(client: Redis, payload: dict[str, Any]) -> None:
    client.set(
        NEWS_EXTRACTION_QUEUE_KEY,
        json.dumps(payload, ensure_ascii=False, default=str),
        ex=NEWS_EXTRACTION_QUEUE_TTL_SECONDS,
    )


def _news_extraction_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts = {key: 0 for key in ("queued", "running", "retrying", "completed", "failed")}
    for item in payload.get("items", []):
        status = item.get("status")
        if status in counts:
            counts[status] += 1
    return counts


def _registry_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _elapsed_ms(start: Any, end: datetime) -> int | None:
    started_at = _registry_datetime(start)
    if started_at is None:
        return None
    return max(0, int((as_utc(end) - started_at).total_seconds() * 1000))


def _news_extraction_item_durations(
    item: dict[str, Any], generated_at: datetime
) -> tuple[int | None, int | None]:
    queue_duration = item.get("queue_duration_ms")
    if queue_duration is None:
        queue_end = _registry_datetime(item.get("started_at"))
        if queue_end is None and item.get("status") == "queued":
            queue_end = generated_at
        if queue_end is not None:
            queue_duration = _elapsed_ms(item.get("queued_at"), queue_end)

    execution_duration = max(0, int(item.get("execution_duration_ms") or 0))
    active_attempt = _elapsed_ms(item.get("attempt_started_at"), generated_at)
    if active_attempt is not None:
        execution_duration += active_attempt
    if not item.get("started_at") and execution_duration == 0:
        return queue_duration, None
    return queue_duration, execution_duration


def get_news_extraction_queue(limit: int = 200) -> dict[str, Any]:
    """Return the current per-news 3B work list without exposing Celery payloads."""

    try:
        payload = _read_news_extraction_queue(_redis_client())
        generated_at = utc_now()
        counts = _news_extraction_counts(payload)
        status_rank = {"running": 0, "retrying": 1, "queued": 2, "failed": 3}
        queue_durations: list[int] = []
        execution_durations: list[int] = []
        recent_execution_cutoff = generated_at - RECENT_EXECUTION_WINDOW
        public_items: list[dict[str, Any]] = []
        for stored_item in payload.get("items", []):
            item = dict(stored_item)
            queue_duration, execution_duration = _news_extraction_item_durations(item, generated_at)
            item["queue_duration_ms"] = queue_duration
            item["execution_duration_ms"] = execution_duration
            item.pop("attempt_started_at", None)
            if queue_duration is not None:
                queue_durations.append(queue_duration)
            completed_at = _registry_datetime(item.get("completed_at") or item.get("updated_at"))
            if (
                execution_duration is not None
                and item.get("status") in {"completed", "failed"}
                and completed_at is not None
                and completed_at >= recent_execution_cutoff
            ):
                execution_durations.append(execution_duration)
            public_items.append(item)
        visible = [item for item in public_items if item.get("status") in status_rank]
        visible.sort(
            key=lambda item: (
                status_rank[item["status"]],
                item.get("queued_at") or "",
            )
        )
        return {
            "generated_at": generated_at,
            "model": payload.get("model") or settings.ollama_extract_model,
            "scan_task_id": payload.get("scan_task_id"),
            "state": payload.get("state") or "idle",
            "total_items": int(payload.get("total_items") or 0),
            "counts": counts,
            "average_queue_duration_ms": (
                sum(queue_durations) // len(queue_durations) if queue_durations else None
            ),
            "average_execution_duration_ms": (
                sum(execution_durations) // len(execution_durations)
                if execution_durations
                else None
            ),
            "queue_duration_sample_count": len(queue_durations),
            "execution_duration_sample_count": len(execution_durations),
            "truncated": len(visible) > limit,
            "items": visible[:limit],
            "error": payload.get("error"),
        }
    except Exception as exc:
        return {
            "generated_at": utc_now(),
            "model": settings.ollama_extract_model,
            "scan_task_id": None,
            "state": "unavailable",
            "total_items": 0,
            "counts": _news_extraction_counts({}),
            "average_queue_duration_ms": None,
            "average_execution_duration_ms": None,
            "queue_duration_sample_count": 0,
            "execution_duration_sample_count": 0,
            "truncated": False,
            "items": [],
            "error": f"queue state unavailable: {type(exc).__name__}",
        }


def clear_news_extraction_queue(
    redis_client: Redis | None = None,
    *,
    instance_id: str | None = None,
) -> dict[str, Any]:
    """Clear active and failed 3B tasks, returning only active Celery task ids."""

    client = redis_client or _redis_client()
    payload = _read_news_extraction_queue(client)
    now = utc_now()
    task_ids: list[str] = []
    cancelled = 0
    active_statuses = {"queued", "running", "retrying"}
    clearable_statuses = active_statuses | {"failed"}
    for item in payload.get("items", []):
        if instance_id is not None and item.get("instance_id") != instance_id:
            continue
        previous_status = item.get("status")
        if previous_status not in clearable_statuses:
            continue
        cancelled += 1
        task_id = item.get("task_id")
        if task_id and previous_status in active_statuses:
            task_ids.append(str(task_id))
        active_attempt = _elapsed_ms(item.get("attempt_started_at"), now)
        if active_attempt is not None:
            item["execution_duration_ms"] = (
                max(0, int(item.get("execution_duration_ms") or 0)) + active_attempt
            )
        item.update(
            status="cancelled",
            updated_at=now.isoformat(),
            completed_at=now.isoformat(),
            attempt_started_at=None,
            error=None,
        )
        if task_id:
            update_instance_assignment(
                "extract",
                str(task_id),
                status="cancelled",
                instance_id=item.get("instance_id"),
                redis_client=client,
            )
    remaining = _news_extraction_counts(payload)
    payload["state"] = (
        "cancelled"
        if instance_id is None
        else "running"
        if remaining["running"]
        else "retrying"
        if remaining["retrying"]
        else "queued"
        if remaining["queued"]
        else "completed_with_errors"
        if remaining["failed"]
        else "completed"
    )
    payload["error"] = None
    _write_news_extraction_queue(client, payload)
    scan_task_id = payload.get("scan_task_id")
    if scan_task_id and instance_id is None:
        _update_scan_status(
            client,
            state="cancelled",
            phase="extracting",
            current=int(payload.get("total_items") or 0) - len(task_ids),
            total=int(payload.get("total_items") or 0),
        )
        if _decode(client.get(SCAN_GATE_KEY)) == scan_task_id:
            client.delete(SCAN_GATE_KEY)
        client.delete(SCAN_PAUSE_KEY)
    return {"cancelled": cancelled, "celery_task_ids": task_ids}


def cancel_news_extraction_task(
    *,
    task_id: str,
    news_id: str | None = None,
    redis_client: Redis | None = None,
) -> list[str]:
    """Cancel one active scan extraction item before a manual replacement."""

    client = redis_client or _redis_client()
    payload = _read_news_extraction_queue(client)
    now = utc_now()
    revoked: list[str] = []
    for item in payload.get("items", []):
        matches = item.get("task_id") == task_id or (
            news_id is not None and item.get("news_id") == news_id
        )
        if not matches or item.get("status") not in {"queued", "running", "retrying"}:
            continue
        if item.get("task_id"):
            revoked.append(str(item["task_id"]))
        active_attempt = _elapsed_ms(item.get("attempt_started_at"), now)
        if active_attempt is not None:
            item["execution_duration_ms"] = (
                max(0, int(item.get("execution_duration_ms") or 0)) + active_attempt
            )
        item.update(
            status="cancelled",
            updated_at=now.isoformat(),
            completed_at=now.isoformat(),
            attempt_started_at=None,
            error=None,
        )
        break
    if revoked:
        counts = _news_extraction_counts(payload)
        payload["state"] = (
            "running"
            if counts["running"]
            else "retrying"
            if counts["retrying"]
            else "queued"
            if counts["queued"]
            else "completed_with_errors"
            if counts["failed"]
            else "completed"
        )
        _write_news_extraction_queue(client, payload)
    return revoked


def _update_scan_status(client: Redis, **updates: Any) -> dict[str, Any]:
    payload = _read_scan_status(client)
    payload.update(updates)
    client.set(SCAN_STATUS_KEY, json.dumps(payload, ensure_ascii=False, default=str))
    return payload


def _initialize_news_extraction_queue(
    client: Redis,
    scan_task_id: str,
    entries: list[dict[str, Any]],
    metadata: dict[str, int],
) -> dict[str, Any]:
    normalized_entries = []
    for entry in entries:
        normalized = dict(entry)
        normalized.setdefault("started_at", None)
        normalized.setdefault("completed_at", None)
        normalized.setdefault("attempt_started_at", None)
        normalized.setdefault("queue_duration_ms", None)
        normalized.setdefault("execution_duration_ms", 0)
        normalized_entries.append(normalized)
    payload = {
        "model": settings.ollama_extract_model,
        "scan_task_id": scan_task_id,
        "state": "queued" if entries else "completed",
        "total_items": len(normalized_entries),
        "metadata": metadata,
        "items": normalized_entries,
        "error": None,
    }
    _write_news_extraction_queue(client, payload)
    return payload


def _update_news_extraction_item(
    client: Redis,
    scan_task_id: str,
    news_id: str,
    status: str,
    *,
    attempt: int | None = None,
    error: str | None = None,
) -> dict[str, Any] | None:
    payload = _read_news_extraction_queue(client)
    if payload.get("scan_task_id") != scan_task_id:
        return None
    now = utc_now()
    changed = False
    for item in payload.get("items", []):
        if item.get("news_id") != news_id:
            continue
        previous_status = item.get("status")
        if previous_status == "cancelled":
            return payload
        if status == "running":
            if not item.get("started_at"):
                item["started_at"] = now.isoformat()
                item["queue_duration_ms"] = _elapsed_ms(item.get("queued_at"), now)
            if previous_status != "running" or not item.get("attempt_started_at"):
                item["attempt_started_at"] = now.isoformat()
            item["completed_at"] = None
        elif status in {"retrying", "completed", "failed"}:
            active_attempt = _elapsed_ms(item.get("attempt_started_at"), now)
            if active_attempt is not None:
                item["execution_duration_ms"] = (
                    max(0, int(item.get("execution_duration_ms") or 0)) + active_attempt
                )
            item["attempt_started_at"] = None
            if status in {"completed", "failed"}:
                item["completed_at"] = now.isoformat()
        item["status"] = status
        item["updated_at"] = now.isoformat()
        item["error"] = error
        if attempt is not None:
            item["attempt"] = attempt
        if item.get("task_id"):
            update_instance_assignment(
                "extract",
                str(item["task_id"]),
                status=status,
                instance_id=item.get("instance_id"),
                redis_client=client,
            )
        changed = True
        break
    if not changed:
        return None
    counts = _news_extraction_counts(payload)
    payload["state"] = (
        "running"
        if counts["running"]
        else "retrying"
        if counts["retrying"]
        else "queued"
        if counts["queued"]
        else "completed_with_errors"
        if counts["failed"]
        else "completed"
    )
    _write_news_extraction_queue(client, payload)
    return payload


def _finish_news_extraction_queue(
    client: Redis, scan_task_id: str, *, error: str | None = None
) -> dict[str, Any] | None:
    payload = _read_news_extraction_queue(client)
    if payload.get("scan_task_id") != scan_task_id:
        return None
    if payload.get("state") == "cancelled":
        return payload
    counts = _news_extraction_counts(payload)
    payload["state"] = (
        "failed" if error else ("completed_with_errors" if counts["failed"] else "completed")
    )
    payload["error"] = error
    _write_news_extraction_queue(client, payload)
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
    return _decode(client.get(SCAN_GATE_KEY)) == task_id and _renew_scan_gate(client, task_id)


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
        next_scan_at=(completed_at + timedelta(minutes=settings.scan_interval_minutes)).isoformat(),
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
        status.get("paused_from_phase") if status.get("state") == "paused" else status.get("phase")
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
    retry_of_run_id: UUID | None = None,
    retry_attempt: int = 0,
    market_factor_refresh_days: int | None = None,
    priority: int | None = None,
    model_instance_id: str | None = None,
    force_research: bool = False,
    queue_phase: str | None = None,
) -> tuple[str, ResearchRun]:
    """Atomically admit and queue one asset research task."""

    with asset_research_admission_lock(db, asset.asset_id):
        active = active_asset_research(db, asset.asset_id)
        ordinary_event = (
            event is not None
            and not historical_replay
            and retry_of_run_id is None
            and market_factor_refresh_days is None
            and not force_research
        )
        mergeable = (
            get_mergeable_queued_run(
                db,
                asset.asset_id,
                utc_now() - timedelta(hours=settings.research_coalesce_window_hours),
            )
            if ordinary_event and active is not None
            else None
        )
        if active is not None and (mergeable is None or mergeable.id != active.id):
            raise ResearchAdmissionError(
                "research_already_active",
                "该标的已有排队中或执行中的研究任务。",
                run_id=str(active.id),
            )
        if active is None:
            enforce_asset_research_admission(
                db,
                asset.asset_id,
                cooldown_hours=settings.research_asset_cooldown_hours,
                now=utc_now(),
                bypass_cooldown=(
                    retry_of_run_id is not None or force_research
                ),
                check_active=False,
            )
        return _enqueue_research_locked(
            db,
            asset,
            event,
            as_of,
            historical_replay,
            retry_of_run_id,
            retry_attempt,
            market_factor_refresh_days,
            priority,
            model_instance_id,
            force_research,
            queue_phase,
        )


def _enqueue_research_locked(
    db,
    asset: AssetRef,
    event: NewsEvent | None = None,
    as_of: datetime | None = None,
    historical_replay: bool = False,
    retry_of_run_id: UUID | None = None,
    retry_attempt: int = 0,
    market_factor_refresh_days: int | None = None,
    priority: int | None = None,
    model_instance_id: str | None = None,
    force_research: bool = False,
    queue_phase: str | None = None,
) -> tuple[str, ResearchRun]:
    """Persist a visible queued run before handing work to the LLM worker."""

    if (
        event
        and not historical_replay
        and retry_of_run_id is None
        and market_factor_refresh_days is None
    ):
        canonical = get_mergeable_queued_run(
            db,
            asset.asset_id,
            utc_now() - timedelta(hours=settings.research_coalesce_window_hours),
        )
        if canonical is not None and event.id not in canonical.trigger_event_ids:
            canonical.trigger_event_ids.append(event.id)
            canonical.analysis_steps.append(
                AnalysisStep(
                    phase="research_coalescing",
                    executor="celery",
                    summary=f"已把事件 {event.headline} 合并到该标的研究任务。",
                    metrics={"trigger_event_id": str(event.id)},
                )
            )
            save_run(db, canonical)
            merged = ResearchRun(
                event_id=event.id,
                trigger_event_ids=[event.id],
                asset=asset,
                status=RunStatus.COALESCED,
                as_of=as_of or utc_now(),
                celery_task_id=canonical.celery_task_id,
                coalesced_into_run_id=canonical.id,
                completed_at=utc_now(),
                analysis_steps=[
                    *event.analysis_steps,
                    AnalysisStep(
                        phase="research_coalescing",
                        executor="celery",
                        summary=f"已合并到同标的主研究任务 {canonical.id}。",
                        metrics={"canonical_run_id": str(canonical.id)},
                    ),
                ],
            )
            save_run(db, merged)
            return canonical.celery_task_id or f"research:{canonical.id}", merged

    task_id = str(uuid4())
    effective_priority = ASSET_RESEARCH_PRIORITY if priority is None else priority
    instance = select_model_instance(
        "research",
        task_id=task_id,
        preferred=model_instance_id,
        settings=settings,
        probe_health=True,
    )
    run = ResearchRun(
        event_id=event.id if event else None,
        trigger_event_ids=[event.id] if event else [],
        asset=asset,
        as_of=as_of or utc_now(),
        historical_replay=historical_replay,
        retry_of_run_id=retry_of_run_id,
        retry_attempt=retry_attempt,
        celery_task_id=task_id,
        model_instance_id=instance.id,
        analysis_steps=[
            *(event.analysis_steps if event else []),
            *(
                [
                    AnalysisStep(
                        phase="market_factor_refresh_queue",
                        executor="celery",
                        summary=(
                            f"事件后 {market_factor_refresh_days} 日市场反应窗口已成熟，"
                            "已创建一次因子重评。"
                        ),
                        metrics={
                            "event_id": str(event.id) if event else None,
                            "target_session_days": market_factor_refresh_days,
                        },
                    )
                ]
                if market_factor_refresh_days is not None
                else []
            ),
            AnalysisStep(
                phase=(
                    queue_phase
                    or ("research_retry_queue" if retry_of_run_id else "research_queue")
                ),
                executor="celery",
                summary=(
                    f"已为历史失败任务创建第 {retry_attempt} 次重新执行。"
                    if retry_of_run_id
                    else (
                        f"已绕过{settings.research_asset_cooldown_hours}小时冷却，"
                        f"为主标的 {asset.symbol} 创建重新调研任务。"
                        if force_research
                        else f"已为主标的 {asset.symbol} 创建深度研究任务。"
                    )
                ),
                metrics={
                    **(
                        {
                            "retry_of_run_id": str(retry_of_run_id),
                            "retry_attempt": retry_attempt,
                        }
                        if retry_of_run_id
                        else {}
                    ),
                    "instance_id": instance.id,
                    "priority": effective_priority,
                },
            ),
        ],
    )
    save_run(db, run)
    dispatch_recorded = _record_research_dispatch(str(run.id), task_id)
    try:
        task = research_asset.apply_async(
            args=[asset.asset_id, str(event.id) if event else None, str(run.id)],
            kwargs={"model_instance_id": instance.id},
            queue=broker_queue_name("research", instance.id),
            task_id=task_id,
            priority=effective_priority,
        )
    except Exception as exc:
        if dispatch_recorded:
            _clear_research_dispatch(str(run.id), task_id)
        run.status = RunStatus.FAILED
        run.error = f"{type(exc).__name__}: research queue failed"
        run.completed_at = utc_now()
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


def reserve_research_run(
    db,
    asset: AssetRef,
    event: NewsEvent | None = None,
    as_of: datetime | None = None,
    historical_replay: bool = False,
) -> ResearchRun:
    """Reserve a synchronous API research run through the same admission gate."""

    with asset_research_admission_lock(db, asset.asset_id):
        enforce_asset_research_admission(
            db,
            asset.asset_id,
            cooldown_hours=settings.research_asset_cooldown_hours,
            now=utc_now(),
        )
        run = ResearchRun(
            event_id=event.id if event else None,
            trigger_event_ids=[event.id] if event else [],
            asset=asset,
            as_of=as_of or utc_now(),
            historical_replay=historical_replay,
            analysis_steps=[
                *(event.analysis_steps if event else []),
                AnalysisStep(
                    phase="research_queue",
                    executor="api",
                    summary=f"已为主标的 {asset.symbol} 创建同步研究任务。",
                ),
            ],
        )
        save_run(db, run)
        return run


MARKET_FACTOR_REFRESH_AGES = ((20, 30), (5, 8), (1, 2))
MARKET_FACTOR_REFRESH_EVENT_DAYS = 45
MARKET_FACTOR_REFRESH_BATCH_SIZE = 20


def _due_market_factor_refresh_session(
    *, age_days: float, completed_session: int = 0
) -> int | None:
    """Choose the largest newly matured 1/5/20-session approximation."""

    for session, minimum_age in MARKET_FACTOR_REFRESH_AGES:
        if age_days >= minimum_age and completed_session < session:
            return session
    return None


def enqueue_event_researches(db, event: NewsEvent, limit: int) -> list[tuple[str, ResearchRun]]:
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
            researched_urls = {canonicalize_url(item.source_url) for item in existing.evidence}
            has_new_cluster_evidence = bool(event_urls - researched_urls)
            if (
                existing.status is not RunStatus.INSUFFICIENT_EVIDENCE
                or not has_new_cluster_evidence
            ):
                continue
        try:
            queued.append(enqueue_research(db, candidate.asset, event))
        except ResearchAdmissionError:
            continue
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


def enqueue_asset_mapping(
    db,
    event: NewsEvent,
    *,
    force: bool = False,
    priority: int | None = None,
    model_instance_id: str | None = None,
) -> str | None:
    """Queue one visible 7B mapping attempt for an unmapped event."""

    if not force and (
        event.candidates
        or any(
            step.phase == "asset_mapping_queue" and step.status in {"queued", "completed"}
            for step in event.analysis_steps
        )
    ):
        return None
    task_id = str(uuid4())
    instance = select_model_instance(
        "assist",
        task_id=task_id,
        preferred=model_instance_id,
        settings=settings,
        probe_health=True,
    )
    _replace_event_step(
        event,
        AnalysisStep(
            phase="asset_mapping_queue",
            status="queued",
            executor="celery",
            model=settings.ollama_assist_model,
            summary=(
                f"确定性映射未找到标的，已创建 {settings.ollama_assist_model} 二次标的发现任务。"
            ),
            metrics={"instance_id": instance.id},
        ),
    )
    save_event(db, event)
    record_model_task(
        "assist",
        task_id=task_id,
        kind="asset_mapping",
        entity_id=str(event.id),
        title=event.headline,
        subtitle=event.event_type.value,
        source="manual" if force else "automatic",
        instance_id=instance.id,
    )
    try:
        task = resolve_event_assets.apply_async(
            args=[str(event.id)],
            kwargs={"model_instance_id": instance.id},
            queue=broker_queue_name("assist", instance.id),
            task_id=task_id,
            **({"priority": priority} if priority is not None else {}),
        )
    except Exception as exc:
        update_model_task(
            "assist",
            task_id,
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
        )
        _replace_event_step(
            event,
            AnalysisStep(
                phase="asset_mapping_queue",
                status="failed",
                executor="celery",
                model=settings.ollama_assist_model,
                summary=(
                    f"{settings.ollama_assist_model} 标的发现任务入队失败（{type(exc).__name__}）。"
                ),
            ),
        )
        save_event(db, event)
        raise
    return str(task.id)


def _asset_mapping_is_terminal(event: NewsEvent) -> bool:
    if event.candidates:
        return True
    queue_step = next(
        (step for step in reversed(event.analysis_steps) if step.phase == "asset_mapping_queue"),
        None,
    )
    mapping_step = next(
        (step for step in reversed(event.analysis_steps) if step.phase == "asset_mapping"),
        None,
    )
    return bool(
        (queue_step and queue_step.status == "completed")
        or (mapping_step and mapping_step.status in {"completed", "unmapped"})
    )


def _revoke_model_task(task_id: str) -> None:
    celery_app.control.revoke(task_id, terminate=False)


@celery_app.task(name="market_loop.reconcile_asset_mapping_leases")
def reconcile_asset_mapping_leases() -> dict[str, int | str]:
    """Replace abandoned mapping tasks without duplicating live or finished work."""

    try:
        client = _redis_client()
        client.ping()
    except Exception:
        return {
            "status": "redis_unavailable",
            "stale": 0,
            "requeued": 0,
            "cancelled": 0,
            "skipped": 0,
            "failed": 0,
        }

    stale = stale_model_task_records(
        "assist",
        lease_seconds=settings.model_task_lease_seconds,
        redis_client=client,
    )
    result = {
        "status": "completed",
        "stale": len(stale),
        "requeued": 0,
        "cancelled": 0,
        "skipped": 0,
        "failed": 0,
    }
    if not stale:
        return result

    init_db()
    for stale_task in stale:
        lock_key = f"market-loop:model-task-recovery:assist:{stale_task.task_id}"
        try:
            acquired = client.set(
                lock_key,
                "1",
                nx=True,
                ex=max(60, settings.model_task_lease_seconds),
            )
        except Exception:
            result["failed"] += 1
            continue
        if not acquired:
            result["skipped"] += 1
            continue
        try:
            current_records = list_model_task_records("assist", redis_client=client)
            current = next(
                (item for item in current_records if item.task_id == stale_task.task_id),
                None,
            )
            cutoff = utc_now() - timedelta(seconds=settings.model_task_lease_seconds)
            if (
                current is None
                or current.status not in {"running", "generating", "testing", "merging"}
                or current.updated_at > cutoff
                or not current.entity_id
            ):
                result["skipped"] += 1
                continue

            replacement = next(
                (
                    item
                    for item in current_records
                    if item.task_id != current.task_id
                    and item.entity_id == current.entity_id
                    and item.status in ACTIVE_STATUSES
                ),
                None,
            )
            with SessionLocal() as db:
                try:
                    event = get_event(db, UUID(current.entity_id))
                except ValueError:
                    event = None
                if event is None or _asset_mapping_is_terminal(event):
                    if cancel_model_task("assist", current.task_id, redis_client=client):
                        _revoke_model_task(current.task_id)
                        result["cancelled"] += 1
                    else:
                        result["skipped"] += 1
                    continue
                if replacement is not None:
                    if cancel_model_task("assist", current.task_id, redis_client=client):
                        _revoke_model_task(current.task_id)
                        result["cancelled"] += 1
                    else:
                        result["skipped"] += 1
                    continue

                try:
                    replacement_id = enqueue_asset_mapping(
                        db,
                        event,
                        force=True,
                        priority=DEFAULT_MODEL_TASK_PRIORITY,
                    )
                except Exception:
                    result["failed"] += 1
                    continue
                if not replacement_id:
                    result["skipped"] += 1
                    continue
                if cancel_model_task("assist", current.task_id, redis_client=client):
                    _revoke_model_task(current.task_id)
                    result["requeued"] += 1
                else:
                    cancel_model_task("assist", replacement_id, redis_client=client)
                    _revoke_model_task(replacement_id)
                    result["skipped"] += 1
        finally:
            try:
                client.delete(lock_key)
            except Exception:
                pass
    return result


def enqueue_news_extraction_retry(
    news: NewsItem,
    *,
    priority: int | None = None,
    model_instance_id: str | None = None,
) -> str:
    """Queue a standalone extraction attempt without reopening a completed scan."""

    task_id = str(uuid4())
    instance = select_model_instance(
        "extract",
        task_id=task_id,
        preferred=model_instance_id,
        settings=settings,
        probe_health=True,
    )
    record_model_task(
        "extract",
        task_id=task_id,
        kind="news_extraction",
        entity_id=str(news.id),
        title=news.title,
        subtitle=news.source,
        source="manual",
        instance_id=instance.id,
    )
    try:
        task = retry_news_item.apply_async(
            args=[str(news.id)],
            kwargs={"model_instance_id": instance.id},
            queue=broker_queue_name("extract", instance.id),
            task_id=task_id,
            **({"priority": priority} if priority is not None else {}),
        )
    except Exception as exc:
        update_model_task(
            "extract",
            task_id,
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
        )
        raise
    return str(task.id)


def enqueue_event_report(
    db,
    event: NewsEvent,
    *,
    model_instance_id: str | None = None,
) -> tuple[str | None, EventResearchRun]:
    """Persist and queue one neutral report for an event with no verified asset."""

    existing = get_event_research_for_event(db, event.id)
    if existing:
        return None, existing
    task_id = str(uuid4())
    instance = select_model_instance(
        "research",
        task_id=task_id,
        preferred=model_instance_id,
        settings=settings,
        probe_health=True,
    )
    run = EventResearchRun(
        event_id=event.id,
        as_of=max(event.as_of, event.observed_at),
        celery_task_id=task_id,
        model_instance_id=instance.id,
        analysis_steps=[
            *event.analysis_steps,
            AnalysisStep(
                phase="event_research_queue",
                status="queued",
                executor="celery",
                model=settings.ollama_research_model,
                summary="未找到经主数据验证的证券标的，已创建中性事件研报任务。",
                metrics={
                    "instance_id": instance.id,
                    "priority": EVENT_RESEARCH_PRIORITY,
                },
            ),
        ],
    )
    save_event_research_run(db, run)
    dispatch_recorded = _record_research_dispatch(str(run.id), task_id)
    try:
        task = research_event.apply_async(
            args=[str(event.id), str(run.id)],
            kwargs={"model_instance_id": instance.id},
            queue=broker_queue_name("research", instance.id),
            task_id=task_id,
            priority=EVENT_RESEARCH_PRIORITY,
        )
    except Exception as exc:
        if dispatch_recorded:
            _clear_research_dispatch(str(run.id), task_id)
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


def enqueue_event_research_retry(
    db,
    event: NewsEvent,
    run: EventResearchRun,
    *,
    priority: int | None = None,
    model_instance_id: str | None = None,
) -> tuple[str, EventResearchRun]:
    """Reset the event's unique durable report row and queue a fresh manual attempt."""

    effective_priority = EVENT_RESEARCH_PRIORITY if priority is None else priority
    run.status = RunStatus.QUEUED
    run.as_of = utc_now()
    run.verification_round = 0
    run.retry_count += 1
    run.missing_requirements = []
    run.contradictions = []
    run.evidence = []
    run.report = None
    run.error = None
    run.retryable_reason = None
    task_id = str(uuid4())
    instance = select_model_instance(
        "research",
        task_id=task_id,
        preferred=model_instance_id,
        settings=settings,
        probe_health=True,
    )
    run.celery_task_id = task_id
    run.model_instance_id = instance.id
    run.analysis_steps.append(
        AnalysisStep(
            phase="event_research_retry_queue",
            status="queued",
            executor="celery",
            model=settings.ollama_research_model,
            summary=f"已为历史失败事件研报创建第 {run.retry_count} 次重新执行。",
            metrics={
                "retry_count": run.retry_count,
                "instance_id": instance.id,
                "priority": effective_priority,
            },
        )
    )
    save_event_research_run(db, run)
    dispatch_recorded = _record_research_dispatch(str(run.id), task_id)
    try:
        task = research_event.apply_async(
            args=[str(event.id), str(run.id)],
            kwargs={"model_instance_id": instance.id},
            queue=broker_queue_name("research", instance.id),
            task_id=task_id,
            priority=effective_priority,
        )
    except Exception as exc:
        if dispatch_recorded:
            _clear_research_dispatch(str(run.id), task_id)
        run.status = RunStatus.FAILED
        run.error = f"{type(exc).__name__}: event research retry queue failed"
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_research_retry_queue",
                status="failed",
                executor="celery",
                summary=f"事件研报重新执行入队失败（{type(exc).__name__}）。",
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


def _persist_news_for_extraction(db, items: list[NewsItem]) -> list[NewsItem]:
    processed_ids = event_news_item_ids(db)
    pending: list[NewsItem] = []
    seen_ids: set[UUID] = set()
    for discovered in items:
        item = enrich_news_lineage(discovered)
        if not save_news(db, item):
            stored = get_news_by_content_hash(db, item.content_hash)
            if stored is None:
                continue
            item = stored
        if item.id in processed_ids or item.id in seen_ids:
            continue
        seen_ids.add(item.id)
        pending.append(item)
    return pending


def _build_news_extraction_workflow(
    client: Redis,
    scan_task_id: str,
    items: list[NewsItem],
    metadata: dict[str, Any],
) -> tuple[list[str], Any | None]:
    now = utc_now().isoformat()
    task_ids = [str(uuid4()) for _ in items]
    instances = [
        select_model_instance(
            "extract",
            task_id=task_id,
            settings=settings,
            probe_health=True,
        )
        for task_id in task_ids
    ]
    entries = [
        {
            "task_id": task_id,
            "instance_id": instance.id,
            "news_id": str(item.id),
            "title": item.title,
            "source": item.source,
            "published_at": item.published_at.isoformat(),
            "status": "queued",
            "attempt": 0,
            "queued_at": now,
            "updated_at": now,
            "error": None,
        }
        for task_id, item, instance in zip(task_ids, items, instances, strict=True)
    ]
    _initialize_news_extraction_queue(client, scan_task_id, entries, metadata)
    if not items:
        return [], None
    header = [
        extract_news_item.s(
            scan_task_id,
            str(item.id),
            model_instance_id=instance.id,
        ).set(
            queue=broker_queue_name("extract", instance.id),
            task_id=task_id,
        )
        for task_id, item, instance in zip(task_ids, items, instances, strict=True)
    ]
    callback = finalize_news_extraction.s(scan_task_id).set(queue="extract")
    return task_ids, chord(header, callback)


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
        with SessionLocal() as db:
            pending_items = _persist_news_for_extraction(db, accepted_items)
        for error in registry.last_errors:
            notifier.send(f"数据源故障：{error}")
        _require_scan_gate(client, task_id)
        metadata = {
            "discovered": len(items),
            "accepted": len(accepted_items),
            "filtered": filtered_count,
        }
        self.update_state(
            state="PROGRESS",
            meta={
                "phase": "extraction_queued",
                "current": 0,
                "total": len(pending_items),
            },
        )
        _update_scan_status(
            client,
            state="running",
            phase="extraction_queued",
            current=0,
            total=len(pending_items),
        )
        _wait_if_scan_paused(
            client,
            task_id,
            phase="extraction_queued",
            current=0,
            total=len(pending_items),
        )
        task_ids, extraction_workflow = _build_news_extraction_workflow(
            client,
            task_id,
            pending_items,
            metadata,
        )
        if task_ids and extraction_workflow is not None:
            return self.replace(extraction_workflow)
        result = {
            "status": "completed",
            **metadata,
            "events": 0,
            "extraction_completed": 0,
            "extraction_failed": 0,
            "research_queued": 0,
            "asset_mapping_queued": 0,
        }
        _finish_news_extraction_queue(client, task_id)
        _complete_scan(client, task_id, result)
        return result
    except Ignore:
        raise
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
            raise self.retry(exc=exc, countdown=min(60, 2 ** (self.request.retries + 1))) from exc
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
        _finish_news_extraction_queue(
            client,
            task_id,
            error=f"{type(exc).__name__}",
        )
        raise
    finally:
        if lock.owned():
            lock.release()


@celery_app.task(bind=True, name="market_loop.extract_news_item", max_retries=2)
@model_instance_task("extract")
def extract_news_item(
    self,
    scan_task_id: str,
    news_id: str,
    model_instance_id: str | None = None,
) -> dict[str, Any]:
    """Extract and cluster one durable news item on the serial 3B queue."""

    client = _redis_client()
    try:
        _require_scan_gate(client, scan_task_id)
        payload = _read_news_extraction_queue(client)
        if any(
            item.get("news_id") == news_id and item.get("status") == "cancelled"
            for item in payload.get("items", [])
        ):
            return {"status": "cancelled", "news_id": news_id, "event_ids": []}
        counts = _news_extraction_counts(payload)
        done = counts["completed"] + counts["failed"]
        _wait_if_scan_paused(
            client,
            scan_task_id,
            phase="extracting",
            current=done,
            total=int(payload.get("total_items") or 0),
        )
        attempt = self.request.retries + 1
        _update_news_extraction_item(
            client,
            scan_task_id,
            news_id,
            "running",
            attempt=attempt,
        )
        _update_scan_status(
            client,
            state="running",
            phase="extracting",
            current=done,
            total=int(payload.get("total_items") or 0),
        )
        init_db()
        with SessionLocal() as db:
            news = get_news(db, UUID(news_id))
            if news is None:
                raise ValueError(f"unknown news item: {news_id}")
            registry = ProviderRegistry(assets=list_assets(db))
            events = EventService(registry).ingest(db, [news])
            provider_errors = list(registry.last_errors)
        updated = _update_news_extraction_item(
            client,
            scan_task_id,
            news_id,
            "completed",
            attempt=attempt,
        )
        counts = _news_extraction_counts(updated or payload)
        _update_scan_status(
            client,
            state="running",
            phase="extracting",
            current=counts["completed"] + counts["failed"],
            total=int((updated or payload).get("total_items") or 0),
        )
        return {
            "status": "completed",
            "news_id": news_id,
            "event_ids": [str(event.id) for event in events],
            "provider_errors": provider_errors,
        }
    except ScanLeaseLost:
        return {"status": "superseded", "news_id": news_id, "event_ids": []}
    except Exception as exc:
        retrying = self.request.retries < self.max_retries
        _update_news_extraction_item(
            client,
            scan_task_id,
            news_id,
            "retrying" if retrying else "failed",
            attempt=self.request.retries + 1,
            error=f"{type(exc).__name__}: {exc}",
        )
        if retrying:
            raise self.retry(
                exc=exc,
                countdown=min(60, 2 ** (self.request.retries + 1)),
            ) from exc
        return {
            "status": "failed",
            "news_id": news_id,
            "event_ids": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


@celery_app.task(bind=True, name="market_loop.retry_news_item", max_retries=2)
@model_instance_task("extract")
def retry_news_item(self, news_id: str, model_instance_id: str | None = None) -> dict[str, Any]:
    """Retry one durable news item independently and queue its downstream work."""

    task_id = str(self.request.id or f"news:{news_id}")
    if model_task_is_cancelled("extract", task_id):
        return {"status": "cancelled", "news_id": news_id, "event_ids": []}
    attempt = self.request.retries + 1
    update_model_task(
        "extract",
        task_id,
        status="running",
        attempt=attempt,
        entity_id=news_id,
    )
    try:
        init_db()
        with SessionLocal() as db:
            news = get_news(db, UUID(news_id))
            if news is None:
                raise ValueError(f"unknown news item: {news_id}")
            update_model_task(
                "extract",
                task_id,
                status="running",
                attempt=attempt,
                entity_id=news_id,
                title=news.title,
                subtitle=news.source,
            )
            registry = ProviderRegistry(assets=list_assets(db))
            events = EventService(registry).ingest(db, [news])
            research_queued = 0
            mapping_queued = 0
            if settings.auto_research:
                for event in events:
                    if event.candidates:
                        research_queued += int(enqueue_event_research(db, event) is not None)
                    else:
                        mapping_queued += int(enqueue_asset_mapping(db, event) is not None)
        update_model_task(
            "extract",
            task_id,
            status="completed",
            attempt=attempt,
            metrics={
                "event_count": len(events),
                "research_queued": research_queued,
                "asset_mapping_queued": mapping_queued,
            },
        )
        return {
            "status": "completed",
            "news_id": news_id,
            "event_ids": [str(event.id) for event in events],
            "research_queued": research_queued,
            "asset_mapping_queued": mapping_queued,
        }
    except Exception as exc:
        retrying = self.request.retries < self.max_retries
        update_model_task(
            "extract",
            task_id,
            status="retrying" if retrying else "failed",
            attempt=attempt,
            error=f"{type(exc).__name__}: {exc}",
        )
        if retrying:
            raise self.retry(
                exc=exc,
                countdown=min(60, 2 ** (self.request.retries + 1)),
            ) from exc
        return {
            "status": "failed",
            "news_id": news_id,
            "event_ids": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


@celery_app.task(bind=True, name="market_loop.finalize_news_extraction", max_retries=2)
def finalize_news_extraction(
    self,
    extraction_results: list[dict[str, Any]],
    scan_task_id: str,
) -> dict[str, Any]:
    """Join one scan batch and enqueue downstream research exactly once."""

    client = _redis_client()
    try:
        _require_scan_gate(client, scan_task_id)
        payload = _read_news_extraction_queue(client)
        if payload.get("scan_task_id") != scan_task_id:
            raise ScanLeaseLost(f"stale extraction batch: {scan_task_id}")
        event_ids = list(
            dict.fromkeys(
                event_id
                for result in extraction_results
                if isinstance(result, dict)
                for event_id in result.get("event_ids", [])
            )
        )
        provider_errors = list(
            dict.fromkeys(
                error
                for result in extraction_results
                if isinstance(result, dict)
                for error in result.get("provider_errors", [])
            )
        )
        for error in provider_errors:
            notifier.send(f"数据源故障：{error}")
        _update_scan_status(
            client,
            state="running",
            phase="queuing",
            current=0,
            total=len(event_ids),
        )
        queued = 0
        mapping_queued = 0
        with SessionLocal() as db:
            events = [
                event
                for event_id in event_ids
                if (event := get_event(db, UUID(event_id))) is not None
            ]
            for event_index, event in enumerate(events):
                _wait_if_scan_paused(
                    client,
                    scan_task_id,
                    phase="queuing",
                    current=event_index,
                    total=len(events),
                )
                if event.priority >= 0.75:
                    notifier.send(
                        f"高优先级事件：{event.headline}\n类型：{event.event_type.value}\n"
                        f"候选标的：{', '.join(item.asset.symbol for item in event.candidates[:5]) or '待解析'}"
                    )
                if not settings.auto_research:
                    continue
                try:
                    if event.candidates:
                        queued += int(enqueue_event_research(db, event) is not None)
                    else:
                        mapping_queued += int(enqueue_asset_mapping(db, event) is not None)
                except Exception as exc:
                    notifier.send(
                        f"下游研究任务入队失败：{event.headline}\n错误：{type(exc).__name__}"
                    )
        counts = _news_extraction_counts(payload)
        metadata = payload.get("metadata") or {}
        result = {
            "status": "completed_with_errors" if counts["failed"] else "completed",
            "discovered": int(metadata.get("discovered") or 0),
            "accepted": int(metadata.get("accepted") or 0),
            "filtered": int(metadata.get("filtered") or 0),
            "events": len(event_ids),
            "extraction_completed": counts["completed"],
            "extraction_failed": counts["failed"],
            "research_queued": queued,
            "asset_mapping_queued": mapping_queued,
        }
        _finish_news_extraction_queue(client, scan_task_id)
        _complete_scan(client, scan_task_id, result)
        return result
    except ScanLeaseLost:
        return {"status": "superseded", "events": 0}
    except Exception as exc:
        if self.request.retries < self.max_retries:
            _update_scan_status(
                client,
                state="retrying",
                task_id=scan_task_id,
                phase="finalizing",
                last_error=f"{type(exc).__name__}",
            )
            raise self.retry(
                exc=exc,
                countdown=min(60, 2 ** (self.request.retries + 1)),
            ) from exc
        failed_at = utc_now()
        _update_scan_status(
            client,
            state="failed",
            task_id=scan_task_id,
            phase="failed",
            next_scan_at=(
                failed_at + timedelta(minutes=settings.scan_interval_minutes)
            ).isoformat(),
            last_error=f"{type(exc).__name__}",
        )
        _finish_news_extraction_queue(
            client,
            scan_task_id,
            error=f"{type(exc).__name__}: {exc}",
        )
        _clear_scan_gate(client, scan_task_id)
        _clear_scan_pause(client, scan_task_id)
        raise


@celery_app.task(bind=True, name="market_loop.resolve_event_assets", max_retries=2)
@model_instance_task("assist")
def resolve_event_assets(self, event_id: str, model_instance_id: str | None = None) -> dict:
    task_id = str(self.request.id or f"event:{event_id}")
    if model_task_is_cancelled("assist", task_id):
        return {"status": "cancelled", "event_id": event_id}
    update_model_task(
        "assist",
        task_id,
        status="running",
        attempt=self.request.retries + 1,
        entity_id=event_id,
        title="股票映射任务",
    )
    init_db()
    with SessionLocal() as db:
        event = get_event(db, UUID(event_id))
        if not event:
            update_model_task(
                "assist",
                task_id,
                status="failed",
                error=f"unknown event: {event_id}",
            )
            raise ValueError(f"unknown event: {event_id}")
        update_model_task(
            "assist",
            task_id,
            status="running",
            entity_id=event_id,
            title=event.headline,
            subtitle=event.event_type.value,
        )
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
                        model=settings.ollama_assist_model,
                        summary=(
                            f"{settings.ollama_assist_model} 正在从原文提及中识别证券，"
                            "并通过主数据验证代码。"
                        ),
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
                        model=settings.ollama_assist_model,
                        summary=(
                            f"{settings.ollama_assist_model} 提出 "
                            f"{mapping_result.proposed_count} 个候选，"
                            f"主数据产品归属补全 {mapping_result.master_derived_count} 个，"
                            f"主数据验证通过 {len(event.candidates)} 个、拒绝 "
                            f"{mapping_result.rejected_count} 个。"
                        ),
                        metrics={
                            "proposed_count": mapping_result.proposed_count,
                            "master_derived_count": mapping_result.master_derived_count,
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
                        model=settings.ollama_assist_model,
                        summary=f"{settings.ollama_assist_model} 二次标的发现任务已完成。",
                    ),
                )
                save_event(db, event)

            if event.candidates:
                queued = enqueue_event_researches(db, event, 3)
                update_model_task(
                    "assist",
                    task_id,
                    status="completed",
                    metrics={
                        "proposed_count": (
                            mapping_result.proposed_count
                            if mapping_result is not None
                            else len(event.candidates)
                        ),
                        "verified_count": len(event.candidates),
                        "rejected_count": (
                            mapping_result.rejected_count if mapping_result is not None else 0
                        ),
                    },
                )
                return {
                    "status": "mapped",
                    "event_id": event_id,
                    "verified_assets": len(event.candidates),
                    "research_queued": len(queued),
                }

            task_id, run = enqueue_event_report(db, event)
            update_model_task(
                "assist",
                str(self.request.id or f"event:{event_id}"),
                status="completed",
                metrics={
                    "proposed_count": (
                        mapping_result.proposed_count if mapping_result is not None else 0
                    ),
                    "verified_count": 0,
                    "rejected_count": (
                        mapping_result.rejected_count if mapping_result is not None else 0
                    ),
                },
            )
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
                    model=settings.ollama_assist_model,
                    summary=(
                        f"{settings.ollama_assist_model} 标的发现"
                        f"{'暂时失败，等待重试' if retrying else '最终失败'}"
                        f"（{type(exc).__name__}）。"
                    ),
                ),
            )
            save_event(db, event)
            update_model_task(
                "assist",
                str(self.request.id or f"event:{event_id}"),
                status="retrying" if retrying else "failed",
                attempt=self.request.retries + 1,
                error=f"{type(exc).__name__}: {exc}",
            )
            if retrying:
                raise self.retry(
                    exc=exc,
                    countdown=min(60, 2 ** (self.request.retries + 1)),
                ) from exc
            raise


@celery_app.task(bind=True, name="market_loop.research_event", max_retries=2)
@model_instance_task("research")
def research_event(
    self,
    event_id: str,
    run_id: str,
    model_instance_id: str | None = None,
) -> dict:
    init_db()
    try:
        client = _redis_client()
        client.ping()
    except Exception:
        client = None
    with SessionLocal() as db:
        event = get_event(db, UUID(event_id))
        run = get_event_research_run(db, UUID(run_id))
        if not event or not run:
            raise ValueError(f"unknown event research run: {run_id}")
        if run.celery_task_id and str(self.request.id) != run.celery_task_id:
            return {
                **run.model_dump(mode="json"),
                "superseded_task_id": str(self.request.id),
            }
        if run.status in {
            RunStatus.COMPLETED,
            RunStatus.INSUFFICIENT_EVIDENCE,
            RunStatus.CANCELLED,
        }:
            return run.model_dump(mode="json")
        if model_instance_id and run.model_instance_id != model_instance_id:
            run.model_instance_id = model_instance_id
            save_event_research_run(db, run)
        with research_lease(
            client,
            run_id=str(run.id),
            task_id=str(self.request.id),
            settings=settings,
        ):
            try:
                result = EventResearchService(db).run(event, run)
                persisted = get_event_research_run(db, run.id)
                if persisted and persisted.status is RunStatus.CANCELLED:
                    return persisted.model_dump(mode="json")
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


@celery_app.task(bind=True, name="market_loop.research_asset")
@model_instance_task("research")
def research_asset(
    self,
    asset_id: str,
    event_id: str | None = None,
    run_id: str | None = None,
    model_instance_id: str | None = None,
) -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        queued_run = get_run(db, UUID(run_id)) if run_id else None
        if (
            queued_run
            and queued_run.celery_task_id
            and str(self.request.id) != queued_run.celery_task_id
        ):
            return {
                **queued_run.model_dump(mode="json"),
                "superseded_task_id": str(self.request.id),
            }
        if queued_run and queued_run.status in {
            RunStatus.COALESCED,
            RunStatus.CANCELLED,
        }:
            return queued_run.model_dump(mode="json")
        if (
            queued_run
            and model_instance_id
            and queued_run.model_instance_id != model_instance_id
        ):
            queued_run.model_instance_id = model_instance_id
            save_run(db, queued_run)
        registry.add_assets(list_assets(db))
        asset = get_asset(db, asset_id) or registry.get_asset(asset_id)
        if not asset:
            raise ValueError(f"unknown asset: {asset_id}")
        event = get_event(db, UUID(event_id)) if event_id else None
        trigger_events = [
            value
            for trigger_id in (queued_run.trigger_event_ids if queued_run else [])
            if (value := get_event(db, trigger_id)) is not None
        ]
    # PostgresSaver.setup() may run concurrent index DDL on first use. Close the
    # read transaction above first so that DDL cannot wait on its own task.
    try:
        client = _redis_client()
        client.ping()
    except Exception:
        client = None
    with research_lease(
        client,
        run_id=str(queued_run.id) if queued_run else str(run_id or self.request.id),
        task_id=str(self.request.id),
        settings=settings,
    ):
        with SessionLocal() as db:
            run = ResearchService(registry, db).run(
                asset,
                event,
                as_of=queued_run.as_of if queued_run else None,
                historical_replay=queued_run.historical_replay if queued_run else False,
                queued_run=queued_run,
                events=trigger_events,
            )
            persisted = get_run(db, run.id)
            if persisted and persisted.status is RunStatus.CANCELLED:
                return persisted.model_dump(mode="json")
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


@celery_app.task(name="market_loop.refresh_event_market_factors")
def refresh_event_market_factors() -> dict[str, int]:
    """Re-run a bounded set of events after 1/5/20-day market windows mature."""

    init_db()
    now = utc_now()
    completed: dict[tuple[str, str], int] = {}
    active: set[tuple[str, str]] = set()
    queued = failed = 0
    with SessionLocal() as db:
        for run in list_runs(db, limit=50_000):
            for step in run.analysis_steps:
                if step.phase != "market_factor_refresh_queue":
                    continue
                event_id = str(step.metrics.get("event_id") or run.event_id or "")
                session = int(step.metrics.get("target_session_days") or 0)
                if not event_id or session not in {1, 5, 20}:
                    continue
                key = (event_id, run.asset.asset_id)
                if run.status in {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}:
                    active.add(key)
                elif run.status in {
                    RunStatus.COMPLETED,
                    RunStatus.INSUFFICIENT_EVIDENCE,
                }:
                    completed[key] = max(completed.get(key, 0), session)

        for event in list_events(db, limit=5000):
            if queued >= MARKET_FACTOR_REFRESH_BATCH_SIZE:
                break
            event_at = as_utc(event.published_at)
            age_days = (now - event_at).total_seconds() / 86_400
            if age_days < 0 or age_days > MARKET_FACTOR_REFRESH_EVENT_DAYS:
                continue
            if not event.candidates:
                continue
            candidate = max(
                event.candidates,
                key=lambda item: (item.relevance, item.mapping_confidence),
            )
            if min(candidate.relevance, candidate.mapping_confidence) < 0.65:
                continue
            key = (str(event.id), candidate.asset.asset_id)
            if key in active:
                continue
            target = _due_market_factor_refresh_session(
                age_days=age_days,
                completed_session=completed.get(key, 0),
            )
            if target is None:
                continue
            try:
                enqueue_research(
                    db,
                    candidate.asset,
                    event,
                    as_of=now,
                    market_factor_refresh_days=target,
                )
                queued += 1
                active.add(key)
            except ResearchAdmissionError:
                continue
            except Exception:
                failed += 1
    return {"queued": queued, "failed": failed}


@celery_app.task(name="market_loop.seed_assets")
def seed_assets() -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        for asset in registry.all_assets():
            upsert_asset(db, asset)
        count = len(list_assets(db))
    return {"assets": count}


def _run_evolution_failures(failures: list[dict], *, task_id: str, source: str) -> dict:
    update_model_task(
        "code",
        task_id,
        status="generating",
        title=f"失败案例代码演进（{len(failures)} 条）",
        subtitle="正在生成改进方案",
        source=source,
    )
    try:
        init_db()
        with SessionLocal() as db:
            service = EvolutionService()
            candidate = service.propose(db, failures)
            update_model_task(
                "code",
                task_id,
                status="testing",
                entity_id=str(candidate.id),
                title=candidate.hypothesis,
                subtitle=candidate.target_metric,
                source=source,
                metrics={
                    "target_metric": candidate.target_metric,
                    "branch": candidate.branch,
                    "expected_improvement": candidate.expected_improvement,
                },
            )
            result = service.execute(db, candidate)
            update_model_task(
                "code",
                task_id,
                status=result.status.value,
                entity_id=str(result.id),
                title=result.hypothesis,
                subtitle=result.target_metric,
                source=source,
                metrics={
                    "target_metric": result.target_metric,
                    "branch": result.branch,
                    "expected_improvement": result.expected_improvement,
                    "baseline_score": result.baseline_score,
                    "candidate_score": result.candidate_score,
                },
                error=(
                    "代码演进候选被拒绝或回滚。"
                    if result.status.value in {"rejected", "rolled_back"}
                    else None
                ),
            )
            notifier.send(
                f"演进结果：{result.status.value}\n分支：{result.branch}\n假设：{result.hypothesis}"
            )
            return result.model_dump(mode="json")
    except Exception as exc:
        update_model_task(
            "code",
            task_id,
            status="failed",
            source=source,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise


def _evolution_failure_cases(db) -> list[dict[str, Any]]:
    """Join outcomes to the mapping, evidence and scoring context needed to fix code."""

    recommendations = {item.id: item for item in list_recommendations(db, limit=50_000)}
    runs = list_runs(db, limit=50_000)
    runs_by_id = {item.id: item for item in runs}
    events = {item.id: item for item in list_events(db, limit=5000)}

    def context_for(recommendation, run, failure_type: str, outcome=None) -> dict[str, Any]:
        event = events.get(run.event_id) if run and run.event_id else None
        raw_factor_metrics = next(
            (
                step.metrics
                for step in reversed(run.analysis_steps if run else [])
                if step.phase == "evidence_gathering"
            ),
            {},
        )
        factor_metrics = {
            key: (value[:10] if isinstance(value, list) else value)
            for key, value in raw_factor_metrics.items()
            if key
            in {
                "provider_groups",
                "research_factor_count",
                "research_factor_reliability",
                "research_factor_missing",
            }
        }
        return {
            "failure_type": failure_type,
            "outcome": outcome.model_dump(mode="json") if outcome else None,
            "recommendation": (
                {
                    "id": str(recommendation.id),
                    "asset": recommendation.asset.model_dump(mode="json"),
                    "score": recommendation.score,
                    "raw_score": recommendation.raw_score,
                    "model_score": recommendation.model_score,
                    "model_direction": (
                        recommendation.model_direction.value
                        if recommendation.model_direction
                        else None
                    ),
                    "model_rating": (
                        recommendation.model_rating.value
                        if recommendation.model_rating
                        else None
                    ),
                    "model_confidence": recommendation.model_confidence,
                    "probabilities": [
                        recommendation.bull_probability,
                        recommendation.base_probability,
                        recommendation.bear_probability,
                    ],
                    "signal_status": recommendation.signal_status.value,
                    "evidence_strength": recommendation.evidence_strength,
                    "mapping_confidence": recommendation.mapping_confidence,
                    "gate_reasons": recommendation.gate_reasons[:10],
                    "horizon_days": recommendation.horizon_days,
                    "scoring_version": recommendation.scoring_version,
                    "calibration_version": recommendation.calibration_version,
                }
                if recommendation
                else None
            ),
            "research": (
                {
                    "run_id": str(run.id),
                    "status": run.status.value,
                    "error": run.error,
                    "retryable_reason": run.retryable_reason,
                    "missing_requirements": run.missing_requirements[:10],
                    "contradictions": run.contradictions[:10],
                    "evidence_count": len(run.evidence),
                    "source_qualities": sorted(
                        {item.source_quality.value for item in run.evidence}
                    ),
                    "factor_metrics": factor_metrics,
                }
                if run
                else None
            ),
            "event": (
                {
                    "id": str(event.id),
                    "headline": event.headline,
                    "event_type": event.event_type.value,
                    "published_at": event.published_at.isoformat(),
                    "candidates": [
                        {
                            "asset_id": item.asset.asset_id,
                            "relationship": item.relationship,
                            "relevance": item.relevance,
                            "mapping_confidence": item.mapping_confidence,
                            "identity_basis": item.identity_basis,
                        }
                        for item in event.candidates[:3]
                    ],
                }
                if event
                else None
            ),
        }

    failures: list[dict[str, Any]] = []
    covered_runs: set[UUID] = set()
    for outcome in list_outcomes(db):
        if outcome.direction_correct and (outcome.alpha is None or outcome.alpha >= 0):
            continue
        recommendation = recommendations.get(outcome.recommendation_id)
        run = runs_by_id.get(recommendation.run_id) if recommendation else None
        if run:
            covered_runs.add(run.id)
        failures.append(context_for(recommendation, run, "outcome_miss", outcome))
        if len(failures) >= 30:
            break

    for run in runs:
        if run.id in covered_runs or run.status not in {
            RunStatus.FAILED,
            RunStatus.INSUFFICIENT_EVIDENCE,
        }:
            continue
        recommendation = run.recommendation
        failure_type = (
            "technical_failure"
            if run.status is RunStatus.FAILED or run.retryable_reason
            else "evidence_gap"
        )
        failures.append(context_for(recommendation, run, failure_type))
        if len(failures) >= 50:
            break
    return failures


@celery_app.task(name="market_loop.dispatch_evolve_from_outcomes")
def dispatch_evolve_from_outcomes() -> dict[str, str]:
    """Bind the periodic evolution run before it enters the model queue."""

    task_id = str(uuid4())
    instance = select_model_instance(
        "code",
        task_id=task_id,
        settings=settings,
        probe_health=True,
    )
    record_model_task(
        "code",
        task_id=task_id,
        kind="code_evolution",
        title="定期失败案例代码演进",
        subtitle="根据历史研究结果生成改进方案",
        source="automatic",
        instance_id=instance.id,
    )
    try:
        task = evolve_from_outcomes.apply_async(
            kwargs={"model_instance_id": instance.id},
            queue=broker_queue_name("code", instance.id),
            task_id=task_id,
        )
    except Exception as exc:
        update_model_task(
            "code",
            task_id,
            status="failed",
            source="automatic",
            error=f"{type(exc).__name__}: {exc}",
        )
        raise
    return {"task_id": str(task.id), "instance_id": instance.id}


@celery_app.task(bind=True, name="market_loop.evolve_from_outcomes")
@model_instance_task("code")
def evolve_from_outcomes(self, model_instance_id: str | None = None) -> dict:
    task_id = str(self.request.id or uuid4())
    if model_task_is_cancelled("code", task_id):
        return {"status": "cancelled"}
    update_model_task(
        "code",
        task_id,
        status="running",
        title="定期失败案例代码演进",
        subtitle="根据历史研究结果生成改进方案",
        source="automatic",
        instance_id=model_instance_id,
    )
    if not settings.evolution_enabled:
        update_model_task("code", task_id, status="completed", source="automatic")
        return {"status": "disabled"}
    init_db()
    with SessionLocal() as db:
        failures = _evolution_failure_cases(db)
        if not failures:
            update_model_task("code", task_id, status="completed", source="automatic")
            return {"status": "no-failures"}
    return _run_evolution_failures(failures, task_id=task_id, source="automatic")


@celery_app.task(bind=True, name="market_loop.evolve_failures")
@model_instance_task("code")
def evolve_failures(
    self,
    failures: list[dict],
    model_instance_id: str | None = None,
) -> dict:
    task_id = str(self.request.id or uuid4())
    if model_task_is_cancelled("code", task_id):
        return {"status": "cancelled"}
    if not settings.evolution_enabled:
        update_model_task(
            "code",
            task_id,
            status="failed",
            source="manual",
            error="EVOLUTION_ENABLED is false",
        )
        return {"status": "disabled"}
    update_model_task(
        "code",
        task_id,
        status="generating",
        title=f"失败案例代码演进（{len(failures)} 条）",
        subtitle="正在生成改进方案",
        source="manual",
    )
    return _run_evolution_failures(failures, task_id=task_id, source="manual")


@celery_app.task(bind=True, name="market_loop.execute_evolution")
@model_instance_task("code")
def execute_evolution(
    self,
    candidate_id: str,
    model_instance_id: str | None = None,
) -> dict:
    task_id = str(self.request.id or uuid4())
    if model_task_is_cancelled("code", task_id):
        return {"status": "cancelled", "candidate_id": candidate_id}
    init_db()
    with SessionLocal() as db:
        candidate = next(
            (item for item in list_evolutions(db) if str(item.id) == candidate_id), None
        )
        if not candidate:
            update_model_task(
                "code",
                task_id,
                status="failed",
                entity_id=candidate_id,
                error=f"unknown evolution candidate: {candidate_id}",
            )
            raise ValueError(f"unknown evolution candidate: {candidate_id}")
        update_model_task(
            "code",
            task_id,
            status="testing",
            entity_id=candidate_id,
            title=candidate.hypothesis,
            subtitle=candidate.target_metric,
            source="manual",
            metrics={
                "target_metric": candidate.target_metric,
                "branch": candidate.branch,
                "expected_improvement": candidate.expected_improvement,
            },
        )
        try:
            result = EvolutionService().execute(db, candidate)
            update_model_task(
                "code",
                task_id,
                status=result.status.value,
                entity_id=str(result.id),
                title=result.hypothesis,
                subtitle=result.target_metric,
                source="manual",
                metrics={
                    "target_metric": result.target_metric,
                    "branch": result.branch,
                    "expected_improvement": result.expected_improvement,
                    "baseline_score": result.baseline_score,
                    "candidate_score": result.candidate_score,
                },
                error=(
                    "代码演进候选被拒绝或回滚。"
                    if result.status.value in {"rejected", "rolled_back"}
                    else None
                ),
            )
            notifier.send(f"演进结果：{result.status.value}\n分支：{result.branch}")
            return result.model_dump(mode="json")
        except Exception as exc:
            update_model_task(
                "code",
                task_id,
                status="failed",
                entity_id=candidate_id,
                source="manual",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise


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
        and (utc_now() - as_utc(latest_news)).total_seconds() > settings.scan_interval_minutes * 180
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
