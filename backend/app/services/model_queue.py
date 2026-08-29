from __future__ import annotations

import json
from datetime import datetime, timedelta
from math import ceil
from typing import Any, Literal

from pydantic import BaseModel, Field
from redis import Redis
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AnalysisStep,
    EventResearchRun,
    EvolutionCandidate,
    NewsEvent,
    ResearchRun,
    RunStatus,
    as_utc,
    utc_now,
)
from backend.app.model_audit import redact_text
from backend.app.services.model_instances import (
    instance_assignment,
    record_instance_assignment,
    update_instance_assignment,
)
from backend.app.services.research_lifecycle import live_research_run_ids
from backend.app.services.research_queue import build_research_queue
from backend.app.storage import (
    list_event_research_runs,
    list_events,
    list_evolutions,
    list_runs,
)

MODEL_TASK_RETENTION = timedelta(hours=24)
RECENT_EXECUTION_WINDOW = timedelta(hours=4)
MODEL_TASK_KEY_TTL_SECONDS = 48 * 60 * 60
MODEL_TASK_HASH_PREFIX = "market-loop:model-queue"
TrackedModelLane = Literal["extract", "assist", "code"]
ACTIVE_STATUSES = {
    "queued",
    "running",
    "retrying",
    "verifying",
    "generating",
    "testing",
    "merging",
    "proposed",
}
RUNNING_STATUSES = {"running", "generating", "testing", "merging"}
LEASED_STATUSES = RUNNING_STATUSES | {"retrying"}
FAILED_STATUSES = {"failed", "rejected", "rolled_back"}
COMPLETED_STATUSES = {"completed", "merged", "unmapped", "insufficient_evidence"}
STATUS_PRIORITY = {
    "running": 0,
    "generating": 0,
    "testing": 0,
    "merging": 0,
    "retrying": 1,
    "verifying": 1,
    "queued": 2,
    "proposed": 2,
    "failed": 3,
    "rejected": 3,
    "rolled_back": 3,
}


class ModelQueueCounts(BaseModel):
    queued: int = 0
    running: int = 0
    retrying: int = 0
    verifying: int = 0
    waiting_for_model: int = 0
    completed: int = 0
    failed: int = 0


class ModelQueueMetrics(BaseModel):
    average_queue_duration_ms: int | None = Field(default=None, ge=0)
    average_execution_duration_ms: int | None = Field(default=None, ge=0)
    longest_wait_ms: int | None = Field(default=None, ge=0)
    estimated_clear_ms: int | None = Field(default=None, ge=0)
    queue_duration_sample_count: int = Field(default=0, ge=0)
    execution_duration_sample_count: int = Field(default=0, ge=0)
    execution_p50_ms: int | None = Field(default=None, ge=0)
    execution_p90_ms: int | None = Field(default=None, ge=0)
    throughput_per_hour: float | None = Field(default=None, ge=0)


class ModelQueueTask(BaseModel):
    task_id: str
    instance_id: str | None = None
    kind: str
    entity_id: str | None = None
    title: str
    subtitle: str = ""
    source: str | None = None
    status: str
    attempt: int = Field(default=1, ge=0)
    task_count: int = Field(default=1, ge=1)
    queued_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime
    queue_duration_ms: int | None = Field(default=None, ge=0)
    execution_duration_ms: int | None = Field(default=None, ge=0)
    error: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class ModelQueueInstance(BaseModel):
    id: str
    healthy: bool
    model_available: bool
    state: str = "idle"
    capacity: int = Field(default=0, ge=0)
    available: int = Field(default=0, ge=0)
    observable: bool = True
    counts: ModelQueueCounts = Field(default_factory=ModelQueueCounts)
    metrics: ModelQueueMetrics = Field(default_factory=ModelQueueMetrics)
    total_tasks: int = Field(default=0, ge=0)
    truncated: bool = False
    tasks: list[ModelQueueTask] = Field(default_factory=list)


class ModelQueueItem(BaseModel):
    id: Literal["extract", "research", "assist", "code"]
    model: str
    purpose: str
    binding: str
    enabled: bool = True
    state: str
    threads: int = Field(ge=0)
    capacity: int = Field(ge=0)
    available: int = Field(ge=0)
    instance_count: int = Field(ge=0)
    per_instance_concurrency: int = Field(ge=0)
    observable: bool = True
    instances: list[ModelQueueInstance] = Field(default_factory=list)
    counts: ModelQueueCounts
    metrics: ModelQueueMetrics
    total_tasks: int = Field(ge=0)
    truncated: bool = False
    tasks: list[ModelQueueTask] = Field(default_factory=list)
    error: str | None = None


class ModelQueueOverviewResponse(BaseModel):
    generated_at: datetime
    queues: list[ModelQueueItem]


class ModelTaskCancellationResult(BaseModel):
    cancelled: int = 0
    celery_task_ids: list[str] = Field(default_factory=list)


def _task_hash_key(lane: str) -> str:
    return f"{MODEL_TASK_HASH_PREFIX}:{lane}:tasks"


def _redis_client(settings: Settings | None = None) -> Redis:
    active_settings = settings or get_settings()
    return Redis.from_url(active_settings.redis_url, socket_connect_timeout=1)


def _safe_error(error: str | None) -> str | None:
    if not error:
        return None
    return redact_text(error)[:500]


def record_model_task(
    lane: TrackedModelLane,
    *,
    task_id: str,
    kind: str,
    title: str,
    entity_id: str | None = None,
    subtitle: str = "",
    source: str | None = None,
    instance_id: str | None = None,
    queued_at: datetime | None = None,
    redis_client: Redis | None = None,
) -> None:
    now = as_utc(queued_at or utc_now())
    payload = {
        "task_id": task_id,
        "instance_id": instance_id,
        "kind": kind,
        "entity_id": entity_id,
        "title": title,
        "subtitle": subtitle,
        "source": source,
        "status": "queued",
        "attempt": 1,
        "task_count": 1,
        "queued_at": now.isoformat(),
        "started_at": None,
        "completed_at": None,
        "updated_at": now.isoformat(),
        "error": None,
        "metrics": {},
    }
    try:
        client = redis_client or _redis_client()
        key = _task_hash_key(lane)
        current = client.hget(key, task_id)
        if current:
            value = current.decode() if isinstance(current, bytes) else current
            if json.loads(value).get("status") == "cancelled":
                return
        client.hset(key, task_id, json.dumps(payload, ensure_ascii=False))
        client.expire(key, MODEL_TASK_KEY_TTL_SECONDS)
        if instance_id:
            record_instance_assignment(
                lane,
                task_id,
                instance_id,
                redis_client=client,
            )
    except Exception:
        return


def cancel_model_tasks(
    lane: TrackedModelLane,
    *,
    include_failed: bool = False,
    instance_id: str | None = None,
    redis_client: Redis | None = None,
) -> ModelTaskCancellationResult:
    """Cancel active and optionally failed tasks for one isolated model lane."""

    try:
        client = redis_client or _redis_client()
        key = _task_hash_key(lane)
        now_value = utc_now()
        now = now_value.isoformat()
        failed_cutoff = now_value - MODEL_TASK_RETENTION
        task_ids: list[str] = []
        cancelled = 0
        clearable_statuses = ACTIVE_STATUSES | (FAILED_STATUSES if include_failed else set())
        for field, raw in client.hgetall(key).items():
            task_id = field.decode() if isinstance(field, bytes) else str(field)
            value = raw.decode() if isinstance(raw, bytes) else raw
            try:
                payload = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
            status = payload.get("status")
            if status not in clearable_statuses:
                continue
            assigned_instance = payload.get("instance_id") or instance_assignment(
                lane, str(payload.get("task_id") or task_id), redis_client=client
            )
            if instance_id is not None and assigned_instance != instance_id:
                continue
            if status in FAILED_STATUSES:
                updated_at = _parse_datetime(payload.get("updated_at"))
                if updated_at is None or updated_at < failed_cutoff:
                    continue
            payload.update(
                status="cancelled",
                updated_at=now,
                completed_at=now,
                error=None,
            )
            client.hset(key, task_id, json.dumps(payload, ensure_ascii=False))
            update_instance_assignment(
                lane,
                str(payload.get("task_id") or task_id),
                status="cancelled",
                instance_id=assigned_instance,
                redis_client=client,
            )
            cancelled += 1
            if status in ACTIVE_STATUSES:
                task_ids.append(str(payload.get("task_id") or task_id))
        client.expire(key, MODEL_TASK_KEY_TTL_SECONDS)
        return ModelTaskCancellationResult(cancelled=cancelled, celery_task_ids=task_ids)
    except Exception:
        return ModelTaskCancellationResult()


def cancel_model_task(
    lane: TrackedModelLane,
    task_id: str,
    *,
    redis_client: Redis | None = None,
) -> bool:
    """Cancel one tracked model task so a replacement can supersede it."""

    try:
        client = redis_client or _redis_client()
        key = _task_hash_key(lane)
        raw = client.hget(key, task_id)
        if not raw:
            return False
        value = raw.decode() if isinstance(raw, bytes) else raw
        payload = json.loads(value)
        if payload.get("status") not in ACTIVE_STATUSES:
            return False
        now = utc_now().isoformat()
        payload.update(
            status="cancelled",
            updated_at=now,
            completed_at=now,
            error=None,
        )
        client.hset(key, task_id, json.dumps(payload, ensure_ascii=False))
        client.expire(key, MODEL_TASK_KEY_TTL_SECONDS)
        update_instance_assignment(
            lane,
            task_id,
            status="cancelled",
            instance_id=payload.get("instance_id"),
            redis_client=client,
        )
        return True
    except Exception:
        return False


def model_task_is_cancelled(
    lane: TrackedModelLane,
    task_id: str,
    *,
    redis_client: Redis | None = None,
) -> bool:
    try:
        client = redis_client or _redis_client()
        raw = client.hget(_task_hash_key(lane), task_id)
        if not raw:
            return False
        value = raw.decode() if isinstance(raw, bytes) else raw
        return json.loads(value).get("status") == "cancelled"
    except Exception:
        return False


def update_model_task(
    lane: TrackedModelLane,
    task_id: str,
    *,
    status: str,
    attempt: int | None = None,
    entity_id: str | None = None,
    title: str | None = None,
    subtitle: str | None = None,
    source: str | None = None,
    instance_id: str | None = None,
    error: str | None = None,
    metrics: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
    redis_client: Redis | None = None,
) -> None:
    now = as_utc(occurred_at or utc_now())
    try:
        client = redis_client or _redis_client()
        key = _task_hash_key(lane)
        raw = client.hget(key, task_id)
        default_kind = {
            "extract": "news_extraction",
            "assist": "asset_mapping",
            "code": "code_evolution",
        }[lane]
        default_title = {
            "extract": "新闻抽取任务",
            "assist": "股票映射任务",
            "code": "代码演进任务",
        }[lane]
        payload = (
            json.loads(raw)
            if raw
            else {
                "task_id": task_id,
                "instance_id": instance_id,
                "kind": default_kind,
                "entity_id": entity_id,
                "title": title or default_title,
                "subtitle": subtitle or "",
                "source": source,
                "status": "queued",
                "attempt": 1,
                "task_count": 1,
                "queued_at": now.isoformat(),
                "started_at": None,
                "completed_at": None,
                "updated_at": now.isoformat(),
                "error": None,
                "metrics": {},
            }
        )
        if payload.get("status") == "cancelled":
            return
        payload["status"] = status
        payload["updated_at"] = now.isoformat()
        if attempt is not None:
            payload["attempt"] = max(0, attempt)
        if entity_id is not None:
            payload["entity_id"] = entity_id
        if title is not None:
            payload["title"] = title
        if subtitle is not None:
            payload["subtitle"] = subtitle
        if source is not None:
            payload["source"] = source
        if instance_id is not None:
            payload["instance_id"] = instance_id
        if error is not None or status not in FAILED_STATUSES:
            payload["error"] = _safe_error(error)
        if metrics:
            payload["metrics"] = {**payload.get("metrics", {}), **metrics}
        if status in RUNNING_STATUSES and not payload.get("started_at"):
            payload["started_at"] = now.isoformat()
        if status in FAILED_STATUSES | COMPLETED_STATUSES | {"cancelled"}:
            payload["completed_at"] = now.isoformat()
        client.hset(key, task_id, json.dumps(payload, ensure_ascii=False))
        client.expire(key, MODEL_TASK_KEY_TTL_SECONDS)
        update_instance_assignment(
            lane,
            task_id,
            status=status,
            instance_id=payload.get("instance_id"),
            redis_client=client,
        )
    except Exception:
        return


def touch_model_task(
    lane: TrackedModelLane,
    task_id: str,
    *,
    instance_id: str | None = None,
    occurred_at: datetime | None = None,
    redis_client: Redis | None = None,
) -> bool:
    """Refresh a running task lease without reviving terminal work."""

    now = as_utc(occurred_at or utc_now())
    try:
        client = redis_client or _redis_client()
        key = _task_hash_key(lane)
        raw = client.hget(key, task_id)
        if not raw:
            return False
        value = raw.decode() if isinstance(raw, bytes) else raw
        payload = json.loads(value)
        if payload.get("status") not in RUNNING_STATUSES:
            return False
        payload["updated_at"] = now.isoformat()
        if instance_id is not None:
            payload["instance_id"] = instance_id
        client.hset(key, task_id, json.dumps(payload, ensure_ascii=False))
        client.expire(key, MODEL_TASK_KEY_TTL_SECONDS)
        update_instance_assignment(
            lane,
            task_id,
            status=str(payload["status"]),
            instance_id=payload.get("instance_id"),
            redis_client=client,
        )
        return True
    except Exception:
        return False


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    try:
        return as_utc(datetime.fromisoformat(str(value)))
    except ValueError:
        return None


def _duration_ms(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return max(0, int((as_utc(end) - as_utc(start)).total_seconds() * 1000))


def _task_from_payload(payload: dict[str, Any], now: datetime) -> ModelQueueTask | None:
    queued_at = _parse_datetime(payload.get("queued_at"))
    if queued_at is None:
        return None
    started_at = _parse_datetime(payload.get("started_at"))
    completed_at = _parse_datetime(payload.get("completed_at"))
    updated_at = _parse_datetime(payload.get("updated_at")) or queued_at
    status = str(payload.get("status") or "queued")
    queue_end = started_at or (now if status in {"queued", "retrying", "proposed"} else None)
    execution_end = completed_at or (now if status in RUNNING_STATUSES else None)
    return ModelQueueTask(
        task_id=str(payload.get("task_id") or "unknown"),
        instance_id=(str(payload["instance_id"]) if payload.get("instance_id") else None),
        kind=str(payload.get("kind") or "task"),
        entity_id=str(payload["entity_id"]) if payload.get("entity_id") else None,
        title=str(payload.get("title") or "未命名任务"),
        subtitle=str(payload.get("subtitle") or ""),
        source=str(payload["source"]) if payload.get("source") else None,
        status=status,
        attempt=max(0, int(payload.get("attempt") or 0)),
        task_count=max(1, int(payload.get("task_count") or 1)),
        queued_at=queued_at,
        started_at=started_at,
        completed_at=completed_at,
        updated_at=updated_at,
        queue_duration_ms=_duration_ms(queued_at, queue_end),
        execution_duration_ms=_duration_ms(started_at, execution_end),
        error=_safe_error(payload.get("error")),
        metrics=dict(payload.get("metrics") or {}),
    )


def list_model_task_records(
    lane: TrackedModelLane,
    *,
    now: datetime | None = None,
    redis_client: Redis | None = None,
) -> list[ModelQueueTask]:
    generated_at = as_utc(now or utc_now())
    cutoff = generated_at - MODEL_TASK_RETENTION
    try:
        client = redis_client or _redis_client()
        key = _task_hash_key(lane)
        records: list[ModelQueueTask] = []
        expired: list[str] = []
        for task_id, raw in client.hgetall(key).items():
            try:
                payload = json.loads(raw)
                item = _task_from_payload(payload, generated_at)
            except (TypeError, ValueError, json.JSONDecodeError):
                item = None
            if item is None:
                expired.append(task_id.decode() if isinstance(task_id, bytes) else str(task_id))
                continue
            if item.status not in ACTIVE_STATUSES and item.updated_at < cutoff:
                expired.append(item.task_id)
                continue
            assigned = instance_assignment(lane, item.task_id, redis_client=client)
            if assigned:
                item = item.model_copy(update={"instance_id": assigned})
            records.append(item)
        if expired:
            client.hdel(key, *expired)
        return records
    except Exception:
        return []


def stale_model_task_records(
    lane: TrackedModelLane,
    *,
    lease_seconds: int,
    now: datetime | None = None,
    redis_client: Redis | None = None,
) -> list[ModelQueueTask]:
    """Return in-flight tasks whose last heartbeat or retry update is stale."""

    generated_at = as_utc(now or utc_now())
    cutoff = generated_at - timedelta(seconds=max(1, lease_seconds))
    return [
        item
        for item in list_model_task_records(lane, now=generated_at, redis_client=redis_client)
        if item.status in LEASED_STATUSES and item.updated_at <= cutoff
    ]


def _latest_step(event: NewsEvent, phase: str) -> AnalysisStep | None:
    return next(
        (step for step in reversed(event.analysis_steps) if step.phase == phase),
        None,
    )


def _event_mapping_tasks(events: list[NewsEvent], now: datetime) -> list[ModelQueueTask]:
    cutoff = now - MODEL_TASK_RETENTION
    tasks: list[ModelQueueTask] = []
    for event in events:
        queue_step = _latest_step(event, "asset_mapping_queue")
        mapping_step = _latest_step(event, "asset_mapping")
        if queue_step is None:
            continue
        status = queue_step.status
        updated_at = as_utc(queue_step.occurred_at)
        metrics: dict[str, Any] = {}
        error = None
        started_at = None
        completed_at = None
        if mapping_step is not None:
            status = mapping_step.status
            updated_at = as_utc(mapping_step.occurred_at)
            metrics = dict(mapping_step.metrics or {})
            if status in {"running", "retrying"}:
                started_at = updated_at
            elif status in {"completed", "unmapped", "failed"}:
                completed_at = updated_at
            if status == "failed":
                error = mapping_step.summary
        if status not in ACTIVE_STATUSES | FAILED_STATUSES | COMPLETED_STATUSES:
            continue
        if status not in ACTIVE_STATUSES and updated_at < cutoff:
            continue
        queued_at = as_utc(queue_step.occurred_at)
        tasks.append(
            ModelQueueTask(
                task_id=f"event:{event.id}",
                instance_id=(
                    str(queue_step.metrics["instance_id"])
                    if queue_step.metrics.get("instance_id")
                    else None
                ),
                kind="asset_mapping",
                entity_id=str(event.id),
                title=event.headline,
                subtitle=event.event_type.value,
                source="automatic",
                status="completed" if status == "unmapped" else status,
                attempt=1,
                queued_at=queued_at,
                started_at=started_at,
                completed_at=completed_at,
                updated_at=updated_at,
                queue_duration_ms=_duration_ms(
                    queued_at,
                    started_at or (now if status in {"queued", "retrying"} else None),
                ),
                execution_duration_ms=_duration_ms(
                    started_at,
                    completed_at or (now if status == "running" else None),
                ),
                error=_safe_error(error),
                metrics=metrics,
            )
        )
    return tasks


def _evolution_candidate_tasks(
    candidates: list[EvolutionCandidate],
    now: datetime,
    redis_client: Redis | None = None,
) -> list[ModelQueueTask]:
    cutoff = now - MODEL_TASK_RETENTION
    tasks: list[ModelQueueTask] = []
    for candidate in candidates:
        status = candidate.status.value
        created_at = as_utc(candidate.created_at)
        if status not in {"proposed", "testing"} and created_at < cutoff:
            continue
        normalized = {
            "proposed": "queued",
            "testing": "testing",
            "merged": "merged",
            "rejected": "rejected",
            "rolled_back": "rolled_back",
        }[status]
        tasks.append(
            ModelQueueTask(
                task_id=f"evolution:{candidate.id}",
                instance_id=(
                    instance_assignment(
                        "code",
                        f"evolution:{candidate.id}",
                        redis_client=redis_client,
                    )
                    if redis_client is not None
                    else None
                ),
                kind="code_evolution",
                entity_id=str(candidate.id),
                title=candidate.hypothesis,
                subtitle=candidate.target_metric,
                source="candidate",
                status=normalized,
                attempt=1,
                queued_at=created_at,
                started_at=created_at if status == "testing" else None,
                completed_at=(
                    created_at if status in {"merged", "rejected", "rolled_back"} else None
                ),
                updated_at=created_at,
                queue_duration_ms=0 if status == "testing" else None,
                execution_duration_ms=None,
                error=(
                    "代码演进候选被拒绝或回滚。" if status in {"rejected", "rolled_back"} else None
                ),
                metrics={
                    "target_metric": candidate.target_metric,
                    "branch": candidate.branch,
                    "expected_improvement": candidate.expected_improvement,
                    "baseline_score": candidate.baseline_score,
                    "candidate_score": candidate.candidate_score,
                },
            )
        )
    return tasks


def _merge_tasks(
    primary: list[ModelQueueTask], fallback: list[ModelQueueTask]
) -> list[ModelQueueTask]:
    task_ids = {item.task_id for item in primary}
    entity_ids = {item.entity_id for item in primary if item.entity_id}
    return [
        *primary,
        *(
            item
            for item in fallback
            if item.task_id not in task_ids
            and (not item.entity_id or item.entity_id not in entity_ids)
        ),
    ]


def _sort_tasks(tasks: list[ModelQueueTask]) -> list[ModelQueueTask]:
    def key(item: ModelQueueTask) -> tuple[int, float]:
        priority = STATUS_PRIORITY.get(item.status, 4)
        timestamp = item.updated_at.timestamp()
        if item.status in FAILED_STATUSES:
            return priority, -timestamp
        return priority, item.queued_at.timestamp()

    return sorted(tasks, key=key)


def _counts(tasks: list[ModelQueueTask]) -> ModelQueueCounts:
    counts = ModelQueueCounts()
    for item in tasks:
        if item.status in {"queued", "proposed"}:
            counts.queued += 1
        elif item.status in RUNNING_STATUSES:
            counts.running += 1
        elif item.status == "retrying":
            counts.retrying += 1
        elif item.status == "verifying":
            counts.verifying += 1
        elif item.status in FAILED_STATUSES:
            counts.failed += 1
        elif item.status in COMPLETED_STATUSES:
            counts.completed += 1
    return counts


def _metrics(
    tasks: list[ModelQueueTask],
    counts: ModelQueueCounts,
    capacity: int,
    *,
    generated_at: datetime,
    use_throughput: bool = False,
    use_recent_throughput: bool = False,
) -> ModelQueueMetrics:
    queue_durations = [
        item.queue_duration_ms for item in tasks if item.queue_duration_ms is not None
    ]
    execution_durations = [
        item.execution_duration_ms
        for item in tasks
        if item.execution_duration_ms is not None
        and (item.status in COMPLETED_STATUSES or item.status in FAILED_STATUSES)
    ]
    recent_execution_cutoff = generated_at - RECENT_EXECUTION_WINDOW
    recent_execution_durations = [
        item.execution_duration_ms
        for item in tasks
        if item.execution_duration_ms is not None
        and (item.status in COMPLETED_STATUSES or item.status in FAILED_STATUSES)
        and as_utc(item.completed_at or item.updated_at) >= recent_execution_cutoff
    ]
    waiting = [
        item.queue_duration_ms
        for item in tasks
        if item.status in {"queued", "retrying"} and item.queue_duration_ms is not None
    ]
    average_execution = (
        sum(recent_execution_durations) // len(recent_execution_durations)
        if recent_execution_durations
        else None
    )
    work = counts.queued + counts.retrying + counts.running + counts.verifying
    completed = [
        item
        for item in tasks
        if item.status in COMPLETED_STATUSES and item.completed_at is not None
    ]
    recent_completed = sorted(
        (
            item
            for item in completed
            if as_utc(item.completed_at or item.updated_at) >= recent_execution_cutoff
        ),
        key=lambda item: as_utc(item.completed_at or item.updated_at),
    )
    throughput_per_hour = None
    if use_recent_throughput:
        if len(recent_completed) >= 2:
            elapsed_hours = (
                as_utc(recent_completed[-1].completed_at or recent_completed[-1].updated_at)
                - as_utc(recent_completed[0].completed_at or recent_completed[0].updated_at)
            ).total_seconds() / 3600
            if elapsed_hours > 0:
                throughput_per_hour = (len(recent_completed) - 1) / elapsed_hours
        elif (
            len(recent_completed) == 1
            and recent_completed[0].execution_duration_ms
            and recent_completed[0].execution_duration_ms > 0
        ):
            throughput_per_hour = 60 * 60 * 1000 / recent_completed[0].execution_duration_ms
    elif use_throughput and completed:
        throughput_per_hour = len(completed) / 24
    estimated_clear = None
    if throughput_per_hour and work > 0:
        estimated_clear = ceil(work / throughput_per_hour * 60 * 60 * 1000)
    elif not use_recent_throughput and average_execution is not None and capacity > 0 and work > 0:
        estimated_clear = ceil(work / capacity) * average_execution
    ordered_execution = sorted(
        recent_execution_durations if use_recent_throughput else execution_durations
    )

    def percentile(ratio: float) -> int | None:
        if not ordered_execution:
            return None
        index = ceil(len(ordered_execution) * ratio) - 1
        return ordered_execution[max(0, min(index, len(ordered_execution) - 1))]

    return ModelQueueMetrics(
        average_queue_duration_ms=(
            sum(queue_durations) // len(queue_durations) if queue_durations else None
        ),
        average_execution_duration_ms=average_execution,
        longest_wait_ms=max(waiting) if waiting else None,
        estimated_clear_ms=estimated_clear,
        queue_duration_sample_count=len(queue_durations),
        execution_duration_sample_count=len(recent_execution_durations),
        execution_p50_ms=percentile(0.5),
        execution_p90_ms=percentile(0.9),
        throughput_per_hour=throughput_per_hour,
    )


def _visible_tasks(tasks: list[ModelQueueTask]) -> list[ModelQueueTask]:
    return [
        item for item in tasks if item.status in ACTIVE_STATUSES or item.status in FAILED_STATUSES
    ]


def _deduplicate_asset_research_tasks(
    tasks: list[ModelQueueTask],
    all_tasks: list[ModelQueueTask],
) -> list[ModelQueueTask]:
    """Keep one visible card per asset; sorted active tasks take precedence."""

    latest_terminal_by_asset: dict[str, ModelQueueTask] = {}
    for task in all_tasks:
        if (
            task.kind != "asset_research"
            or not task.entity_id
            or task.status not in FAILED_STATUSES | COMPLETED_STATUSES
        ):
            continue
        current = latest_terminal_by_asset.get(task.entity_id)
        if current is None or task.updated_at > current.updated_at:
            latest_terminal_by_asset[task.entity_id] = task

    seen_assets: set[str] = set()
    visible: list[ModelQueueTask] = []
    for task in tasks:
        if task.kind != "asset_research" or not task.entity_id:
            visible.append(task)
            continue
        if task.entity_id in seen_assets:
            continue
        latest_terminal = latest_terminal_by_asset.get(task.entity_id)
        if (
            task.status in FAILED_STATUSES
            and latest_terminal is not None
            and latest_terminal.status in COMPLETED_STATUSES
        ):
            continue
        seen_assets.add(task.entity_id)
        visible.append(task)
    return visible


def _state(counts: ModelQueueCounts, *, enabled: bool, observable: bool) -> str:
    if not enabled:
        return "disabled"
    if not observable:
        return "unavailable"
    if counts.running or counts.verifying:
        return "running"
    if counts.queued or counts.retrying or counts.waiting_for_model:
        return "queued"
    if counts.failed:
        return "failed"
    return "idle"


def _inference_values(status: dict[str, Any]) -> tuple[int, int, int, bool]:
    return (
        max(0, int(status.get("queued") or 0)),
        max(0, int(status.get("running") or 0)),
        max(0, int(status.get("available") or 0)),
        bool(status.get("observable", False)),
    )


def _queue_item(
    *,
    queue_id: Literal["extract", "research", "assist", "code"],
    model: str,
    purpose: str,
    binding: str,
    enabled: bool,
    threads: int,
    capacity: int,
    inference: dict[str, Any],
    tasks: list[ModelQueueTask],
    limit: int,
    generated_at: datetime,
    redis_client: Redis | None = None,
    broker_queued: int | None = None,
    count_overrides: dict[str, int] | None = None,
    metric_overrides: dict[str, int | None] | None = None,
    error: str | None = None,
) -> ModelQueueItem:
    waiting_for_model, running_slots, available, observable = _inference_values(inference)
    raw_instances = list(inference.get("instances") or [])
    instance_count = max(
        0,
        int(inference.get("instance_count") or len(raw_instances) or (1 if capacity else 0)),
    )
    per_instance_concurrency = max(
        0,
        int(
            inference.get("per_instance_concurrency")
            or (ceil(capacity / instance_count) if instance_count else 0)
        ),
    )
    instance_ids = [str(item.get("id")) for item in raw_instances if item.get("id")]
    normalized_tasks: list[ModelQueueTask] = []
    for task in tasks:
        instance_id = task.instance_id or (
            instance_assignment(
                queue_id,
                task.task_id,
                redis_client=redis_client,
            )
            if redis_client is not None
            else None
        )
        if instance_id not in instance_ids and instance_ids:
            instance_id = instance_ids[sum(task.task_id.encode("utf-8")) % len(instance_ids)]
        if redis_client is not None and instance_id and task.instance_id != instance_id:
            record_instance_assignment(
                queue_id,
                task.task_id,
                instance_id,
                status=task.status,
                redis_client=redis_client,
            )
        normalized_tasks.append(task.model_copy(update={"instance_id": instance_id}))
    tasks = normalized_tasks
    counts = _counts(tasks)
    if broker_queued is not None:
        counts.queued = max(counts.queued, max(0, broker_queued))
    counts.running = max(counts.running, running_slots)
    counts.waiting_for_model = waiting_for_model
    for field, value in (count_overrides or {}).items():
        if hasattr(counts, field):
            setattr(counts, field, max(0, int(value or 0)))
    metrics = _metrics(
        tasks,
        counts,
        capacity,
        generated_at=generated_at,
        use_throughput=queue_id == "research",
        use_recent_throughput=queue_id == "assist",
    )
    for field, value in (metric_overrides or {}).items():
        if hasattr(metrics, field):
            setattr(metrics, field, value)
    work = counts.queued + counts.retrying + counts.running + counts.verifying
    if (
        queue_id != "assist"
        and not metrics.throughput_per_hour
        and metrics.average_execution_duration_ms is not None
        and capacity > 0
        and work > 0
    ):
        metrics.estimated_clear_ms = ceil(work / capacity) * metrics.average_execution_duration_ms
    visible = _sort_tasks(_visible_tasks(tasks))
    if queue_id == "research":
        visible = _deduplicate_asset_research_tasks(visible, tasks)
    visible_task_ids = {task.task_id for task in visible}
    instance_queues: list[ModelQueueInstance] = []
    for raw_instance in raw_instances:
        instance_id = str(raw_instance.get("id") or "unknown")
        instance_tasks = [item for item in tasks if item.instance_id == instance_id]
        instance_counts = _counts(instance_tasks)
        instance_counts.running = max(
            instance_counts.running,
            max(0, int(raw_instance.get("running") or 0)),
        )
        instance_counts.waiting_for_model = max(0, int(raw_instance.get("queued") or 0))
        instance_capacity = max(0, int(raw_instance.get("capacity") or 0))
        instance_observable = bool(raw_instance.get("observable", observable))
        instance_metrics = _metrics(
            instance_tasks,
            instance_counts,
            instance_capacity,
            generated_at=generated_at,
            use_throughput=queue_id == "research",
            use_recent_throughput=queue_id == "assist",
        )
        instance_visible = _sort_tasks(_visible_tasks(instance_tasks))
        if queue_id == "research":
            instance_visible = [
                task for task in instance_visible if task.task_id in visible_task_ids
            ]
        instance_queues.append(
            ModelQueueInstance(
                id=instance_id,
                healthy=bool(raw_instance.get("healthy", False)),
                model_available=bool(raw_instance.get("model_available", False)),
                state=_state(
                    instance_counts,
                    enabled=enabled,
                    observable=(
                        instance_observable
                        and bool(raw_instance.get("healthy", False))
                        and bool(raw_instance.get("model_available", False))
                    ),
                ),
                capacity=instance_capacity,
                available=max(0, int(raw_instance.get("available") or 0)),
                observable=instance_observable,
                counts=instance_counts,
                metrics=instance_metrics,
                total_tasks=sum(
                    (
                        instance_counts.queued,
                        instance_counts.running,
                        instance_counts.retrying,
                        instance_counts.verifying,
                        instance_counts.completed,
                        instance_counts.failed,
                    )
                ),
                truncated=len(instance_visible) > limit,
                tasks=instance_visible[:limit],
            )
        )
    if instance_queues:
        count_fields = (
            "queued",
            "running",
            "retrying",
            "verifying",
            "completed",
            "failed",
            "waiting_for_model",
        )
        for field in count_fields:
            instance_total = sum(
                int(getattr(instance.counts, field)) for instance in instance_queues
            )
            aggregate_total = int(getattr(counts, field))
            if aggregate_total > instance_total:
                setattr(
                    instance_queues[0].counts,
                    field,
                    int(getattr(instance_queues[0].counts, field))
                    + aggregate_total
                    - instance_total,
                )
                instance_total = aggregate_total
            setattr(counts, field, instance_total)
        for instance in instance_queues:
            instance.metrics = _metrics(
                [item for item in tasks if item.instance_id == instance.id],
                instance.counts,
                instance.capacity,
                generated_at=generated_at,
                use_throughput=queue_id == "research",
                use_recent_throughput=queue_id == "assist",
            )
            instance.total_tasks = sum(
                (
                    instance.counts.queued,
                    instance.counts.running,
                    instance.counts.retrying,
                    instance.counts.verifying,
                    instance.counts.completed,
                    instance.counts.failed,
                )
            )
            instance.state = _state(
                instance.counts,
                enabled=enabled,
                observable=(instance.observable and instance.healthy and instance.model_available),
            )
        if queue_id == "assist":
            metrics = _metrics(
                tasks,
                counts,
                capacity,
                generated_at=generated_at,
                use_recent_throughput=True,
            )
    return ModelQueueItem(
        id=queue_id,
        model=model,
        purpose=purpose,
        binding=binding,
        enabled=enabled,
        state=_state(counts, enabled=enabled, observable=observable),
        threads=max(0, threads),
        capacity=max(0, capacity),
        available=available,
        instance_count=instance_count,
        per_instance_concurrency=per_instance_concurrency,
        observable=observable,
        instances=instance_queues,
        counts=counts,
        metrics=metrics,
        total_tasks=sum(
            (
                counts.queued,
                counts.running,
                counts.retrying,
                counts.verifying,
                counts.completed,
                counts.failed,
            )
        ),
        truncated=len(visible) > limit,
        tasks=visible[:limit],
        error=_safe_error(error),
    )


def _extraction_tasks(
    payload: dict[str, Any],
    now: datetime,
    redis_client: Redis | None = None,
) -> list[ModelQueueTask]:
    tasks: list[ModelQueueTask] = []
    for raw in payload.get("items") or []:
        queued_at = _parse_datetime(raw.get("queued_at")) or now
        started_at = _parse_datetime(raw.get("started_at"))
        completed_at = _parse_datetime(raw.get("completed_at"))
        updated_at = _parse_datetime(raw.get("updated_at")) or queued_at
        tasks.append(
            ModelQueueTask(
                task_id=str(raw.get("task_id") or raw.get("news_id") or "unknown"),
                instance_id=(
                    (
                        instance_assignment(
                            "extract",
                            str(raw.get("task_id") or raw.get("news_id") or "unknown"),
                            redis_client=redis_client,
                        )
                        if redis_client is not None
                        else None
                    )
                    or (str(raw["instance_id"]) if raw.get("instance_id") else None)
                ),
                kind="news_extraction",
                entity_id=str(raw.get("news_id")) if raw.get("news_id") else None,
                title=str(raw.get("title") or "未命名新闻"),
                subtitle=str(raw.get("source") or ""),
                source=str(raw.get("source")) if raw.get("source") else None,
                status=str(raw.get("status") or "queued"),
                attempt=max(0, int(raw.get("attempt") or 0)),
                queued_at=queued_at,
                started_at=started_at,
                completed_at=completed_at,
                updated_at=updated_at,
                queue_duration_ms=raw.get("queue_duration_ms"),
                execution_duration_ms=raw.get("execution_duration_ms"),
                error=_safe_error(raw.get("error")),
            )
        )
    return tasks


def _research_tasks(
    runs: list[ResearchRun],
    event_runs: list[EventResearchRun],
    events: list[NewsEvent],
    *,
    model: str,
    limit: int,
    now: datetime,
    live_run_ids: set[str] | None = None,
    redis_client: Redis | None = None,
) -> list[ModelQueueTask]:
    cutoff = now - MODEL_TASK_RETENTION
    active_runs = [
        run
        for run in runs
        if run.status in {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}
        and (run.status is RunStatus.QUEUED or live_run_ids is None or str(run.id) in live_run_ids)
    ]
    failed_runs = [
        run for run in runs if run.status is RunStatus.FAILED and as_utc(run.updated_at) >= cutoff
    ]
    terminal_runs = [
        run
        for run in runs
        if run.status in {RunStatus.COMPLETED, RunStatus.INSUFFICIENT_EVIDENCE}
        and as_utc(run.updated_at) >= cutoff
    ]
    queue = build_research_queue(active_runs, max(limit, len(active_runs)), model, now)

    def research_instance(run: ResearchRun | EventResearchRun) -> str | None:
        return (
            instance_assignment(
                "research",
                run.celery_task_id or str(run.id),
                redis_client=redis_client,
            )
            if redis_client is not None
            else None
        ) or run.model_instance_id

    tasks = [
        ModelQueueTask(
            task_id=f"asset:{item.asset_id}:{item.representative_queued_at.isoformat()}",
            instance_id=next(
                (
                    research_instance(run)
                    for run in active_runs
                    if run.asset.asset_id == item.asset_id and research_instance(run)
                ),
                None,
            ),
            kind="asset_research",
            entity_id=item.asset_id,
            title=f"{item.symbol} · {item.name}",
            subtitle=f"{item.market.value} · {item.asset_class.value}",
            status=item.status.value,
            task_count=item.task_count,
            queued_at=item.queued_at,
            started_at=item.started_at,
            completed_at=item.completed_at,
            updated_at=item.updated_at,
            queue_duration_ms=item.queue_duration_ms,
            execution_duration_ms=item.execution_duration_ms,
        )
        for item in queue.items
    ]
    for run in failed_runs:
        tasks.append(
            ModelQueueTask(
                task_id=str(run.id),
                instance_id=research_instance(run),
                kind="asset_research",
                entity_id=run.asset.asset_id,
                title=f"{run.asset.symbol} · {run.asset.name}",
                subtitle=f"{run.asset.market.value} · {run.asset.asset_class.value}",
                status="failed",
                attempt=run.retry_attempt + 1,
                queued_at=as_utc(run.created_at),
                started_at=as_utc(run.started_at) if run.started_at else None,
                completed_at=as_utc(run.completed_at) if run.completed_at else None,
                updated_at=as_utc(run.updated_at),
                queue_duration_ms=_duration_ms(
                    as_utc(run.created_at), as_utc(run.started_at) if run.started_at else None
                ),
                execution_duration_ms=_duration_ms(
                    as_utc(run.started_at) if run.started_at else None,
                    as_utc(run.completed_at or run.updated_at),
                ),
                error=_safe_error(run.error),
            )
        )
    for run in terminal_runs:
        completed_at = as_utc(run.completed_at or run.updated_at)
        tasks.append(
            ModelQueueTask(
                task_id=str(run.id),
                instance_id=research_instance(run),
                kind="asset_research",
                entity_id=run.asset.asset_id,
                title=f"{run.asset.symbol} · {run.asset.name}",
                subtitle=f"{run.asset.market.value} · {run.asset.asset_class.value}",
                status=run.status.value,
                attempt=run.retry_attempt + 1,
                queued_at=as_utc(run.created_at),
                started_at=as_utc(run.started_at) if run.started_at else None,
                completed_at=completed_at,
                updated_at=as_utc(run.updated_at),
                queue_duration_ms=_duration_ms(
                    as_utc(run.created_at),
                    as_utc(run.started_at) if run.started_at else None,
                ),
                execution_duration_ms=_duration_ms(
                    as_utc(run.started_at) if run.started_at else None,
                    completed_at,
                ),
            )
        )
    event_map = {str(event.id): event for event in events}
    for run in event_runs:
        if run.status not in {
            RunStatus.QUEUED,
            RunStatus.RUNNING,
            RunStatus.VERIFYING,
            RunStatus.COMPLETED,
            RunStatus.INSUFFICIENT_EVIDENCE,
            RunStatus.FAILED,
        }:
            continue
        if (
            run.status
            in {
                RunStatus.FAILED,
                RunStatus.COMPLETED,
                RunStatus.INSUFFICIENT_EVIDENCE,
            }
            and as_utc(run.updated_at) < cutoff
        ):
            continue
        if (
            run.status in {RunStatus.RUNNING, RunStatus.VERIFYING}
            and live_run_ids is not None
            and str(run.id) not in live_run_ids
        ):
            continue
        event = event_map.get(str(run.event_id))
        started_step = next(
            (step for step in run.analysis_steps if step.status in {"running", "verifying"}),
            None,
        )
        started_at = as_utc(started_step.occurred_at) if started_step else None
        tasks.append(
            ModelQueueTask(
                task_id=str(run.id),
                instance_id=research_instance(run),
                kind="event_research",
                entity_id=str(run.event_id),
                title=event.headline if event else f"事件 {run.event_id}",
                subtitle="逐目标事件研报",
                status=run.status.value,
                attempt=run.retry_count + 1,
                queued_at=as_utc(run.created_at),
                started_at=started_at,
                completed_at=(
                    as_utc(run.updated_at)
                    if run.status
                    in {
                        RunStatus.FAILED,
                        RunStatus.COMPLETED,
                        RunStatus.INSUFFICIENT_EVIDENCE,
                    }
                    else None
                ),
                updated_at=as_utc(run.updated_at),
                queue_duration_ms=_duration_ms(
                    as_utc(run.created_at),
                    started_at or (now if run.status is RunStatus.QUEUED else None),
                ),
                execution_duration_ms=_duration_ms(
                    started_at,
                    as_utc(run.updated_at)
                    if run.status
                    in {
                        RunStatus.FAILED,
                        RunStatus.COMPLETED,
                        RunStatus.INSUFFICIENT_EVIDENCE,
                    }
                    else (now if run.status in {RunStatus.RUNNING, RunStatus.VERIFYING} else None),
                ),
                error=_safe_error(run.error),
            )
        )
    return tasks


def build_model_queue_overview(
    db: Session,
    *,
    extraction_queue: dict[str, Any],
    inference_statuses: dict[str, dict[str, Any]],
    threads: dict[str, int],
    limit: int,
    settings: Settings | None = None,
    redis_client: Redis | None = None,
    generated_at: datetime | None = None,
) -> ModelQueueOverviewResponse:
    active_settings = settings or get_settings()
    now = as_utc(generated_at or utc_now())
    client = redis_client
    if client is None:
        try:
            client = _redis_client(active_settings)
            client.ping()
        except Exception:
            client = None

    source_errors: dict[str, str] = {}
    try:
        runs = list_runs(db, 5000)
    except Exception:
        runs = []
        source_errors["research"] = "标的研究任务状态暂时不可用。"
    try:
        event_runs = list_event_research_runs(db, 5000)
    except Exception:
        event_runs = []
        source_errors["event_research"] = "事件研报任务状态暂时不可用。"
    try:
        events = list_events(db, 5000)
    except Exception:
        events = []
        source_errors["events"] = "事件与股票映射状态暂时不可用。"
    try:
        candidates = list_evolutions(db)
    except Exception:
        candidates = []
        source_errors["code"] = "代码演进候选状态暂时不可用。"

    extract_records = (
        list_model_task_records("extract", now=now, redis_client=client)
        if client is not None
        else []
    )
    extract_lease_cutoff = now - timedelta(seconds=active_settings.model_task_lease_seconds)
    extract_records = [
        item
        for item in extract_records
        if item.status not in RUNNING_STATUSES or item.updated_at > extract_lease_cutoff
    ]
    extraction_tasks = _extraction_tasks(extraction_queue, now, redis_client=client)
    extract_tasks = _merge_tasks(
        extraction_tasks,
        extract_records,
    )
    research_live_ids = live_research_run_ids(client) if client is not None else None
    research_tasks = _research_tasks(
        runs,
        event_runs,
        events,
        model=active_settings.ollama_research_model,
        limit=limit,
        now=now,
        live_run_ids=research_live_ids,
        redis_client=client,
    )
    assist_records = (
        list_model_task_records("assist", now=now, redis_client=client)
        if client is not None
        else []
    )
    assist_tasks = _merge_tasks(assist_records, _event_mapping_tasks(events, now))
    code_records = (
        list_model_task_records("code", now=now, redis_client=client) if client is not None else []
    )
    code_tasks = _merge_tasks(
        code_records,
        _evolution_candidate_tasks(candidates, now, redis_client=client),
    )

    broker_mapping = None
    broker_code = None
    if client is not None:
        try:
            broker_mapping = int(client.llen("mapping"))
        except Exception:
            broker_mapping = None
        try:
            broker_code = int(client.llen("evolution"))
        except Exception:
            broker_code = None

    extract_inference = inference_statuses.get("extract", {})
    research_inference = inference_statuses.get("research", {})
    assist_inference = inference_statuses.get("assist", {})
    code_inference = inference_statuses.get("code", {})
    merged_extraction_counts = _counts(extract_tasks)
    extraction_counts = {
        key: max(
            int((extraction_queue.get("counts") or {}).get(key) or 0),
            int(getattr(merged_extraction_counts, key)),
        )
        for key in {"queued", "running", "retrying", "verifying", "completed", "failed"}
    }
    queues = [
        _queue_item(
            queue_id="extract",
            model=active_settings.ollama_extract_model,
            purpose="新闻抽取",
            binding="新闻事件结构化抽取",
            enabled=True,
            threads=threads.get("extract", 0),
            capacity=int(extract_inference.get("capacity") or 0),
            inference=extract_inference,
            tasks=extract_tasks,
            limit=min(limit, 200),
            generated_at=now,
            redis_client=client,
            broker_queued=(extraction_queue.get("counts") or {}).get("queued"),
            count_overrides=extraction_counts,
            metric_overrides={
                key: extraction_queue.get(key)
                for key in {
                    "average_queue_duration_ms",
                    "average_execution_duration_ms",
                    "queue_duration_sample_count",
                    "execution_duration_sample_count",
                }
            },
            error=extraction_queue.get("error"),
        ),
        _queue_item(
            queue_id="research",
            model=active_settings.ollama_research_model,
            purpose="标的研究",
            binding="工具深度研究与逐目标事件研报",
            enabled=True,
            threads=threads.get("research", 0),
            capacity=int(research_inference.get("capacity") or 0),
            inference=research_inference,
            tasks=research_tasks,
            limit=limit,
            generated_at=now,
            redis_client=client,
            error=" ".join(
                filter(
                    None,
                    (
                        source_errors.get("research"),
                        source_errors.get("event_research"),
                    ),
                )
            )
            or None,
        ),
        _queue_item(
            queue_id="assist",
            model=active_settings.ollama_assist_model,
            purpose="股票映射",
            binding="新闻事件二次股票映射",
            enabled=True,
            threads=threads.get("assist", 0),
            capacity=int(assist_inference.get("capacity") or 0),
            inference=assist_inference,
            tasks=assist_tasks,
            limit=limit,
            generated_at=now,
            redis_client=client,
            broker_queued=broker_mapping,
            error=" ".join(
                filter(
                    None,
                    (
                        source_errors.get("events"),
                        None if client is not None else "Redis 任务状态暂时不可用。",
                    ),
                )
            )
            or None,
        ),
        _queue_item(
            queue_id="code",
            model=active_settings.ollama_code_model,
            purpose="代码演进",
            binding=(
                "失败案例驱动的代码演进 · 自动合并开启"
                if active_settings.evolution_auto_merge
                else "失败案例驱动的代码演进 · 自动合并关闭"
            ),
            enabled=active_settings.evolution_enabled,
            threads=threads.get("code", 0),
            capacity=int(code_inference.get("capacity") or 0),
            inference=code_inference,
            tasks=code_tasks,
            limit=limit,
            generated_at=now,
            redis_client=client,
            broker_queued=broker_code,
            error=" ".join(
                filter(
                    None,
                    (
                        source_errors.get("code"),
                        None if client is not None else "Redis 任务状态暂时不可用。",
                    ),
                )
            )
            or None,
        ),
    ]
    return ModelQueueOverviewResponse(generated_at=now, queues=queues)
