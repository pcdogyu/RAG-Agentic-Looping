from __future__ import annotations

import json
import threading
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import monotonic
from typing import Literal

import httpx
from redis import Redis

from backend.app.config import Settings, get_settings
from backend.app.domain import utc_now

ModelLane = Literal["extract", "assist", "research", "code"]
ACTIVE_ASSIGNMENT_STATUSES = {
    "queued",
    "running",
    "retrying",
    "verifying",
    "generating",
    "testing",
    "merging",
}
ASSIGNMENT_TTL_SECONDS = 48 * 60 * 60
_health_guard = threading.Lock()
_health_cache: dict[tuple[str, str], tuple[float, bool, bool]] = {}


@dataclass(frozen=True)
class ModelInstanceSpec:
    id: str
    lane: ModelLane
    url: str
    capacity: int


@dataclass
class ModelInstanceAffinity:
    lane: ModelLane
    instance_id: str
    task_id: str | None = None


_active_affinity: ContextVar[ModelInstanceAffinity | None] = ContextVar(
    "model_instance_affinity", default=None
)


def lane_model(settings: Settings, lane: ModelLane) -> str:
    return {
        "extract": settings.ollama_extract_model,
        "assist": settings.ollama_assist_model,
        "research": settings.ollama_research_model,
        "code": settings.ollama_code_model,
    }[lane]


def lane_urls(settings: Settings, lane: ModelLane) -> list[str]:
    return {
        "extract": settings.ollama_extract_urls,
        "assist": settings.ollama_assist_urls,
        "research": settings.ollama_research_urls,
        "code": settings.ollama_code_urls,
    }[lane]


def lane_capacity(settings: Settings, lane: ModelLane) -> int:
    return {
        "extract": settings.ollama_extract_max_concurrency,
        "assist": settings.ollama_assist_max_concurrency,
        "research": settings.ollama_research_max_concurrency,
        "code": settings.ollama_code_max_concurrency,
    }[lane]


def configured_model_instances(
    lane: ModelLane, settings: Settings | None = None
) -> list[ModelInstanceSpec]:
    active_settings = settings or get_settings()
    urls = lane_urls(active_settings, lane)
    total_capacity = lane_capacity(active_settings, lane)
    base, extra = divmod(total_capacity, len(urls))
    return [
        ModelInstanceSpec(
            id=f"{lane}-{index}",
            lane=lane,
            url=url,
            capacity=base + (1 if index < extra else 0),
        )
        for index, url in enumerate(urls)
    ]


def broker_queue_name(lane: ModelLane, instance_id: str) -> str:
    base = {
        "extract": "extract",
        "assist": "mapping",
        "research": "research",
        "code": "evolution",
    }[lane]
    return f"{base}.{instance_id}"


def worker_queue_names(lane: ModelLane, settings: Settings | None = None) -> str:
    legacy = {
        "extract": "extract",
        "assist": "mapping",
        "research": "research",
        "code": "evolution",
    }[lane]
    queues = [legacy]
    queues.extend(
        broker_queue_name(lane, instance.id)
        for instance in configured_model_instances(lane, settings)
    )
    return ",".join(queues)


def _assignment_key(lane: ModelLane) -> str:
    return f"market-loop:model-instance:{lane}:assignments"


def _redis_client(settings: Settings | None = None) -> Redis:
    return Redis.from_url(
        (settings or get_settings()).redis_url,
        socket_connect_timeout=1,
    )


def record_instance_assignment(
    lane: ModelLane,
    task_id: str,
    instance_id: str,
    *,
    status: str = "queued",
    redis_client: Redis | None = None,
) -> None:
    try:
        client = redis_client or _redis_client()
        key = _assignment_key(lane)
        client.hset(
            key,
            task_id,
            json.dumps(
                {
                    "task_id": task_id,
                    "instance_id": instance_id,
                    "status": status,
                    "updated_at": utc_now().isoformat(),
                }
            ),
        )
        client.expire(key, ASSIGNMENT_TTL_SECONDS)
    except Exception:
        return


def update_instance_assignment(
    lane: ModelLane,
    task_id: str,
    *,
    status: str,
    instance_id: str | None = None,
    redis_client: Redis | None = None,
) -> None:
    try:
        client = redis_client or _redis_client()
        key = _assignment_key(lane)
        raw = client.hget(key, task_id)
        payload = json.loads(raw) if raw else {"task_id": task_id}
        if instance_id is not None:
            payload["instance_id"] = instance_id
        if not payload.get("instance_id"):
            return
        payload.update(status=status, updated_at=utc_now().isoformat())
        client.hset(key, task_id, json.dumps(payload))
        client.expire(key, ASSIGNMENT_TTL_SECONDS)
    except Exception:
        return


def instance_assignment(
    lane: ModelLane,
    task_id: str,
    *,
    redis_client: Redis | None = None,
) -> str | None:
    try:
        client = redis_client or _redis_client()
        raw = client.hget(_assignment_key(lane), task_id)
        if not raw:
            return None
        return str(json.loads(raw).get("instance_id") or "") or None
    except Exception:
        return None


def assignment_loads(
    lane: ModelLane, *, redis_client: Redis | None = None
) -> Counter[str]:
    counts: Counter[str] = Counter()
    try:
        client = redis_client or _redis_client()
        for raw in client.hvals(_assignment_key(lane)):
            payload = json.loads(raw)
            if payload.get("status") in ACTIVE_ASSIGNMENT_STATUSES:
                counts[str(payload.get("instance_id") or "")] += 1
    except Exception:
        pass
    return counts


def instance_health(
    instance: ModelInstanceSpec,
    model: str,
    *,
    client: httpx.Client | None = None,
) -> tuple[bool, bool]:
    if client is not None and not hasattr(client, "get"):
        return True, True
    cache_key = (instance.url, model)
    now = monotonic()
    cached = _health_cache.get(cache_key)
    if cached is not None and now - cached[0] < 10:
        return cached[1], cached[2]
    healthy = False
    model_available = False
    try:
        if client is None:
            response = httpx.get(f"{instance.url}/api/tags", timeout=2)
        else:
            response = client.get(f"{instance.url}/api/tags", timeout=2)
        response.raise_for_status()
        names = {str(item.get("name")) for item in response.json().get("models", [])}
        healthy = True
        model_available = model in names
    except (httpx.HTTPError, ValueError, KeyError):
        pass
    with _health_guard:
        _health_cache[cache_key] = (now, healthy, model_available)
    return healthy, model_available


def mark_instance_unhealthy(instance: ModelInstanceSpec, model: str) -> None:
    with _health_guard:
        _health_cache[(instance.url, model)] = (monotonic(), False, False)


def select_model_instance(
    lane: ModelLane,
    *,
    task_id: str | None = None,
    preferred: str | None = None,
    settings: Settings | None = None,
    redis_client: Redis | None = None,
    probe_health: bool = False,
    excluded: set[str] | None = None,
    rebalance_preferred: bool = False,
) -> ModelInstanceSpec:
    active_settings = settings or get_settings()
    instances = [
        item
        for item in configured_model_instances(lane, active_settings)
        if item.id not in (excluded or set())
    ]
    if not instances:
        raise ValueError(f"no configured {lane} model instance")
    model = lane_model(active_settings, lane)
    healthy = [
        item
        for item in instances
        if not probe_health or all(instance_health(item, model))
    ]
    candidates = healthy or instances
    loads = assignment_loads(lane, redis_client=redis_client)
    minimum = min(loads[item.id] for item in candidates)
    if preferred:
        selected = next((item for item in candidates if item.id == preferred), None)
        preferred_is_balanced = (
            selected is not None
            and loads[selected.id] <= minimum + max(1, selected.capacity)
        )
        if selected is not None and (
            not rebalance_preferred or preferred_is_balanced
        ):
            if task_id:
                record_instance_assignment(
                    lane, task_id, selected.id, redis_client=redis_client
                )
            return selected
    tied = [item for item in candidates if loads[item.id] == minimum]
    offset = 0
    try:
        client = redis_client or _redis_client(active_settings)
        offset = int(client.incr(f"market-loop:model-instance:{lane}:round-robin")) - 1
    except Exception:
        if task_id:
            offset = sum(task_id.encode("utf-8"))
    selected = tied[offset % len(tied)]
    if task_id:
        record_instance_assignment(lane, task_id, selected.id, redis_client=redis_client)
    return selected


@contextmanager
def model_instance_affinity(
    lane: ModelLane, instance_id: str, *, task_id: str | None = None
) -> Iterator[ModelInstanceAffinity]:
    affinity = ModelInstanceAffinity(lane=lane, instance_id=instance_id, task_id=task_id)
    token = _active_affinity.set(affinity)
    try:
        yield affinity
    finally:
        _active_affinity.reset(token)


def current_model_instance(lane: ModelLane) -> ModelInstanceAffinity | None:
    affinity = _active_affinity.get()
    return affinity if affinity is not None and affinity.lane == lane else None


def reassign_current_model_instance(lane: ModelLane, instance_id: str) -> None:
    affinity = current_model_instance(lane)
    if affinity is None:
        return
    affinity.instance_id = instance_id
    if affinity.task_id:
        update_instance_assignment(
            lane,
            affinity.task_id,
            status="running",
            instance_id=instance_id,
        )


if __name__ == "__main__":
    import sys

    print(worker_queue_names(sys.argv[1]))
