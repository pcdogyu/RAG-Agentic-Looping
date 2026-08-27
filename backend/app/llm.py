from __future__ import annotations

import json
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from queue import Queue
from time import monotonic, sleep, time
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ValidationError

from backend.app.config import Settings, get_settings
from backend.app.domain import utc_now
from backend.app.model_audit import persist_model_audit
from backend.app.services.prompt_budget import QwenPromptBudget


class LlmError(RuntimeError):
    pass


class LlmResponseError(LlmError):
    """The model responded, but the structured payload was unusable."""


INFERENCE_LOCK_LEASE_SECONDS = 120
INFERENCE_LOCK_HEARTBEAT_SECONDS = 30
LlmLane = Literal["extract", "assist", "research", "code"]


def serialize_keep_alive(value: str) -> str | int:
    """Preserve duration strings while sending numeric Ollama values as numbers."""
    normalized = value.strip()
    try:
        return int(normalized)
    except ValueError:
        return normalized


class GpuSemaphore:
    """Model-aware inference slots coordinated across API and Celery processes."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._local_slots: dict[tuple[str, tuple[int, ...]], Queue[int]] = {}
        self._local_guard = threading.Lock()
        self._redis = None
        try:
            from redis import Redis

            client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.2)
            client.ping()
            self._redis = client
        except Exception:
            self._redis = None

    def _lane(self, model: str, lane: LlmLane | None = None) -> tuple[str, int]:
        if lane == "extract":
            return lane, self.settings.ollama_extract_max_concurrency
        if lane == "assist":
            return lane, self.settings.ollama_assist_max_concurrency
        if lane == "research":
            return lane, self.settings.ollama_research_max_concurrency
        if lane == "code":
            return lane, self.settings.ollama_code_max_concurrency
        if model == self.settings.ollama_extract_model:
            return "extract", self.settings.ollama_extract_max_concurrency
        if model == self.settings.ollama_assist_model:
            return "assist", self.settings.ollama_assist_max_concurrency
        if model == self.settings.ollama_research_model:
            return "research", self.settings.ollama_research_max_concurrency
        if model == self.settings.ollama_code_model:
            return "code", self.settings.ollama_code_max_concurrency
        lane = re.sub(r"[^a-z0-9]+", "-", model.casefold()).strip("-") or "unknown"
        return lane[:48], 1

    def capacity_for(self, model: str, lane: LlmLane | None = None) -> int:
        return self._lane(model, lane)[1]

    @staticmethod
    def _waiting_key(lane: str) -> str:
        return f"market-loop:llm:{lane}:waiting"

    def queue_status(
        self,
        model: str,
        available_slots: set[int] | None = None,
        *,
        lane: LlmLane | None = None,
    ) -> dict[str, int | str | bool]:
        """Return the cross-process waiting/running state for one model lane."""
        lane, configured_capacity = self._lane(model, lane)
        slots = tuple(
            sorted(
                available_slots
                if available_slots is not None
                else range(configured_capacity)
            )
        )
        capacity = len(slots)
        if self._redis is None:
            return {
                "lane": lane,
                "capacity": capacity,
                "queued": 0,
                "running": 0,
                "available": capacity,
                "observable": False,
            }
        try:
            waiting_key = self._waiting_key(lane)
            self._redis.zremrangebyscore(waiting_key, "-inf", time())
            queued = int(self._redis.zcard(waiting_key))
            running = sum(
                int(bool(self._redis.exists(f"market-loop:llm:{lane}:{slot}")))
                for slot in slots
            )
        except Exception:
            return {
                "lane": lane,
                "capacity": capacity,
                "queued": 0,
                "running": 0,
                "available": capacity,
                "observable": False,
            }
        return {
            "lane": lane,
            "capacity": capacity,
            "queued": queued,
            "running": running,
            "available": max(0, capacity - running),
            "observable": True,
        }

    def _local_slot_queue(self, lane: str, slots: tuple[int, ...]) -> Queue[int]:
        with self._local_guard:
            key = (lane, slots)
            queue = self._local_slots.get(key)
            if queue is None:
                queue = Queue(maxsize=len(slots))
                for slot in slots:
                    queue.put_nowait(slot)
                self._local_slots[key] = queue
            return queue

    @contextmanager
    def acquire(
        self,
        model: str,
        timeout: float = 300,
        available_slots: set[int] | None = None,
        *,
        lane: LlmLane | None = None,
    ) -> Iterator[int]:
        lane, capacity = self._lane(model, lane)
        slots = tuple(
            sorted(
                available_slots if available_slots is not None else range(capacity)
            )
        )
        if not slots:
            raise LlmError(f"no healthy {lane} inference endpoint")
        if self._redis:
            deadline = monotonic() + timeout
            lock = None
            waiter_id = uuid4().hex
            waiting_key = self._waiting_key(lane)
            try:
                self._redis.zadd(waiting_key, {waiter_id: time() + timeout + 30})
                while lock is None:
                    for slot in slots:
                        candidate = self._redis.lock(
                            f"market-loop:llm:{lane}:{slot}",
                            timeout=INFERENCE_LOCK_LEASE_SECONDS,
                            blocking_timeout=0,
                            thread_local=False,
                        )
                        if candidate.acquire(blocking=False):
                            lock = candidate
                            break
                    if lock is not None:
                        break
                    if monotonic() >= deadline:
                        raise LlmError(f"timed out waiting for the {lane} inference slot")
                    sleep(min(0.1, max(0.0, deadline - monotonic())))
                self._redis.zrem(waiting_key, waiter_id)
                heartbeat_stop = threading.Event()

                def renew_lock() -> None:
                    while not heartbeat_stop.wait(INFERENCE_LOCK_HEARTBEAT_SECONDS):
                        try:
                            lock.extend(INFERENCE_LOCK_LEASE_SECONDS, replace_ttl=True)
                        except Exception:
                            break

                heartbeat = threading.Thread(
                    target=renew_lock,
                    name=f"{lane}-{slot}-lease",
                    daemon=True,
                )
                heartbeat.start()
                try:
                    yield slot
                finally:
                    heartbeat_stop.set()
                    heartbeat.join(timeout=1)
                    try:
                        if lock.owned():
                            lock.release()
                    except Exception:
                        pass
            finally:
                self._redis.zrem(waiting_key, waiter_id)
            return
        slot_queue = self._local_slot_queue(lane, slots)
        try:
            slot = slot_queue.get(timeout=timeout)
        except Exception as exc:
            raise LlmError(
                f"timed out waiting for the local {lane} inference slot"
            ) from exc
        try:
            yield slot
        finally:
            slot_queue.put_nowait(slot)


class LlmGateway:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=self.settings.ollama_timeout_seconds)
        self.gpu = GpuSemaphore(self.settings)
        self.prompt_budget = QwenPromptBudget(self.settings)
        self._health_guard = threading.Lock()
        self._endpoint_health: dict[str, tuple[float, bool, bool]] = {}

    def _model_urls(self, lane: LlmLane) -> list[str]:
        if lane == "assist":
            return [self.settings.ollama_assist_url]
        if lane == "research":
            return self.settings.ollama_research_urls
        return [self.settings.ollama_base_url.rstrip("/")]

    def _endpoint_status(self, lane: LlmLane, model: str) -> list[dict[str, Any]]:
        if lane not in {"assist", "research"}:
            return []
        if lane == "assist" and not self.settings.ollama_assist_base_url:
            return []
        if lane == "research" and not self.settings.ollama_research_base_urls:
            return []
        capacity = self.gpu.capacity_for(model, lane)
        urls = self._model_urls(lane)[:capacity]
        now = monotonic()
        result: list[dict[str, Any]] = []
        for index, url in enumerate(urls):
            cached = self._endpoint_health.get(url)
            if cached is None or now - cached[0] >= 10:
                healthy = False
                model_available = False
                try:
                    response = self.client.get(f"{url}/api/tags", timeout=2)
                    response.raise_for_status()
                    names = {str(item.get("name")) for item in response.json().get("models", [])}
                    healthy = True
                    model_available = model in names
                except (httpx.HTTPError, ValueError, KeyError):
                    pass
                with self._health_guard:
                    self._endpoint_health[url] = (now, healthy, model_available)
            else:
                _, healthy, model_available = cached
            result.append(
                {
                    "id": "assist-0" if lane == "assist" else f"research-{index}",
                    "healthy": healthy,
                    "model_available": model_available,
                }
            )
        return result

    def queue_status(self, model: str, *, lane: LlmLane | None = None) -> dict[str, Any]:
        resolved_lane = self._resolve_lane(model, lane)
        instances = self._endpoint_status(resolved_lane, model)
        configured_capacity = self.gpu.capacity_for(model, resolved_lane)
        instance_count = len(instances) or 1
        per_instance_concurrency = (
            (configured_capacity + instance_count - 1) // instance_count
            if configured_capacity > 0
            else 0
        )
        available_slots = (
            {
                index
                for index, instance in enumerate(instances)
                if instance["healthy"] and instance["model_available"]
            }
            if instances
            else None
        )
        status = (
            self.gpu.queue_status(model, available_slots, lane=resolved_lane)
            if available_slots is not None
            else self.gpu.queue_status(model, lane=resolved_lane)
        )
        return {
            **status,
            "instances": instances,
            "instance_count": instance_count,
            "per_instance_concurrency": per_instance_concurrency,
        }

    def _resolve_lane(self, model: str, lane: LlmLane | None = None) -> LlmLane:
        if lane is not None:
            return lane
        if model == self.settings.ollama_extract_model:
            return "extract"
        if model == self.settings.ollama_code_model:
            return "code"
        if model == self.settings.ollama_assist_model:
            return "assist"
        if model == self.settings.ollama_research_model:
            return "research"
        raise LlmError(f"explicit inference lane required for model {model}")

    def _num_threads_for(self, lane: LlmLane | str) -> int:
        if lane not in {"extract", "assist", "research", "code"}:
            lane = self._resolve_lane(lane)
        if lane == "extract":
            return self.settings.ollama_extract_num_threads or self.settings.ollama_num_threads
        if lane == "assist":
            return self.settings.ollama_assist_num_threads or self.settings.ollama_num_threads
        if lane == "research":
            return self.settings.ollama_research_num_threads or self.settings.ollama_num_threads
        if lane == "code":
            return self.settings.ollama_code_num_threads or self.settings.ollama_num_threads
        return self.settings.ollama_num_threads

    def num_threads_for(self, model: str, *, lane: LlmLane | None = None) -> int:
        return self._num_threads_for(self._resolve_lane(model, lane))

    def _max_output_tokens_for(self, lane: LlmLane) -> int:
        if lane == "assist":
            return self.settings.ollama_assist_max_output_tokens
        if lane == "research":
            return self.settings.ollama_research_max_output_tokens
        return self.settings.ollama_max_output_tokens

    def _context_length_for(self, lane: LlmLane) -> int:
        if lane == "assist":
            return self.settings.ollama_assist_context_length
        if lane == "research":
            return self.settings.ollama_research_context_length
        return self.settings.ollama_context_length

    def _timeout_for(self, lane: LlmLane, attempt: int) -> int:
        if lane == "research":
            if attempt > 1:
                return self.settings.ollama_research_validation_retry_timeout_seconds
            return self.settings.ollama_research_timeout_seconds
        return self.settings.ollama_timeout_seconds

    def generate_json(
        self,
        *,
        model: str,
        lane: LlmLane | None = None,
        system: str,
        prompt: str,
        schema: type[BaseModel] | None = None,
        temperature: float = 0.1,
        operation: str = "generate_json",
        entity_type: str | None = None,
        entity_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        resolved_lane = self._resolve_lane(model, lane)
        schema_hint = schema.model_json_schema() if schema else {"type": "object"}
        if resolved_lane in {"assist", "research"}:
            try:
                messages, estimated_prompt_tokens = self.prompt_budget.fit(
                    system=system,
                    prompt=prompt,
                    schema_payload=schema_hint,
                    max_tokens=self.settings.ollama_7b_max_input_tokens,
                )
            except Exception as exc:
                raise LlmError(f"7B prompt budget enforcement failed: {exc}") from exc
        else:
            messages = [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": f"{prompt}\n\n只返回符合请求中 format JSON Schema 的 JSON。",
                },
            ]
            estimated_prompt_tokens = None
        logical_call_id = uuid4()
        for attempt in (1, 2):
            started_at = utc_now()
            content = ""
            parsed: dict[str, Any] | list[Any] | None = None
            response_data: dict[str, Any] = {}
            endpoint_id: str | None = None
            try:
                options: dict[str, int | float] = {
                    "temperature": temperature,
                    "num_ctx": self._context_length_for(resolved_lane),
                    "num_predict": self._max_output_tokens_for(resolved_lane),
                }
                num_threads = self._num_threads_for(resolved_lane)
                if num_threads:
                    options["num_thread"] = num_threads
                model_urls = self._model_urls(resolved_lane)
                instances = self._endpoint_status(resolved_lane, model)
                available_slots = (
                    {
                        index
                        for index, instance in enumerate(instances)
                        if instance["healthy"] and instance["model_available"]
                    }
                    if instances
                    else None
                )
                request_timeout = self._timeout_for(resolved_lane, attempt)
                with self.gpu.acquire(
                    model,
                    request_timeout + 30,
                    available_slots,
                    lane=resolved_lane,
                ) as slot:
                    slot_index = int(slot or 0)
                    endpoint_id = (
                        "assist-0"
                        if resolved_lane == "assist"
                        else f"research-{slot_index}"
                        if resolved_lane == "research"
                        else "main"
                    )
                    response = self.client.post(
                        f"{model_urls[slot_index % len(model_urls)]}/api/chat",
                        json={
                            "model": model,
                            "messages": messages,
                            "format": schema_hint,
                            "stream": False,
                            "keep_alive": serialize_keep_alive(
                                self.settings.ollama_keep_alive
                            ),
                            "options": options,
                        },
                        timeout=request_timeout,
                    )
                    response.raise_for_status()
                response_data = response.json()
                actual_prompt_tokens = response_data.get("prompt_eval_count")
                if (
                    resolved_lane in {"assist", "research"}
                    and isinstance(actual_prompt_tokens, int)
                    and actual_prompt_tokens > self.settings.ollama_7b_max_input_tokens
                ):
                    raise LlmError(
                        f"Ollama evaluated {actual_prompt_tokens} prompt tokens; "
                        f"limit is {self.settings.ollama_7b_max_input_tokens}"
                    )
                content = response_data.get("message", {}).get("content", "")
                try:
                    parsed = json.loads(content)
                except json.JSONDecodeError as exc:
                    raise LlmResponseError("Ollama returned invalid JSON") from exc
                payload = parsed
                if schema:
                    payload = schema.model_validate(payload).model_dump(mode="json")
                completed_at = utc_now()
                persist_model_audit(
                    logical_call_id=logical_call_id,
                    provider="ollama",
                    model=model,
                    operation=operation,
                    attempt=attempt,
                    status="completed",
                    started_at=started_at,
                    completed_at=completed_at,
                    messages=messages,
                    schema_payload=schema_hint,
                    raw_response=content,
                    parsed_response=payload,
                    prompt_tokens=response_data.get("prompt_eval_count"),
                    completion_tokens=response_data.get("eval_count"),
                    metrics={
                        **{
                            key: response_data[key]
                            for key in (
                                "total_duration",
                                "load_duration",
                                "prompt_eval_duration",
                                "eval_duration",
                            )
                            if key in response_data
                        },
                        "endpoint": endpoint_id,
                        "lane": resolved_lane,
                        "estimated_prompt_tokens": estimated_prompt_tokens,
                    },
                    entity_type=entity_type,
                    entity_id=str(entity_id) if entity_id is not None else None,
                    settings=self.settings,
                )
                return payload
            except (httpx.HTTPError, LlmError, ValidationError) as exc:
                persist_model_audit(
                    logical_call_id=logical_call_id,
                    provider="ollama",
                    model=model,
                    operation=operation,
                    attempt=attempt,
                    status="failed",
                    started_at=started_at,
                    completed_at=utc_now(),
                    messages=messages,
                    schema_payload=schema_hint,
                    raw_response=content,
                    parsed_response=parsed,
                    error=f"{type(exc).__name__}: {exc}",
                    prompt_tokens=response_data.get("prompt_eval_count"),
                    completion_tokens=response_data.get("eval_count"),
                    metrics={
                        "endpoint": endpoint_id,
                        "lane": resolved_lane,
                        "estimated_prompt_tokens": estimated_prompt_tokens,
                    },
                    entity_type=entity_type,
                    entity_id=str(entity_id) if entity_id is not None else None,
                    settings=self.settings,
                )
                should_retry = attempt == 1 and isinstance(
                    exc, (LlmResponseError, ValidationError)
                )
                if not should_retry:
                    raise
                sleep(0.5)
        raise LlmError("unreachable model retry state")

    def cloud_verify(self, prompt: str, schema: type[BaseModel]) -> dict[str, Any] | None:
        if not self.settings.cloud_verifier_enabled:
            return None
        logical_call_id = uuid4()
        started_at = utc_now()
        messages = [{"role": "user", "content": prompt}]
        content = ""
        parsed: dict[str, Any] | list[Any] | None = None
        try:
            response = self.client.post(
                f"{self.settings.cloud_llm_base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {self.settings.cloud_llm_api_key}"},
                json={
                    "model": self.settings.cloud_llm_model,
                    "messages": messages,
                    "response_format": {"type": "json_object"},
                    "temperature": 0,
                },
            )
            response.raise_for_status()
            response_data = response.json()
            content = response_data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            payload = schema.model_validate(parsed).model_dump(mode="json")
            usage = response_data.get("usage", {})
            persist_model_audit(
                logical_call_id=logical_call_id,
                provider="openai-compatible",
                model=self.settings.cloud_llm_model,
                operation="cloud_verification",
                attempt=1,
                status="completed",
                started_at=started_at,
                completed_at=utc_now(),
                messages=messages,
                schema_payload=schema.model_json_schema(),
                raw_response=content,
                parsed_response=payload,
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
                settings=self.settings,
            )
            return payload
        except (httpx.HTTPError, KeyError, json.JSONDecodeError, ValidationError) as exc:
            persist_model_audit(
                logical_call_id=logical_call_id,
                provider="openai-compatible",
                model=self.settings.cloud_llm_model,
                operation="cloud_verification",
                attempt=1,
                status="failed",
                started_at=started_at,
                completed_at=utc_now(),
                messages=messages,
                schema_payload=schema.model_json_schema(),
                raw_response=content,
                parsed_response=parsed,
                error=f"{type(exc).__name__}: {exc}",
                settings=self.settings,
            )
            raise


gateway = LlmGateway()
