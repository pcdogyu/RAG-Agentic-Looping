from __future__ import annotations

import json
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from time import monotonic, sleep, time
from typing import Any
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ValidationError

from backend.app.config import Settings, get_settings
from backend.app.domain import utc_now
from backend.app.model_audit import persist_model_audit


class LlmError(RuntimeError):
    pass


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
        self._local: dict[str, threading.BoundedSemaphore] = {}
        self._local_guard = threading.Lock()
        self._redis = None
        try:
            from redis import Redis

            client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.2)
            client.ping()
            self._redis = client
        except Exception:
            self._redis = None

    def _lane(self, model: str) -> tuple[str, int]:
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

    def capacity_for(self, model: str) -> int:
        return self._lane(model)[1]

    @staticmethod
    def _waiting_key(lane: str) -> str:
        return f"market-loop:llm:{lane}:waiting"

    def queue_status(self, model: str) -> dict[str, int | str | bool]:
        """Return the cross-process waiting/running state for one model lane."""
        lane, capacity = self._lane(model)
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
                for slot in range(capacity)
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

    def _local_semaphore(self, lane: str, capacity: int) -> threading.BoundedSemaphore:
        with self._local_guard:
            return self._local.setdefault(lane, threading.BoundedSemaphore(capacity))

    @contextmanager
    def acquire(self, model: str, timeout: float = 300) -> Iterator[None]:
        lane, capacity = self._lane(model)
        if self._redis:
            deadline = monotonic() + timeout
            lock = None
            waiter_id = uuid4().hex
            waiting_key = self._waiting_key(lane)
            try:
                self._redis.zadd(waiting_key, {waiter_id: time() + timeout + 30})
                while lock is None:
                    for slot in range(capacity):
                        candidate = self._redis.lock(
                            f"market-loop:llm:{lane}:{slot}",
                            timeout=timeout + 30,
                            blocking_timeout=0,
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
                try:
                    yield
                finally:
                    if lock.owned():
                        lock.release()
            finally:
                self._redis.zrem(waiting_key, waiter_id)
            return
        semaphore = self._local_semaphore(lane, capacity)
        acquired = semaphore.acquire(timeout=timeout)
        if not acquired:
            raise LlmError(f"timed out waiting for the local {lane} inference slot")
        try:
            yield
        finally:
            semaphore.release()


class LlmGateway:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=self.settings.ollama_timeout_seconds)
        self.gpu = GpuSemaphore(self.settings)

    def _num_threads_for(self, model: str) -> int:
        if model == self.settings.ollama_extract_model:
            return self.settings.ollama_extract_num_threads or self.settings.ollama_num_threads
        if model == self.settings.ollama_assist_model:
            return self.settings.ollama_assist_num_threads or self.settings.ollama_num_threads
        if model == self.settings.ollama_research_model:
            return self.settings.ollama_research_num_threads or self.settings.ollama_num_threads
        if model == self.settings.ollama_code_model:
            return self.settings.ollama_code_num_threads or self.settings.ollama_num_threads
        return self.settings.ollama_num_threads

    def num_threads_for(self, model: str) -> int:
        return self._num_threads_for(model)

    def generate_json(
        self,
        *,
        model: str,
        system: str,
        prompt: str,
        schema: type[BaseModel] | None = None,
        temperature: float = 0.1,
        operation: str = "generate_json",
        entity_type: str | None = None,
        entity_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        schema_hint = schema.model_json_schema() if schema else {"type": "object"}
        messages = [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": f"{prompt}\n\n只返回符合请求中 format JSON Schema 的 JSON。",
            },
        ]
        logical_call_id = uuid4()
        retryable = (httpx.HTTPError, LlmError, ValidationError)
        for attempt in (1, 2):
            started_at = utc_now()
            content = ""
            parsed: dict[str, Any] | list[Any] | None = None
            response_data: dict[str, Any] = {}
            try:
                options: dict[str, int | float] = {
                    "temperature": temperature,
                    "num_ctx": 8192,
                    "num_predict": self.settings.ollama_max_output_tokens,
                }
                num_threads = self._num_threads_for(model)
                if num_threads:
                    options["num_thread"] = num_threads
                with self.gpu.acquire(model, self.settings.ollama_timeout_seconds + 30):
                    response = self.client.post(
                        f"{self.settings.ollama_base_url.rstrip('/')}/api/chat",
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
                    )
                    response.raise_for_status()
                response_data = response.json()
                content = response_data.get("message", {}).get("content", "")
                try:
                    parsed = json.loads(content)
                except json.JSONDecodeError as exc:
                    raise LlmError("Ollama returned invalid JSON") from exc
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
                        key: response_data[key]
                        for key in (
                            "total_duration",
                            "load_duration",
                            "prompt_eval_duration",
                            "eval_duration",
                        )
                        if key in response_data
                    },
                    entity_type=entity_type,
                    entity_id=str(entity_id) if entity_id is not None else None,
                    settings=self.settings,
                )
                return payload
            except retryable as exc:
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
                    entity_type=entity_type,
                    entity_id=str(entity_id) if entity_id is not None else None,
                    settings=self.settings,
                )
                if attempt == 2:
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
