from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from backend.app.config import Settings, get_settings


class LlmError(RuntimeError):
    pass


class GpuSemaphore:
    """One global inference slot. Redis coordinates API and Celery processes."""

    def __init__(self, settings: Settings) -> None:
        self._local = threading.Lock()
        self._redis = None
        try:
            from redis import Redis

            client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.2)
            client.ping()
            self._redis = client
        except Exception:
            self._redis = None

    @contextmanager
    def acquire(self, timeout: int = 300) -> Iterator[None]:
        if self._redis:
            lock = self._redis.lock(
                "market-loop:gpu", timeout=timeout + 30, blocking_timeout=timeout
            )
            acquired = lock.acquire(blocking=True)
            if not acquired:
                raise LlmError("timed out waiting for the GPU inference slot")
            try:
                yield
            finally:
                if lock.owned():
                    lock.release()
            return
        acquired = self._local.acquire(timeout=timeout)
        if not acquired:
            raise LlmError("timed out waiting for the local inference slot")
        try:
            yield
        finally:
            self._local.release()


class LlmGateway:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=self.settings.ollama_timeout_seconds)
        self.gpu = GpuSemaphore(self.settings)

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, LlmError, ValidationError)),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        stop=stop_after_attempt(2),
        reraise=True,
    )
    def generate_json(
        self,
        *,
        model: str,
        system: str,
        prompt: str,
        schema: type[BaseModel] | None = None,
        temperature: float = 0.1,
    ) -> dict[str, Any]:
        schema_hint = schema.model_json_schema() if schema else {"type": "object"}
        with self.gpu.acquire(self.settings.ollama_timeout_seconds + 30):
            response = self.client.post(
                f"{self.settings.ollama_base_url.rstrip('/')}/api/chat",
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system},
                        {
                            "role": "user",
                            "content": f"{prompt}\n\n只返回符合此 JSON Schema 的 JSON：{json.dumps(schema_hint, ensure_ascii=False)}",
                        },
                    ],
                    "format": schema_hint,
                    "stream": False,
                    "keep_alive": 0,
                    "options": {"temperature": temperature, "num_ctx": 8192},
                },
            )
            response.raise_for_status()
        content = response.json().get("message", {}).get("content", "")
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise LlmError("Ollama returned invalid JSON") from exc
        if schema:
            return schema.model_validate(payload).model_dump(mode="json")
        return payload

    def cloud_verify(self, prompt: str, schema: type[BaseModel]) -> dict[str, Any] | None:
        if not self.settings.cloud_verifier_enabled:
            return None
        response = self.client.post(
            f"{self.settings.cloud_llm_base_url.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {self.settings.cloud_llm_api_key}"},
            json={
                "model": self.settings.cloud_llm_model,
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "temperature": 0,
            },
        )
        response.raise_for_status()
        payload = json.loads(response.json()["choices"][0]["message"]["content"])
        return schema.model_validate(payload).model_dump(mode="json")


gateway = LlmGateway()
