from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Callable
from typing import Any, TypeVar

from backend.app.config import get_settings

T = TypeVar("T")


class ResponseCache:
    def __init__(self) -> None:
        self._local: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._redis = None
        try:
            from redis import Redis

            client = Redis.from_url(get_settings().redis_url, socket_connect_timeout=0.2)
            client.ping()
            self._redis = client
        except Exception:
            self._redis = None

    @staticmethod
    def key(namespace: str, value: Any) -> str:
        encoded = json.dumps(value, sort_keys=True, default=str).encode()
        return f"market-loop:{namespace}:{hashlib.sha256(encoded).hexdigest()}"

    def get(self, key: str) -> Any | None:
        if self._redis:
            value = self._redis.get(key)
            return json.loads(value) if value else None
        with self._lock:
            entry = self._local.get(key)
            if not entry or entry[0] <= time.time():
                self._local.pop(key, None)
                return None
            return entry[1]

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if self._redis:
            self._redis.setex(key, ttl_seconds, json.dumps(value, default=str))
            return
        with self._lock:
            self._local[key] = (time.time() + ttl_seconds, value)

    def remember(self, key: str, ttl_seconds: int, loader: Callable[[], T]) -> T:
        cached = self.get(key)
        if cached is not None:
            return cached
        value = loader()
        self.set(key, value, ttl_seconds)
        return value


cache = ResponseCache()
