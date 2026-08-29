from __future__ import annotations

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml-adapter")
_lock = Lock()
_tokenizer: Any = None
_embedding: Any = None


def tokenizer() -> Any:
    global _tokenizer
    with _lock:
        if _tokenizer is None:
            from transformers import AutoTokenizer

            _tokenizer = AutoTokenizer.from_pretrained(
                os.getenv("TOKENIZER_MODEL", "Qwen/Qwen2.5-7B-Instruct"),
                revision=os.getenv("TOKENIZER_REVISION", "a09a35458c702b33eeacc393d103063234e8bc28"),
            )
    return _tokenizer


def embedding() -> Any:
    global _embedding
    with _lock:
        if _embedding is None:
            from fastembed import TextEmbedding

            _embedding = TextEmbedding(
                model_name=os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-small"),
                threads=int(os.getenv("EMBEDDING_THREADS", "2")),
            )
    return _embedding


def texts(payload: dict[str, Any]) -> list[str]:
    values = payload.get("texts")
    if not isinstance(values, list) or not values or len(values) > 128:
        raise ValueError("texts must contain between 1 and 128 strings")
    if not all(isinstance(value, str) and len(value) <= 100_000 for value in values):
        raise ValueError("each text must be a string with at most 100000 characters")
    return values


class Handler(BaseHTTPRequestHandler):
    server_version = "ml-adapter/1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._json(200, {"status": "ok", "service": "ml-adapter"})
            return
        self._json(404, {"detail": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/v1/embed", "/v1/token-count"}:
            self._json(404, {"detail": "not found"})
            return
        try:
            size = int(self.headers.get("Content-Length") or 0)
            if size > 10_000_000:
                raise ValueError("request is too large")
            payload = json.loads(self.rfile.read(size) or b"{}")
            values = texts(payload)
            if self.path == "/v1/token-count":
                model = tokenizer()
                self._json(200, {"counts": [len(model.encode(value, add_special_tokens=False)) for value in values]})
            else:
                vectors = [vector.tolist() for vector in embedding().embed(values)]
                self._json(200, {"vectors": vectors, "dimensions": len(vectors[0]) if vectors else 0})
        except ValueError as exc:
            self._json(422, {"detail": str(exc)})
        except Exception as exc:  # pragma: no cover - model loading is integration-tested
            logger.exception("adapter request failed")
            self._json(502, {"detail": f"{type(exc).__name__}: ML adapter failed"})

    def log_message(self, format: str, *args: Any) -> None:
        logger.info(format, *args)

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(
        (os.getenv("ADAPTER_ADDRESS", "0.0.0.0"), int(os.getenv("ADAPTER_PORT", "8092"))),
        Handler,
    ).serve_forever()
