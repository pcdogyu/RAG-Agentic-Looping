from __future__ import annotations

import math
import re
from collections import Counter
from hashlib import blake2b
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.db import DocumentChunkRow
from backend.app.domain import Evidence, as_utc

TOKEN_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*|[\u3400-\u9fff]", re.IGNORECASE)


class EmbeddingBackend(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class CpuEmbeddingBackend:
    """Lazy CPU embeddings with a deterministic, offline-safe fallback."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._model = None
        self._model_failed = False

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not self._model_failed and self._model is None:
            try:
                from fastembed import TextEmbedding

                self._model = TextEmbedding(model_name=self.settings.embedding_model, threads=2)
            except Exception:
                self._model_failed = True
        if self._model is not None:
            try:
                return [self._fit(vector.tolist()) for vector in self._model.embed(texts)]
            except Exception:
                self._model_failed = True
                self._model = None
        return [self._hashed_embedding(text) for text in texts]

    def _fit(self, vector: list[float]) -> list[float]:
        size = self.settings.embedding_dimensions
        return (vector + [0.0] * size)[:size]

    def _hashed_embedding(self, text: str) -> list[float]:
        vector = [0.0] * self.settings.embedding_dimensions
        for token in tokenize(text):
            digest = blake2b(token.encode("utf-8"), digest_size=8).digest()
            index = int.from_bytes(digest[:4], "big") % len(vector)
            vector[index] += 1.0 if digest[4] & 1 else -1.0
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]


def tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def cosine(left: list[float], right: list[float]) -> float:
    numerator = sum(a * b for a, b in zip(left, right, strict=False))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    return numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0


class RetrievalService:
    """Metadata-filtered BM25/vector retrieval combined with reciprocal-rank fusion."""

    def __init__(
        self,
        db: Session,
        settings: Settings | None = None,
        embeddings: EmbeddingBackend | None = None,
    ) -> None:
        self.db = db
        self.settings = settings or get_settings()
        self.embeddings = embeddings or CpuEmbeddingBackend(self.settings)

    def index(self, asset_id: str, evidence: list[Evidence]) -> None:
        existing = set(
            self.db.scalars(
                select(DocumentChunkRow.evidence_id).where(
                    DocumentChunkRow.evidence_id.in_([item.id for item in evidence])
                )
            )
        )
        pending = [item for item in evidence if item.id not in existing]
        if not pending:
            return
        texts = [f"{item.claim}\n{item.excerpt}" for item in pending]
        vectors = self.embeddings.embed(texts)
        for item, text, vector in zip(pending, texts, vectors, strict=True):
            self.db.add(
                DocumentChunkRow(
                    evidence_id=item.id,
                    run_id=item.run_id,
                    asset_id=asset_id,
                    text=text,
                    terms=tokenize(text),
                    embedding=vector,
                    source_url=item.source_url,
                    source_quality=item.source_quality.value,
                    published_at=item.published_at,
                    observed_at=item.observed_at,
                    as_of=item.as_of,
                )
            )
        self.db.commit()

    def search(self, query: str, *, asset_id: str, as_of, limit: int = 12) -> list[dict]:
        rows = list(
            self.db.scalars(
                select(DocumentChunkRow).where(DocumentChunkRow.asset_id == asset_id)
            )
        )
        boundary = as_utc(as_of)
        rows = [
            row
            for row in rows
            if as_utc(row.published_at) <= boundary
            and as_utc(row.observed_at) <= boundary
            and as_utc(row.as_of) <= boundary
        ]
        if not rows:
            return []

        query_terms = tokenize(query)
        query_vector = self.embeddings.embed([query])[0]
        document_frequency = Counter(
            term for row in rows for term in set(row.terms or tokenize(row.text))
        )
        average_length = sum(len(row.terms or []) for row in rows) / len(rows) or 1.0

        def bm25(row: DocumentChunkRow) -> float:
            terms = row.terms or tokenize(row.text)
            counts = Counter(terms)
            score = 0.0
            for term in query_terms:
                frequency = counts[term]
                if not frequency:
                    continue
                df = document_frequency[term]
                inverse = math.log(1 + (len(rows) - df + 0.5) / (df + 0.5))
                denominator = frequency + 1.2 * (0.25 + 0.75 * len(terms) / average_length)
                score += inverse * frequency * 2.2 / denominator
            return score

        lexical = sorted(rows, key=bm25, reverse=True)
        semantic = sorted(rows, key=lambda row: cosine(query_vector, list(row.embedding)), reverse=True)
        fused: dict[str, float] = Counter()
        for ranking in (lexical, semantic):
            for rank, row in enumerate(ranking, start=1):
                fused[str(row.id)] += 1 / (60 + rank)
        by_id = {str(row.id): row for row in rows}
        ranked = sorted(fused, key=fused.get, reverse=True)[:limit]
        return [
            {
                "evidence_id": str(by_id[row_id].evidence_id),
                "text": by_id[row_id].text,
                "source_url": by_id[row_id].source_url,
                "source_quality": by_id[row_id].source_quality,
                "score": fused[row_id],
            }
            for row_id in ranked
        ]
