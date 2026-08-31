from __future__ import annotations

import argparse
import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from redis import Redis
from sqlalchemy import text

from backend.app.config import get_settings
from backend.app.db import SessionLocal, init_db

EVOLUTION_TASKS = {
    "market_loop.evolve_from_outcomes",
    "market_loop.evolve_failures",
    "market_loop.execute_evolution",
}
ARCHIVE_PREFIX = "market-loop:archive:evolution-cutover:"


@dataclass(frozen=True)
class LegacyEvolutionMessage:
    queue_key: bytes
    task_id: str
    task_type: str
    candidate_id: str | None
    args: list[Any]
    kwargs: dict[str, Any]
    priority: int


def decode_legacy_message(queue_key: bytes, raw: bytes) -> LegacyEvolutionMessage:
    envelope = json.loads(raw)
    headers = envelope.get("headers") or {}
    task_type = str(headers.get("task") or "")
    if task_type not in EVOLUTION_TASKS:
        raise ValueError(f"unsupported evolution task: {task_type or 'missing'}")
    task_id = str(headers.get("id") or "")
    UUID(task_id)
    body = json.loads(base64.b64decode(envelope["body"]))
    args = body[0] if isinstance(body, list) and body else []
    kwargs = body[1] if isinstance(body, list) and len(body) > 1 else {}
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise ValueError(f"evolution task {task_id} has an invalid body")
    candidate_id = None
    if task_type == "market_loop.execute_evolution":
        if not args:
            raise ValueError(f"evolution task {task_id} has no candidate id")
        candidate_id = str(args[0])
        UUID(candidate_id)
    properties = envelope.get("properties") or {}
    raw_priority = properties.get("priority")
    return LegacyEvolutionMessage(
        queue_key=queue_key,
        task_id=task_id,
        task_type=task_type,
        candidate_id=candidate_id,
        args=args,
        kwargs=kwargs,
        priority=int(raw_priority if raw_priority is not None else 5),
    )


def discover_legacy_messages(
    client: Redis,
) -> tuple[list[bytes], list[LegacyEvolutionMessage]]:
    keys = sorted(
        key for key in client.scan_iter(match="evolution*") if client.type(key) == b"list"
    )
    messages = [
        decode_legacy_message(key, raw)
        for key in keys
        for raw in client.lrange(key, 0, -1)
    ]
    task_ids = [item.task_id for item in messages]
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("duplicate task ids found in legacy evolution queues")
    return keys, messages


def _archive_name(queue_key: bytes, stamp: str) -> bytes:
    digest = hashlib.sha256(queue_key).hexdigest()[:12]
    return f"{ARCHIVE_PREFIX}{stamp}:{digest}".encode()


def migrate_legacy_messages(client: Redis, *, apply: bool = False) -> dict[str, Any]:
    keys, messages = discover_legacy_messages(client)
    result: dict[str, Any] = {
        "requested": len(messages),
        "queued": 0,
        "existing": 0,
        "archived_queues": [],
        "applied": apply,
    }
    if not apply or not messages:
        return result

    init_db()
    with SessionLocal() as db:
        candidate_ids = sorted(
            {item.candidate_id for item in messages if item.candidate_id is not None}
        )
        if candidate_ids:
            known = set(
                db.execute(
                    text(
                        "SELECT id::text FROM evolution_candidates "
                        "WHERE id::text = ANY(CAST(:ids AS text[]))"
                    ),
                    {"ids": candidate_ids},
                ).scalars()
            )
            missing = sorted(set(candidate_ids) - known)
            if missing:
                raise RuntimeError(f"evolution candidates missing: {len(missing)}")

        for item in messages:
            payload = json.dumps(
                {"args": item.args, "kwargs": item.kwargs},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            dedupe_key = (
                f"evolution-candidate:{item.candidate_id}"
                if item.candidate_id
                else f"evolution-task:{item.task_id}"
            )
            inserted = db.execute(
                text(
                    """
                    INSERT INTO go_jobs(
                        id,queue,task_type,payload,status,priority,max_attempts,
                        available_at,dedupe_key,created_at,updated_at
                    ) VALUES(
                        :id,'code',:task_type,CAST(:payload AS jsonb),'queued',
                        :priority,1,now(),:dedupe_key,now(),now()
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """
                ),
                {
                    "id": item.task_id,
                    "task_type": item.task_type,
                    "payload": payload,
                    "priority": item.priority,
                    "dedupe_key": dedupe_key,
                },
            ).scalar_one_or_none()
            if inserted is None:
                existing = db.execute(
                    text(
                        """
                        SELECT id FROM go_jobs
                        WHERE id=:id OR (
                            queue='code' AND dedupe_key=:dedupe_key
                            AND status IN ('queued','running','retrying')
                        ) LIMIT 1
                        """
                    ),
                    {"id": item.task_id, "dedupe_key": dedupe_key},
                ).scalar_one_or_none()
                if existing is None:
                    raise RuntimeError(f"Go job was not persisted: {item.task_id}")
                result["existing"] += 1
            else:
                result["queued"] += 1
        db.commit()

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for key in keys:
        archive = _archive_name(key, stamp)
        if client.exists(archive):
            raise RuntimeError(f"archive key already exists: {archive.decode()}")
        if client.renamenx(key, archive):
            result["archived_queues"].append(
                {"source": key.decode("utf-8", "backslashreplace"), "archive": archive.decode()}
            )
        elif client.exists(key):
            raise RuntimeError(f"legacy queue could not be archived: {key!r}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move drained legacy evolution messages into durable Go jobs."
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    client = Redis.from_url(get_settings().redis_url)
    print(
        json.dumps(
            migrate_legacy_messages(client, apply=args.apply),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
