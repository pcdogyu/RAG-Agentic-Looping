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

RESEARCH_TASKS = {
    "market_loop.research_event": "event",
    "market_loop.research_asset": "asset",
}
ARCHIVE_PREFIX = "market-loop:archive:research-cutover:"


@dataclass(frozen=True)
class LegacyResearchMessage:
    queue_key: bytes
    task_id: str
    task_type: str
    run_id: str
    args: list[Any]
    kwargs: dict[str, Any]
    priority: int


def decode_legacy_message(queue_key: bytes, raw: bytes) -> LegacyResearchMessage:
    envelope = json.loads(raw)
    headers = envelope.get("headers") or {}
    task_type = str(headers.get("task") or "")
    if task_type not in RESEARCH_TASKS:
        raise ValueError(f"unsupported research task: {task_type or 'missing'}")
    task_id = str(headers.get("id") or "")
    UUID(task_id)
    body = json.loads(base64.b64decode(envelope["body"]))
    args = body[0] if isinstance(body, list) and body else []
    kwargs = body[1] if isinstance(body, list) and len(body) > 1 else {}
    required = 2 if task_type.endswith("research_event") else 3
    if not isinstance(args, list) or len(args) < required:
        raise ValueError(f"research task {task_id} has invalid args")
    run_id = str(args[1] if task_type.endswith("research_event") else args[2])
    UUID(run_id)
    if not isinstance(kwargs, dict):
        raise ValueError(f"research task {task_id} has invalid kwargs")
    properties = envelope.get("properties") or {}
    raw_priority = properties.get("priority")
    return LegacyResearchMessage(
        queue_key=queue_key,
        task_id=task_id,
        task_type=task_type,
        run_id=run_id,
        args=args,
        kwargs=kwargs,
        priority=int(
            raw_priority
            if raw_priority is not None
            else (1 if task_type.endswith("research_event") else 3)
        ),
    )


def discover_legacy_messages(client: Redis) -> tuple[list[bytes], list[LegacyResearchMessage]]:
    keys = sorted(
        key for key in client.scan_iter(match="research*") if client.type(key) == b"list"
    )
    messages = [
        decode_legacy_message(key, raw)
        for key in keys
        for raw in client.lrange(key, 0, -1)
    ]
    task_ids = [item.task_id for item in messages]
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("duplicate task ids found in legacy research queues")
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
        event_ids = [item.run_id for item in messages if RESEARCH_TASKS[item.task_type] == "event"]
        asset_ids = [item.run_id for item in messages if RESEARCH_TASKS[item.task_type] == "asset"]
        known = set()
        if event_ids:
            known.update(
                db.execute(
                    text("SELECT id::text FROM event_research_runs WHERE id::text = ANY(CAST(:ids AS text[]))"),
                    {"ids": event_ids},
                ).scalars()
            )
        if asset_ids:
            known.update(
                db.execute(
                    text("SELECT id::text FROM research_runs WHERE id::text = ANY(CAST(:ids AS text[]))"),
                    {"ids": asset_ids},
                ).scalars()
            )
        missing = sorted({item.run_id for item in messages} - known)
        if missing:
            raise RuntimeError(f"research run rows missing: {len(missing)}")

        for item in messages:
            payload = json.dumps(
                {"args": item.args, "kwargs": item.kwargs},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            inserted = db.execute(
                text(
                    """
                    INSERT INTO go_jobs(
                        id,queue,task_type,payload,status,priority,max_attempts,
                        available_at,dedupe_key,created_at,updated_at
                    ) VALUES(
                        :id,'research',:task_type,CAST(:payload AS jsonb),'queued',
                        :priority,3,now(),:dedupe_key,now(),now()
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
                    "dedupe_key": f"research-run:{item.run_id}",
                },
            ).scalar_one_or_none()
            if inserted is None:
                existing = db.execute(
                    text(
                        """
                        SELECT id FROM go_jobs
                        WHERE id=:id OR (
                            queue='research' AND dedupe_key=:dedupe_key
                            AND status IN ('queued','running','retrying')
                        )
                        LIMIT 1
                        """
                    ),
                    {"id": item.task_id, "dedupe_key": f"research-run:{item.run_id}"},
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
    parser = argparse.ArgumentParser(description="Move drained legacy research messages into durable Go jobs.")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    client = Redis.from_url(get_settings().redis_url)
    print(json.dumps(migrate_legacy_messages(client, apply=args.apply), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
