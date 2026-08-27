from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from redis import Redis

from backend.app.config import Settings, get_settings

SOURCE_QUEUE = "llm"
TASK_DESTINATIONS = {
    "market_loop.resolve_event_assets": "mapping",
    "market_loop.research_asset": "research",
    "market_loop.research_event": "research",
}
BACKUP_TTL_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class QueueMigrationPlan:
    messages: list[bytes]
    destinations: dict[str, list[bytes]]
    task_ids: list[str]

    @property
    def counts(self) -> dict[str, int]:
        return {
            "source": len(self.messages),
            **{queue: len(items) for queue, items in self.destinations.items()},
        }


def _message_metadata(raw: bytes) -> tuple[str, str]:
    try:
        payload: dict[str, Any] = json.loads(raw)
        headers = payload["headers"]
        return str(headers["task"]), str(headers["id"])
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError("unsupported Celery message in legacy llm queue") from exc


def plan_messages(
    messages: list[bytes], existing_messages: dict[str, list[bytes]] | None = None
) -> QueueMigrationPlan:
    destinations: dict[str, list[bytes]] = {"mapping": [], "research": []}
    task_ids: list[str] = []
    for raw in messages:
        task_name, task_id = _message_metadata(raw)
        destination = TASK_DESTINATIONS.get(task_name)
        if destination is None:
            raise ValueError(f"unsupported legacy task type: {task_name}")
        destinations[destination].append(raw)
        task_ids.append(task_id)
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("duplicate task id found inside legacy llm queue")
    existing_ids: set[str] = set()
    for queue in destinations:
        for raw in (existing_messages or {}).get(queue, []):
            _, task_id = _message_metadata(raw)
            existing_ids.add(task_id)
    duplicates = sorted(existing_ids.intersection(task_ids))
    if duplicates:
        raise ValueError(f"task ids already exist in destination queues: {duplicates[:5]}")
    return QueueMigrationPlan(messages, destinations, task_ids)


def build_plan(client: Redis) -> QueueMigrationPlan:
    return plan_messages(
        list(client.lrange(SOURCE_QUEUE, 0, -1)),
        {
            queue: list(client.lrange(queue, 0, -1))
            for queue in {"mapping", "research"}
        },
    )


def apply_plan(client: Redis, plan: QueueMigrationPlan) -> dict[str, Any]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_key = f"market-loop:backup:queue:{SOURCE_QUEUE}:{stamp}"
    metadata_key = f"{backup_key}:metadata"
    with client.pipeline() as pipe:
        while True:
            try:
                pipe.watch(SOURCE_QUEUE, *plan.destinations)
                current = list(pipe.lrange(SOURCE_QUEUE, 0, -1))
                if current != plan.messages:
                    raise RuntimeError("legacy llm queue changed after migration planning")
                pipe.multi()
                if plan.messages:
                    pipe.rpush(backup_key, *plan.messages)
                pipe.set(
                    metadata_key,
                    json.dumps({"source": SOURCE_QUEUE, "counts": plan.counts}),
                    ex=BACKUP_TTL_SECONDS,
                )
                pipe.expire(backup_key, BACKUP_TTL_SECONDS)
                pipe.delete(SOURCE_QUEUE)
                for queue, messages in plan.destinations.items():
                    if messages:
                        # Celery consumes Redis lists from the right. LPUSH in
                        # oldest-to-newest order preserves the original order.
                        pipe.lpush(queue, *reversed(messages))
                pipe.execute()
                break
            except Exception:
                pipe.reset()
                raise
    migrated = sum(len(items) for items in plan.destinations.values())
    if migrated != len(plan.messages):
        raise RuntimeError("legacy queue migration count mismatch")
    return {"backup_key": backup_key, "counts": plan.counts, "migrated": migrated}


def migrate_legacy_llm_queue(
    *, settings: Settings | None = None, apply: bool = False
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    client = Redis.from_url(active_settings.redis_url)
    plan = build_plan(client)
    if not apply:
        return {"dry_run": True, "counts": plan.counts, "task_ids": len(plan.task_ids)}
    return {"dry_run": False, **apply_plan(client, plan)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Split the legacy Celery llm queue by task lane")
    parser.add_argument("--apply", action="store_true", help="apply the migration")
    args = parser.parse_args()
    print(json.dumps(migrate_legacy_llm_queue(apply=args.apply), ensure_ascii=False))


if __name__ == "__main__":
    main()
