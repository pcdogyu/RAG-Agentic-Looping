from __future__ import annotations

import argparse
import json
import logging
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from backend.app.db import EventResearchRunRow, SessionLocal, init_db
from backend.app.domain import AnalysisStep, EventResearchRun, RunStatus
from backend.app.services.model_instances import broker_queue_name, select_model_instance
from backend.app.storage import get_event, get_event_research_run, save_event_research_run
from backend.app.worker import (
    EVENT_RESEARCH_PRIORITY,
    _clear_research_dispatch,
    _record_research_dispatch,
    research_event,
)

MIGRATION_PHASE = "event_priority_migration"
logger = logging.getLogger(__name__)


def _already_migrated(run: EventResearchRun) -> bool:
    return any(
        step.phase == MIGRATION_PHASE
        and step.metrics.get("priority") == EVENT_RESEARCH_PRIORITY
        for step in run.analysis_steps
    )


def migrate_queued_event_priorities(*, apply: bool = False) -> dict[str, Any]:
    """Republish legacy queued event reports onto the corrected Redis priority."""

    init_db()
    with SessionLocal() as db:
        rows = list(
            db.scalars(
                select(EventResearchRunRow)
                .where(EventResearchRunRow.status == RunStatus.QUEUED.value)
                .order_by(EventResearchRunRow.created_at)
            ).all()
        )
        summary: dict[str, Any] = {
            "dry_run": not apply,
            "requested": len(rows),
            "requeued": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
        }
        for row in rows:
            run = EventResearchRun.model_validate(row.payload)
            result: dict[str, Any] = {"run_id": str(run.id)}
            if _already_migrated(run):
                result.update(status="skipped", detail="already migrated")
                summary["skipped"] += 1
                summary["results"].append(result)
                continue
            event = get_event(db, run.event_id)
            if event is None:
                result.update(status="skipped", detail="source event no longer exists")
                summary["skipped"] += 1
                summary["results"].append(result)
                continue
            if not apply:
                result.update(status="pending", previous_task_id=run.celery_task_id)
                summary["results"].append(result)
                continue

            db.expire_all()
            current = get_event_research_run(db, run.id)
            if current is None or current.status is not RunStatus.QUEUED:
                result.update(status="skipped", detail="run is no longer queued")
                summary["skipped"] += 1
                summary["results"].append(result)
                continue
            if _already_migrated(current):
                result.update(status="skipped", detail="already migrated")
                summary["skipped"] += 1
                summary["results"].append(result)
                continue

            original = current.model_copy(deep=True)
            previous_task_id = current.celery_task_id
            new_task_id = str(uuid4())
            try:
                instance = select_model_instance(
                    "research",
                    task_id=new_task_id,
                    preferred=current.model_instance_id,
                    probe_health=True,
                )
                if not _record_research_dispatch(str(current.id), new_task_id):
                    raise RuntimeError("research dispatch marker unavailable")
                current.celery_task_id = new_task_id
                current.model_instance_id = instance.id
                current.analysis_steps.append(
                    AnalysisStep(
                        phase=MIGRATION_PHASE,
                        status="queued",
                        executor="deployment",
                        summary=(
                            "事件研报已按修正后的 Redis 优先级重新派发；原消息将作为过期副本跳过。"
                        ),
                        metrics={
                            "priority": EVENT_RESEARCH_PRIORITY,
                            "previous_task_id": previous_task_id,
                            "instance_id": instance.id,
                        },
                    )
                )
                save_event_research_run(db, current)
                research_event.apply_async(
                    args=[str(current.event_id), str(current.id)],
                    kwargs={"model_instance_id": instance.id},
                    queue=broker_queue_name("research", instance.id),
                    task_id=new_task_id,
                    priority=EVENT_RESEARCH_PRIORITY,
                )
            except Exception as exc:
                db.rollback()
                _clear_research_dispatch(str(current.id), new_task_id)
                if previous_task_id:
                    _record_research_dispatch(str(current.id), previous_task_id)
                save_event_research_run(db, original)
                logger.exception("event priority migration failed for %s", current.id)
                result.update(status="failed", detail=f"{type(exc).__name__}: {exc}")
                summary["failed"] += 1
                summary["results"].append(result)
                continue

            result.update(
                status="requeued",
                previous_task_id=previous_task_id,
                task_id=new_task_id,
                instance_id=instance.id,
            )
            summary["requeued"] += 1
            summary["results"].append(result)
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Republish queued event reports at the corrected Redis priority"
    )
    parser.add_argument("--apply", action="store_true", help="republish selected event reports")
    args = parser.parse_args()
    print(json.dumps(migrate_queued_event_priorities(apply=args.apply), ensure_ascii=False))


if __name__ == "__main__":
    main()
