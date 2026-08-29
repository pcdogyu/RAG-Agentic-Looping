from __future__ import annotations

import argparse
import json
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from backend.app.db import EventResearchRunRow, EventRow, SessionLocal, init_db
from backend.app.domain import AnalysisStep, EventResearchRun, RunStatus
from backend.app.services.events import EventService
from backend.app.services.model_instances import broker_queue_name, select_model_instance
from backend.app.storage import get_event, get_news, save_event, save_event_research_run

BACKFILL_PHASE = "target_impact_v2_replay"
BACKFILL_SCORING_VERSION = "target-transmission-v2"
ACTIVE_STATUSES = {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}


def _is_v2(run: EventResearchRun | None) -> bool:
    return bool(
        run
        and run.report
        and run.report.scoring_version == BACKFILL_SCORING_VERSION
    )


def reprocess_target_impacts(
    *,
    apply: bool = False,
    batch_size: int = 25,
    max_active: int = 50,
) -> dict[str, Any]:
    """Preview or enqueue one idempotent, point-in-time v2 replay batch."""

    from backend.app.worker import (
        EVENT_RESEARCH_PRIORITY,
        _clear_research_dispatch,
        _record_research_dispatch,
        research_event,
    )

    init_db()
    with SessionLocal() as db:
        event_rows = list(db.scalars(select(EventRow).order_by(EventRow.published_at)).all())
        run_rows = list(db.scalars(select(EventResearchRunRow)).all())
        runs_by_event = {
            row.event_id: EventResearchRun.model_validate(row.payload) for row in run_rows
        }
        pending_rows = [
            row for row in event_rows if not _is_v2(runs_by_event.get(row.id))
        ]
        active = sum(
            run.status in ACTIVE_STATUSES
            for run in runs_by_event.values()
            if not _is_v2(run)
        )
        failures = sum(
            run.status is RunStatus.FAILED
            for run in runs_by_event.values()
            if not _is_v2(run)
        )
        capacity = max(0, max_active - active)
        available_rows = [
            row
            for row in pending_rows
            if (
                runs_by_event.get(row.id) is None
                or runs_by_event[row.id].status not in ACTIVE_STATUSES
            )
        ]
        selected = available_rows[: min(batch_size, capacity)]
        summary: dict[str, Any] = {
            "dry_run": not apply,
            "scoring_version": BACKFILL_SCORING_VERSION,
            "pending": len(pending_rows),
            "failed": failures,
            "active": active,
            "capacity": capacity,
            "selected": len(selected),
            "queued": 0,
            "queue_failures": 0,
            "complete": len(pending_rows) == 0 and failures == 0,
            "results": [],
        }
        if not apply:
            summary["results"] = [
                {"event_id": str(row.id), "status": "pending"} for row in selected
            ]
            return summary

        for event_row in selected:
            event = get_event(db, event_row.id)
            if event is None:
                summary["queue_failures"] += 1
                summary["results"].append(
                    {"event_id": str(event_row.id), "status": "missing_event"}
                )
                continue
            if not event.actions:
                source = next(
                    (
                        item
                        for news_id in event.news_item_ids
                        if (item := get_news(db, news_id)) is not None
                    ),
                    None,
                )
                if source is not None:
                    event.actions = EventService._fallback_actions(source)
                    save_event(db, event)

            current = runs_by_event.get(event.id)
            if current is not None and _is_v2(current):
                summary["results"].append(
                    {"event_id": str(event.id), "status": "already_v2"}
                )
                continue
            original = current.model_copy(deep=True) if current else None
            run = current or EventResearchRun(event_id=event.id)
            if run.report is not None:
                run.report_history.append(run.report)
            task_id = str(uuid4())
            instance = select_model_instance(
                "research",
                task_id=task_id,
                preferred=run.model_instance_id,
                probe_health=True,
            )
            run.report = None
            run.status = RunStatus.QUEUED
            run.as_of = event.as_of
            run.historical_replay = True
            run.verification_round = 0
            run.retry_count = 0
            run.celery_task_id = task_id
            run.model_instance_id = instance.id
            run.retryable_reason = None
            run.missing_requirements = []
            run.contradictions = []
            run.evidence = []
            run.error = None
            run.analysis_steps.append(
                AnalysisStep(
                    phase=BACKFILL_PHASE,
                    status="queued",
                    executor="target-impact-backfill",
                    model=BACKFILL_SCORING_VERSION,
                    summary="已按原 as_of 创建点时逐目标 v2 后继运行；实时搜索和当前基本面已禁用。",
                    metrics={
                        "historical_replay": True,
                        "as_of": event.as_of.isoformat(),
                        "archived_report_count": len(run.report_history),
                    },
                )
            )
            try:
                if current is not None and current.status is RunStatus.CANCELLED:
                    event_run_row = db.get(EventResearchRunRow, current.id)
                    assert event_run_row is not None
                    event_run_row.status = RunStatus.QUEUED.value
                    db.commit()
                if not _record_research_dispatch(str(run.id), task_id):
                    raise RuntimeError("research dispatch marker unavailable")
                save_event_research_run(db, run)
                research_event.apply_async(
                    args=[str(event.id), str(run.id)],
                    kwargs={"model_instance_id": instance.id},
                    queue=broker_queue_name("research", instance.id),
                    task_id=task_id,
                    priority=EVENT_RESEARCH_PRIORITY,
                )
            except Exception as exc:
                db.rollback()
                _clear_research_dispatch(str(run.id), task_id)
                if original is not None:
                    if original.status is RunStatus.CANCELLED:
                        event_run_row = db.get(EventResearchRunRow, original.id)
                        assert event_run_row is not None
                        event_run_row.status = RunStatus.QUEUED.value
                        db.commit()
                    save_event_research_run(db, original)
                summary["queue_failures"] += 1
                summary["results"].append(
                    {
                        "event_id": str(event.id),
                        "status": "failed",
                        "detail": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            summary["queued"] += 1
            summary["results"].append(
                {
                    "event_id": str(event.id),
                    "run_id": str(run.id),
                    "task_id": task_id,
                    "status": "queued",
                }
            )
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay historical events with target v2")
    parser.add_argument("--apply", action="store_true", help="enqueue the selected batch")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--max-active", type=int, default=50)
    args = parser.parse_args()
    result = reprocess_target_impacts(
        apply=args.apply,
        batch_size=max(1, args.batch_size),
        max_active=max(1, args.max_active),
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
