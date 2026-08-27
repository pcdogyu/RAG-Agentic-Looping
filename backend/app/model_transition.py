from __future__ import annotations

import argparse
import json
from typing import Any

from sqlalchemy import select

from backend.app.db import EventResearchRunRow, ResearchRunRow, SessionLocal, init_db
from backend.app.domain import AnalysisStep, EventResearchRun, ResearchRun, RunStatus, utc_now
from backend.app.services.research_cancellation import _cancel_asset_run, _cancel_event_run
from backend.app.storage import get_event, save_event_research_run, save_run
from backend.app.worker import enqueue_event_research_retry, enqueue_research

MIGRATED_STATUSES = {RunStatus.RUNNING.value, RunStatus.VERIFYING.value}
MIGRATION_REASON = "模型迁移取消：14B 已下线，任务将使用 qwen2.5:7b 重新执行。"


def transition_active_research(*, apply: bool = False) -> dict[str, Any]:
    init_db()
    with SessionLocal() as db:
        asset_rows = list(
            db.scalars(select(ResearchRunRow).where(ResearchRunRow.status.in_(MIGRATED_STATUSES)))
        )
        event_rows = list(
            db.scalars(
                select(EventResearchRunRow).where(
                    EventResearchRunRow.status.in_(MIGRATED_STATUSES)
                )
            )
        )
        preview = {
            "dry_run": not apply,
            "asset_runs": len(asset_rows),
            "event_runs": len(event_rows),
            "old_task_ids": [
                task_id
                for task_id in [
                    *(ResearchRun.model_validate(row.payload).celery_task_id for row in asset_rows),
                    *(EventResearchRun.model_validate(row.payload).celery_task_id for row in event_rows),
                ]
                if task_id
            ],
        }
        if not apply:
            return preview

        asset_snapshots = [ResearchRun.model_validate(row.payload) for row in asset_rows]
        event_snapshots = [EventResearchRun.model_validate(row.payload) for row in event_rows]
        for run in asset_snapshots:
            _cancel_asset_run(run, MIGRATION_REASON)
            run.analysis_steps[-1].metrics["migration_model"] = "qwen2.5:7b"
            save_run(db, run)
        for run in event_snapshots:
            _cancel_event_run(run, MIGRATION_REASON)
            run.analysis_steps[-1].metrics["migration_model"] = "qwen2.5:7b"

        queued: list[dict[str, str]] = []
        for original in asset_snapshots:
            event = get_event(db, original.event_id) if original.event_id else None
            task_id, retry = enqueue_research(
                db,
                original.asset,
                event,
                as_of=utc_now(),
                historical_replay=original.historical_replay,
                retry_of_run_id=original.id,
                retry_attempt=original.retry_attempt + 1,
            )
            retry.analysis_steps.append(
                AnalysisStep(
                    phase="model_migration_retry",
                    executor="deployment",
                    model="qwen2.5:7b",
                    summary="14B 活动任务已迁移到专用 7B 研究实例。",
                    metrics={"source_run_id": str(original.id)},
                )
            )
            save_run(db, retry)
            queued.append({"kind": "asset", "run_id": str(retry.id), "task_id": task_id})
        for run in event_snapshots:
            event = get_event(db, run.event_id)
            if event is None:
                continue
            task_id, retry = enqueue_event_research_retry(db, event, run)
            retry.analysis_steps.append(
                AnalysisStep(
                    phase="model_migration_retry",
                    executor="deployment",
                    model="qwen2.5:7b",
                    summary="14B 活动事件研报已迁移到专用 7B 研究实例。",
                )
            )
            save_event_research_run(db, retry)
            queued.append({"kind": "event", "run_id": str(retry.id), "task_id": task_id})
        return {**preview, "queued": queued}


def main() -> None:
    parser = argparse.ArgumentParser(description="Requeue active 14B research onto the 7B lane")
    parser.add_argument("--apply", action="store_true", help="cancel and requeue active runs")
    args = parser.parse_args()
    print(json.dumps(transition_active_research(apply=args.apply), ensure_ascii=False))


if __name__ == "__main__":
    main()
