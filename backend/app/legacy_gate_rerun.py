from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from backend.app.db import SessionLocal, init_db
from backend.app.services.research_admission import (
    ResearchAdmissionError,
    active_asset_research,
)
from backend.app.storage import (
    asset_has_research_phase,
    asset_has_scoring_version,
    get_event,
    get_run,
    list_latest_legacy_gate_recommendations,
)
from backend.app.worker import enqueue_research

CURRENT_SCORING_VERSION = "short-term-impact-v1"
RERUN_QUEUE_PHASE = "legacy_gate_research_queue"
logger = logging.getLogger(__name__)


def rerun_legacy_gate_recommendations(*, apply: bool = False) -> dict[str, Any]:
    """Queue at most one current-data rerun for each legacy gated asset."""

    init_db()
    with SessionLocal() as db:
        candidates = list_latest_legacy_gate_recommendations(
            db,
            current_scoring_version=CURRENT_SCORING_VERSION,
        )
        summary: dict[str, Any] = {
            "dry_run": not apply,
            "scoring_version": CURRENT_SCORING_VERSION,
            "requested": len(candidates),
            "queued": 0,
            "active": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
        }
        if not apply:
            return summary

        for recommendation in candidates:
            asset_id = recommendation.asset.asset_id
            result: dict[str, Any] = {
                "asset_id": asset_id,
                "recommendation_id": str(recommendation.id),
            }
            if asset_has_scoring_version(db, asset_id, CURRENT_SCORING_VERSION):
                result["status"] = "updated"
                summary["updated"] += 1
            elif (active := active_asset_research(db, asset_id)) is not None:
                result.update(status="active", run_id=str(active.id))
                summary["active"] += 1
            elif asset_has_research_phase(db, asset_id, RERUN_QUEUE_PHASE):
                result.update(status="skipped", detail="legacy gate rerun already attempted")
                summary["skipped"] += 1
            else:
                source_run = get_run(db, recommendation.run_id)
                if source_run is None:
                    result.update(status="skipped", detail="source research run no longer exists")
                    summary["skipped"] += 1
                else:
                    event = get_event(db, source_run.event_id) if source_run.event_id else None
                    if source_run.event_id is not None and event is None:
                        result.update(status="skipped", detail="source event no longer exists")
                        summary["skipped"] += 1
                    else:
                        try:
                            task_id, run = enqueue_research(
                                db,
                                recommendation.asset,
                                event,
                                force_research=True,
                                queue_phase=RERUN_QUEUE_PHASE,
                            )
                            result.update(
                                status="queued",
                                run_id=str(run.id),
                                task_id=task_id,
                            )
                            summary["queued"] += 1
                        except ResearchAdmissionError as exc:
                            db.rollback()
                            result.update(status="active", detail=exc.message)
                            if exc.run_id is not None:
                                result["run_id"] = exc.run_id
                            summary["active"] += 1
                        except Exception as exc:
                            db.rollback()
                            logger.exception("legacy gate rerun failed for %s", asset_id)
                            result.update(
                                status="failed",
                                detail=f"{type(exc).__name__}: research queue failed",
                            )
                            summary["failed"] += 1
            summary["results"].append(result)
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Queue one current-data rerun per legacy insufficient-evidence asset"
    )
    parser.add_argument("--apply", action="store_true", help="enqueue the selected reruns")
    args = parser.parse_args()
    print(
        json.dumps(
            rerun_legacy_gate_recommendations(apply=args.apply),
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
