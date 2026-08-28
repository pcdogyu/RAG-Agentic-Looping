from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy.orm import Session

from backend.app.domain import EventResearchRun, NewsEvent, ResearchRun, RunStatus
from backend.app.storage import (
    get_events_by_ids,
    list_recommendation_run_ids,
    list_retryable_event_research_runs,
    list_retryable_run_lineages,
)

ACTIVE_RETRY_STATUSES = {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.VERIFYING}


@dataclass(frozen=True)
class FailedResearchCandidate:
    kind: Literal["asset", "event"]
    run: ResearchRun | EventResearchRun
    event: NewsEvent | None
    retries: tuple[ResearchRun, ...] = ()

    @property
    def source_run_id(self) -> UUID:
        return self.run.id

    @property
    def updated_at(self) -> datetime:
        return self.run.updated_at

    def api_item(self) -> dict:
        latest_retry = self.retries[0] if self.retries else None
        asset = self.run.asset if isinstance(self.run, ResearchRun) else None
        return {
            "kind": self.kind,
            "id": str(self.run.id),
            "status": self.run.status.value,
            "asset": asset.model_dump(mode="json") if asset else None,
            "event": (
                {"id": str(self.event.id), "headline": self.event.headline}
                if self.event
                else None
            ),
            "error": self.run.error or self.run.retryable_reason,
            "updated_at": self.run.updated_at.isoformat(),
            "retry_count": (
                len(self.retries)
                if isinstance(self.run, ResearchRun)
                else self.run.retry_count
            ),
            "latest_retry": (
                {
                    "id": str(latest_retry.id),
                    "status": latest_retry.status.value,
                    "updated_at": latest_retry.updated_at.isoformat(),
                }
                if latest_retry
                else None
            ),
        }


def failed_research_candidates(db: Session) -> list[FailedResearchCandidate]:
    """Return one old-to-new snapshot shared by the list and bulk retry APIs."""

    originals, retries_by_original = list_retryable_run_lineages(db)
    event_runs = list_retryable_event_research_runs(db, None)
    recommendation_run_ids = list_recommendation_run_ids(db)
    event_ids = {
        event_id
        for event_id in [
            *(run.event_id for run in originals),
            *(run.event_id for run in event_runs),
        ]
        if event_id is not None
    }
    events = get_events_by_ids(db, event_ids)
    candidates: list[FailedResearchCandidate] = []

    for run in originals:
        retries = tuple(retries_by_original.get(run.id, []))
        if run.retryable_reason is None and run.id in recommendation_run_ids:
            continue
        if any(
            retry.retryable_reason is None and retry.id in recommendation_run_ids
            for retry in retries
        ):
            continue
        if any(retry.status in ACTIVE_RETRY_STATUSES for retry in retries):
            continue
        candidates.append(
            FailedResearchCandidate(
                kind="asset",
                run=run,
                event=events.get(run.event_id) if run.event_id else None,
                retries=retries,
            )
        )

    candidates.extend(
        FailedResearchCandidate(
            kind="event",
            run=run,
            event=events.get(run.event_id),
        )
        for run in event_runs
    )
    candidates.sort(key=lambda item: item.updated_at)
    return candidates
