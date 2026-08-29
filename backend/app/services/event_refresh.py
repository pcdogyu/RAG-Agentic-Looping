from __future__ import annotations

from typing import Any

from backend.app.domain import AnalysisStep, EventResearchRun, NewsEvent, SourceQuality, utc_now

FULL_EVENT_RESEARCH_PHASE = "full_event_research"
FULL_EVENT_RESEARCH_ACTIVE_STATUSES = {"queued", "running", "retrying"}


def latest_full_event_research_step(run: EventResearchRun) -> AnalysisStep | None:
    return next(
        (
            step
            for step in reversed(run.analysis_steps)
            if step.phase == FULL_EVENT_RESEARCH_PHASE
        ),
        None,
    )


def full_event_research_is_active(run: EventResearchRun) -> bool:
    step = latest_full_event_research_step(run)
    return bool(step and step.status in FULL_EVENT_RESEARCH_ACTIVE_STATUSES)


def begin_full_event_research(run: EventResearchRun, *, task_id: str) -> None:
    run.analysis_steps.append(
        AnalysisStep(
            phase=FULL_EVENT_RESEARCH_PHASE,
            status="queued",
            executor="celery",
            summary="已创建事件抽取、股票映射、深度研究与联网搜索的完整重跑任务。",
            metrics={"stage": "event_extraction", "task_id": task_id},
        )
    )


def update_full_event_research(
    run: EventResearchRun,
    *,
    status: str,
    stage: str,
    summary: str,
    error: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> None:
    step = latest_full_event_research_step(run)
    if step is None:
        step = AnalysisStep(
            phase=FULL_EVENT_RESEARCH_PHASE,
            executor="celery",
            summary=summary,
        )
        run.analysis_steps.append(step)
    step.status = status
    step.summary = summary
    step.occurred_at = utc_now()
    step.metrics = {
        **step.metrics,
        **(metrics or {}),
        "stage": stage,
        **({"error": error} if error else {}),
    }


def public_full_event_research(run: EventResearchRun) -> dict[str, Any] | None:
    step = latest_full_event_research_step(run)
    if step is None or step.status == "completed":
        return None
    return {
        "status": step.status,
        "stage": str(step.metrics.get("stage") or "event_extraction"),
        "error": step.metrics.get("error"),
    }


def rebuild_event_from_extractions(
    event: NewsEvent,
    extracted_events: list[NewsEvent],
    *,
    missing_news_count: int = 0,
) -> NewsEvent:
    """Replace extracted facts while preserving the durable event identity."""

    if not extracted_events:
        raise ValueError("event has no source news available for extraction")
    lead = extracted_events[0]
    entities: list[str] = []
    actions = []
    known_actions = set()
    extraction_steps: list[AnalysisStep] = []
    quality_rank = {
        SourceQuality.SOCIAL: 0,
        SourceQuality.AGGREGATOR: 1,
        SourceQuality.PROFESSIONAL: 2,
        SourceQuality.PRIMARY: 3,
        SourceQuality.OFFICIAL: 4,
    }
    for extracted in extracted_events:
        entities.extend(value for value in extracted.entities if value not in entities)
        for action in extracted.actions:
            if action.id in known_actions:
                continue
            known_actions.add(action.id)
            actions.append(action)
        extraction_steps.extend(extracted.analysis_steps)

    event.headline = lead.headline
    event.event_type = lead.event_type
    event.entities = entities
    event.actions = actions[:3]
    event.direct_impact = lead.direct_impact
    event.horizon_days = lead.horizon_days
    event.source_quality = max(
        (item.source_quality for item in extracted_events),
        key=quality_rank.__getitem__,
    )
    event.published_at = min(item.published_at for item in extracted_events)
    event.observed_at = min(item.observed_at for item in extracted_events)
    event.as_of = max(item.as_of for item in extracted_events)
    event.candidates = []
    event.industry_ids = []
    event.novelty = min(item.novelty for item in extracted_events)
    event.priority = max(item.priority for item in extracted_events)
    event.analysis_steps.extend(extraction_steps)
    event.analysis_steps.append(
        AnalysisStep(
            phase="full_event_reextraction",
            status="completed",
            executor="event-refresh",
            summary=(
                f"已从 {len(extracted_events)} 篇关联新闻重新抽取事件事实，"
                f"缺失 {missing_news_count} 篇。"
            ),
            metrics={
                "available_news_count": len(extracted_events),
                "missing_news_count": missing_news_count,
            },
        )
    )
    return event
