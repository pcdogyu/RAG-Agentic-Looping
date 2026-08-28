from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

from backend.app import legacy_gate_rerun, worker
from backend.app.domain import (
    AnalysisStep,
    EventType,
    NewsEvent,
    Rating,
    Recommendation,
    ResearchRun,
    RunStatus,
    SourceQuality,
    Thesis,
    utc_now,
)
from backend.app.providers.registry import SEED_ASSETS
from backend.app.storage import list_runs, save_event, save_recommendation, save_run


def recommendation_for(run: ResearchRun) -> Recommendation:
    return Recommendation(
        run_id=run.id,
        asset=run.asset,
        score=0,
        rating=Rating.WATCH,
        confidence=0.45,
        bull_probability=0.2,
        base_probability=0.6,
        bear_probability=0.2,
        thesis=Thesis(summary="Legacy gated conclusion"),
        as_of=run.as_of,
        evidence_complete=False,
    )


def _patch_research_queue(monkeypatch):
    queued = []
    monkeypatch.setattr(
        worker,
        "select_model_instance",
        lambda *_args, **_kwargs: SimpleNamespace(id="research-0"),
    )
    monkeypatch.setattr(
        worker.research_asset,
        "apply_async",
        lambda **kwargs: queued.append(kwargs) or SimpleNamespace(id=kwargs["task_id"]),
    )
    monkeypatch.setattr(worker, "_record_research_dispatch", lambda *_args, **_kwargs: False)
    return queued


def test_legacy_gate_rerun_keeps_latest_per_asset_and_is_idempotent(
    db, monkeypatch
):
    queued = _patch_research_queue(monkeypatch)
    # The command must preserve the latest legacy conclusion's event lineage.
    now = utc_now()
    old_event = NewsEvent(
        news_item_ids=[],
        headline="Old gated event",
        event_type=EventType.OTHER,
        direct_impact="Old event",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now - timedelta(hours=2),
        observed_at=now - timedelta(hours=2),
        as_of=now - timedelta(hours=2),
    )
    latest_event = old_event.model_copy(
        update={
            "id": None,
            "headline": "Latest gated event",
            "published_at": now - timedelta(hours=1),
            "observed_at": now - timedelta(hours=1),
            "as_of": now - timedelta(hours=1),
        }
    )
    latest_event = NewsEvent(**latest_event.model_dump(exclude={"id"}))
    save_event(db, old_event)
    save_event(db, latest_event)
    old_run = ResearchRun(
        asset=SEED_ASSETS[0],
        event_id=old_event.id,
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        as_of=old_event.as_of,
        completed_at=old_event.as_of,
    )
    latest_run = ResearchRun(
        asset=SEED_ASSETS[0],
        event_id=latest_event.id,
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        as_of=latest_event.as_of,
        completed_at=latest_event.as_of,
    )
    save_run(db, old_run)
    save_run(db, latest_run)
    old_recommendation = recommendation_for(old_run)
    old_recommendation.as_of = old_event.as_of
    latest_recommendation = recommendation_for(latest_run)
    latest_recommendation.as_of = latest_event.as_of
    save_recommendation(db, old_recommendation)
    save_recommendation(db, latest_recommendation)

    first = legacy_gate_rerun.rerun_legacy_gate_recommendations(apply=True)
    repeated = legacy_gate_rerun.rerun_legacy_gate_recommendations(apply=True)

    assert first["requested"] == 1
    assert first["queued"] == 1
    assert first["results"][0]["recommendation_id"] == str(latest_recommendation.id)
    assert repeated["queued"] == 0
    assert repeated["active"] == 1
    assert len(queued) == 1
    rerun = next(
        run
        for run in list_runs(db, limit=10)
        if any(step.phase == legacy_gate_rerun.RERUN_QUEUE_PHASE for step in run.analysis_steps)
    )
    assert rerun.event_id == latest_event.id
    assert rerun.retry_of_run_id is None
    assert rerun.historical_replay is False

    rerun.status = RunStatus.FAILED
    rerun.completed_at = utc_now()
    save_run(db, rerun)
    after_failure = legacy_gate_rerun.rerun_legacy_gate_recommendations(apply=True)
    assert after_failure["queued"] == 0
    assert after_failure["skipped"] == 1
    assert len(queued) == 1


def test_legacy_gate_rerun_reports_assets_already_updated(db, monkeypatch):
    _patch_research_queue(monkeypatch)
    legacy_run = ResearchRun(
        asset=SEED_ASSETS[0],
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        completed_at=worker.utc_now(),
    )
    current_run = ResearchRun(
        asset=SEED_ASSETS[0],
        status=RunStatus.COMPLETED,
        completed_at=worker.utc_now(),
    )
    save_run(db, legacy_run)
    save_run(db, current_run)
    save_recommendation(db, recommendation_for(legacy_run))
    current = recommendation_for(current_run)
    current.scoring_version = legacy_gate_rerun.CURRENT_SCORING_VERSION
    save_recommendation(db, current)

    result = legacy_gate_rerun.rerun_legacy_gate_recommendations(apply=True)

    assert result["requested"] == 1
    assert result["updated"] == 1
    assert result["queued"] == 0
    assert result["results"][0]["status"] == "updated"


def test_legacy_gate_rerun_continues_after_one_queue_failure(db, monkeypatch):
    runs = []
    for index, base_asset in enumerate(SEED_ASSETS[:2]):
        asset = base_asset.model_copy(
            update={"asset_id": f"test:legacy:{index}", "symbol": f"L{index}"}
        )
        run = ResearchRun(
            asset=asset,
            status=RunStatus.INSUFFICIENT_EVIDENCE,
            completed_at=worker.utc_now(),
        )
        save_run(db, run)
        save_recommendation(db, recommendation_for(run))
        runs.append(run)

    calls = []

    def fake_enqueue(_db, asset, _event, **_kwargs):
        calls.append(asset.asset_id)
        if len(calls) == 1:
            raise RuntimeError("broker offline")
        return "queued-task", ResearchRun(asset=asset)

    monkeypatch.setattr(legacy_gate_rerun, "enqueue_research", fake_enqueue)

    result = legacy_gate_rerun.rerun_legacy_gate_recommendations(apply=True)

    assert calls == [run.asset.asset_id for run in runs]
    assert result["requested"] == 2
    assert result["queued"] == 1
    assert result["failed"] == 1
    assert [item["status"] for item in result["results"]] == ["failed", "queued"]


def test_legacy_gate_rerun_counts_active_and_missing_sources(db, monkeypatch):
    _patch_research_queue(monkeypatch)
    missing_run = ResearchRun(
        asset=SEED_ASSETS[0].model_copy(
            update={"asset_id": "test:legacy:missing-run", "symbol": "MR"}
        ),
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        completed_at=utc_now(),
    )
    # Recommendations are historical payloads and can outlive their source run.
    save_recommendation(db, recommendation_for(missing_run))

    missing_event = ResearchRun(
        asset=SEED_ASSETS[0].model_copy(
            update={"asset_id": "test:legacy:missing-event", "symbol": "ME"}
        ),
        event_id=NewsEvent(
            news_item_ids=[],
            headline="Deleted event",
            event_type=EventType.OTHER,
            direct_impact="Deleted event",
            source_quality=SourceQuality.PROFESSIONAL,
            published_at=utc_now(),
        ).id,
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        completed_at=utc_now(),
    )
    save_run(db, missing_event)
    save_recommendation(db, recommendation_for(missing_event))

    active_source = ResearchRun(
        asset=SEED_ASSETS[0].model_copy(
            update={"asset_id": "test:legacy:active", "symbol": "AC"}
        ),
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        completed_at=utc_now(),
    )
    save_run(db, active_source)
    save_recommendation(db, recommendation_for(active_source))
    active = ResearchRun(
        asset=active_source.asset,
        status=RunStatus.RUNNING,
        analysis_steps=[
            AnalysisStep(
                phase=legacy_gate_rerun.RERUN_QUEUE_PHASE,
                executor="celery",
                summary="new scoring rerun active",
                metrics={"scoring_version": legacy_gate_rerun.CURRENT_SCORING_VERSION},
            )
        ],
    )
    save_run(db, active)

    result = legacy_gate_rerun.rerun_legacy_gate_recommendations(apply=True)

    assert result["requested"] == 3
    assert result["queued"] == 0
    assert result["active"] == 1
    assert result["updated"] == 0
    assert result["skipped"] == 2
    assert result["failed"] == 0
    assert {item["status"] for item in result["results"]} == {"active", "skipped"}
    assert any(
        item.get("detail") == "source research run no longer exists"
        for item in result["results"]
    )
    assert any(
        item.get("detail") == "source event no longer exists"
        for item in result["results"]
    )
