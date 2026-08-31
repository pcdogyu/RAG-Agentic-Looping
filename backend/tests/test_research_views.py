from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient

from backend.app.db import RecommendationRow
from backend.app.domain import (
    AnalysisStep,
    AssetClass,
    AssetRef,
    EventReport,
    EventResearchRun,
    EventType,
    Market,
    NewsEvent,
    Rating,
    Recommendation,
    ResearchRun,
    RunStatus,
    SignalStatus,
    SourceQuality,
    TargetImpact,
    TargetType,
    Thesis,
    utc_now,
)
from backend.app.main import app
from backend.app.storage import (
    save_event,
    save_event_research_run,
    save_recommendation,
    save_run,
)


def asset(symbol: str, *, asset_class: AssetClass = AssetClass.EQUITY) -> AssetRef:
    market = {
        AssetClass.EQUITY: Market.US,
        AssetClass.CRYPTO: Market.CRYPTO,
        AssetClass.COMMODITY: Market.COMMODITY,
        AssetClass.FX: Market.FX,
    }[asset_class]
    return AssetRef(
        asset_id=f"{asset_class.value}:test:{symbol}",
        asset_class=asset_class,
        market=market,
        symbol=symbol,
        name=f"{symbol} Research Target",
        exchange_or_provider="test",
    )


def recommendation(
    target: AssetRef,
    *,
    rating: Rating,
    score: int,
    as_of,
    news_confidence: float = 0.72,
) -> tuple[Recommendation, ResearchRun]:
    run = ResearchRun(asset=target, status=RunStatus.COMPLETED, as_of=as_of)
    item = Recommendation(
        run_id=run.id,
        asset=target,
        score=score,
        direction_score=score,
        raw_score=score,
        rating=rating,
        confidence=0.72,
        rating_confidence=0.72,
        news_confidence=news_confidence,
        bull_probability=0.6 if score > 0 else 0.2,
        base_probability=0.2,
        bear_probability=0.2 if score > 0 else 0.6,
        thesis=Thesis(summary=f"{target.symbol} latest research"),
        as_of=as_of,
        evidence_complete=True,
        signal_status=SignalStatus.DIRECTIONAL,
        scoring_version="llm-direction-v3",
    )
    run.recommendation = item
    return item, run


def event_run(
    db,
    name: str,
    *,
    status: RunStatus,
    impacts: list[TargetImpact],
    report: bool = True,
    news_confidence: float = 0.67,
    retryable_reason: str | None = None,
) -> EventResearchRun:
    now = utc_now()
    event = NewsEvent(
        news_item_ids=[],
        headline=name,
        event_type=EventType.MACRO,
        direct_impact=f"{name} direct impact",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now,
        observed_at=now,
        as_of=now,
    )
    save_event(db, event)
    run = EventResearchRun(
        event_id=event.id,
        status=status,
        retryable_reason=retryable_reason,
        report=(
            EventReport(
                summary=f"{name} report",
                confidence=0.61,
                news_confidence=news_confidence,
                evidence_complete=status is RunStatus.COMPLETED,
                impacts=impacts,
            )
            if report
            else None
        ),
    )
    save_event_research_run(db, run)
    return run


def impact(
    name: str,
    *,
    target_type: TargetType,
    rating: Rating,
    score: int,
    target_asset: AssetRef | None = None,
) -> TargetImpact:
    return TargetImpact(
        target_type=target_type,
        target_name=name,
        asset=target_asset,
        direction_score=score,
        rating=rating,
        rating_confidence=0.68,
    )


def test_research_conclusions_unify_visible_terminal_results_and_detail(db):
    now = utc_now()
    target = asset("UNION")
    asset_item, asset_run = recommendation(
        target,
        rating=Rating.BULLISH,
        score=45,
        as_of=now + timedelta(minutes=5),
    )
    save_run(db, asset_run)
    save_recommendation(db, asset_item)

    completed = event_run(
        db,
        "completed macro",
        status=RunStatus.COMPLETED,
        impacts=[
            impact(
                "Energy sector",
                target_type=TargetType.SECTOR,
                rating=Rating.BULLISH,
                score=40,
            ),
            impact(
                "Energy costs",
                target_type=TargetType.COMMODITY_PRICE,
                rating=Rating.STRONGLY_BEARISH,
                score=-75,
            ),
        ],
    )
    tied = event_run(
        db,
        "tied macro",
        status=RunStatus.COMPLETED,
        impacts=[
            impact(
                "First tied target",
                target_type=TargetType.SECTOR,
                rating=Rating.STRONGLY_BULLISH,
                score=70,
            ),
            impact(
                "Second tied target",
                target_type=TargetType.COMMODITY_PRICE,
                rating=Rating.STRONGLY_BEARISH,
                score=-70,
            ),
        ],
    )
    insufficient = event_run(
        db,
        "insufficient macro",
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        impacts=[],
    )
    refreshing = event_run(
        db,
        "refreshing macro",
        status=RunStatus.QUEUED,
        impacts=[],
    )
    refreshing.analysis_steps.append(
        AnalysisStep(
            phase="full_event_research",
            status="running",
            executor="celery",
            summary="Mapping refreshed event",
            metrics={"stage": "asset_mapping"},
        )
    )
    save_event_research_run(db, refreshing)
    event_run(db, "failed macro", status=RunStatus.FAILED, impacts=[])
    event_run(db, "cancelled macro", status=RunStatus.CANCELLED, impacts=[])
    event_run(
        db,
        "report missing",
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        impacts=[],
        report=False,
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/research-conclusions?limit=10")
        event_only = client.get("/api/v1/research-conclusions?kind=event&limit=10")
        market_only = client.get("/api/v1/research-conclusions?market=US&limit=10")
        detail = client.get(f"/api/v1/event-conclusions/{completed.id}")
        first = client.get("/api/v1/research-conclusions?limit=1")
        second = client.get(
            "/api/v1/research-conclusions",
            params={"limit": 1, "cursor": first.json()["next_cursor"]},
        )

    assert response.status_code == 200
    visible_ids = {item["id"] for item in response.json()["items"]}
    assert str(asset_item.id) in visible_ids
    assert str(completed.id) in visible_ids
    assert str(tied.id) in visible_ids
    assert str(insufficient.id) in visible_ids
    assert str(refreshing.id) in visible_ids
    assert {item["kind"] for item in event_only.json()["items"]} == {"event"}
    assert {item["kind"] for item in market_only.json()["items"]} == {"asset"}
    assert detail.status_code == 200
    assert detail.json()["event"]["headline"] == "completed macro"
    assert detail.json()["report"]["impacts"][0]["target_name"] == "Energy sector"
    events_by_title = {
        item["title"]: item for item in response.json()["items"] if item["kind"] == "event"
    }
    assert events_by_title["completed macro"]["report"]["direction_score"] == -75
    assert events_by_title["completed macro"]["report"]["rating"] == "strongly_bearish"
    assert events_by_title["tied macro"]["report"]["direction_score"] == 70
    assert events_by_title["tied macro"]["report"]["rating"] == "strongly_bullish"
    assert events_by_title["insufficient macro"]["report"]["direction_score"] is None
    assert events_by_title["refreshing macro"]["refresh"] == {
        "status": "running",
        "stage": "asset_mapping",
        "error": None,
    }
    assert events_by_title["insufficient macro"]["report"]["rating"] is None
    assert first.json()["items"][0]["id"] != second.json()["items"][0]["id"]


def test_research_conclusions_exclude_retryable_fallbacks(db):
    now = utc_now()
    asset_item, asset_run = recommendation(
        asset("RETRYABLE"),
        rating=Rating.WATCH,
        score=0,
        as_of=now,
    )
    asset_run.status = RunStatus.INSUFFICIENT_EVIDENCE
    asset_run.retryable_reason = "model_LlmError"
    save_run(db, asset_run)
    save_recommendation(db, asset_item)

    event_item = event_run(
        db,
        "retryable fallback event",
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        impacts=[],
        retryable_reason="model_LlmError",
    )

    with TestClient(app) as client:
        conclusions = client.get("/api/v1/research-conclusions?limit=10")
        failures = client.get("/api/v1/failed-research-runs?limit=10")

    assert conclusions.status_code == 200
    assert failures.status_code == 200
    conclusion_ids = {item["id"] for item in conclusions.json()["items"]}
    failure_ids = {item["id"] for item in failures.json()}
    assert str(asset_item.id) not in conclusion_ids
    assert str(event_item.id) not in conclusion_ids
    assert {str(asset_run.id), str(event_item.id)} <= failure_ids


def test_research_conclusions_stop_after_the_requested_page(db, monkeypatch):
    now = utc_now()
    for index in range(4):
        item, run = recommendation(
            asset(f"PAGE{index}"),
            rating=Rating.WATCH,
            score=0,
            as_of=now - timedelta(minutes=index),
        )
        save_run(db, run)
        save_recommendation(db, item)
    db.add(
        RecommendationRow(
            id=uuid4(),
            run_id=uuid4(),
            asset_id="equity:test:BROKEN",
            score=0,
            rating=Rating.WATCH.value,
            confidence=0,
            as_of=now - timedelta(days=1),
            payload={"broken": True},
        )
    )
    db.commit()
    monkeypatch.setattr(
        "backend.app.api_integrations._CONCLUSION_SCAN_BATCH_SIZE", 2
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/research-conclusions?kind=asset&limit=1")

    assert response.status_code == 200
    assert len(response.json()["items"]) == 1
    assert response.json()["next_cursor"] is not None


def test_target_changes_split_macro_and_assets_and_link_latest_research(db):
    now = utc_now()
    equity = asset("SECURITY")
    first_asset, first_run = recommendation(
        equity,
        rating=Rating.WATCH,
        score=0,
        as_of=now - timedelta(minutes=3),
    )
    changed_asset, changed_run = recommendation(
        equity,
        rating=Rating.BULLISH,
        score=40,
        as_of=now - timedelta(minutes=2),
    )
    latest_asset, latest_run = recommendation(
        equity,
        rating=Rating.BULLISH,
        score=55,
        as_of=now - timedelta(minutes=1),
    )
    for item, run in (
        (first_asset, first_run),
        (changed_asset, changed_run),
        (latest_asset, latest_run),
    ):
        save_run(db, run)
        save_recommendation(db, item)

    oil = asset("CLUSD", asset_class=AssetClass.COMMODITY)
    for item, run in (
        recommendation(
            oil,
            rating=Rating.WATCH,
            score=0,
            as_of=now - timedelta(minutes=12),
        ),
        recommendation(
            oil,
            rating=Rating.BEARISH,
            score=-35,
            as_of=now - timedelta(minutes=11),
        ),
        recommendation(
            oil,
            rating=Rating.BEARISH,
            score=-40,
            as_of=now - timedelta(minutes=10),
        ),
    ):
        save_run(db, run)
        save_recommendation(db, item)
    event_run(
        db,
        "macro baseline",
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        impacts=[
            impact(
                "WTI Crude Oil Continuous Benchmark",
                target_type=TargetType.COMMODITY_PRICE,
                rating=Rating.WATCH,
                score=0,
                target_asset=oil,
            ),
            impact(
                "SECURITY Research Target (SECURITY)",
                target_type=TargetType.ECONOMY,
                rating=Rating.BEARISH,
                score=-40,
                target_asset=equity,
            ),
        ],
    )
    changed_commodity = event_run(
        db,
        "macro changed",
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        impacts=[
            impact(
                "WTI Crude Oil (CLUSD)",
                target_type=TargetType.COMMODITY_PRICE,
                rating=Rating.BEARISH,
                score=-45,
            )
        ],
    )
    latest_commodity = event_run(
        db,
        "macro latest",
        status=RunStatus.INSUFFICIENT_EVIDENCE,
        impacts=[
            impact(
                "WTI Crude Oil",
                target_type=TargetType.COMMODITY_PRICE,
                rating=Rating.BEARISH,
                score=-55,
                target_asset=oil,
            )
        ],
    )

    with TestClient(app) as client:
        macro = client.get("/api/v1/target-changes?kind=macro&limit=50")
        assets = client.get("/api/v1/target-changes?kind=asset&limit=50")

    assert macro.status_code == 200
    macro_items = macro.json()["items"]
    assert macro_items == []
    assert all("SECURITY" not in item["label"] for item in macro_items)
    assert assets.status_code == 200
    asset_items = assets.json()["items"]
    assert len(asset_items) == 2
    items_by_type = {item["target_type"]: item for item in asset_items}
    security_item = items_by_type["tradable_asset"]
    assert security_item["current"]["rating"] == "bullish"
    assert security_item["latest"]["direction_score"] == 55
    assert security_item["latest"]["news_confidence"] == 0.72
    assert security_item["latest_detail"]["id"] == str(latest_asset.id)
    commodity_item = items_by_type["commodity_price"]
    assert commodity_item["kind"] == "asset"
    assert commodity_item["previous"]["rating"] == "watch"
    assert commodity_item["current"]["rating"] == "bearish"
    assert commodity_item["latest"]["direction_score"] == -55
    assert commodity_item["latest"]["news_confidence"] == 0.67
    assert commodity_item["change_detail_id"] == str(changed_commodity.id)
    assert commodity_item["latest_detail"]["kind"] == "event"
    assert commodity_item["latest_detail"]["id"] == str(latest_commodity.id)
    assert commodity_item["trend"]["algorithm_version"] == "dual-horizon-v1"


def test_macro_change_keeps_published_report_visible_during_refresh(db):
    event_run(
        db,
        "macro refresh baseline",
        status=RunStatus.COMPLETED,
        impacts=[
            impact(
                "能源行业",
                target_type=TargetType.SECTOR,
                rating=Rating.WATCH,
                score=0,
            )
        ],
    )
    refreshing = event_run(
        db,
        "macro refresh published",
        status=RunStatus.COMPLETED,
        impacts=[
            impact(
                "能源行业",
                target_type=TargetType.SECTOR,
                rating=Rating.BEARISH,
                score=-48,
            )
        ],
    )
    refreshing.report_history.append(refreshing.report.model_copy(deep=True))
    refreshing.status = RunStatus.RUNNING
    save_event_research_run(db, refreshing)

    with TestClient(app) as client:
        response = client.get("/api/v1/target-changes?kind=macro&limit=50")

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["label"] == "能源"
    assert items[0]["current"]["rating"] == "bearish"
    assert items[0]["latest_detail"]["id"] == str(refreshing.id)
