from datetime import timedelta

from fastapi.testclient import TestClient

from backend.app.domain import (
    EventReport,
    EventResearchRun,
    EventType,
    NewsEvent,
    Rating,
    RunStatus,
    SourceQuality,
    TargetImpact,
    TargetType,
    TransmissionFactors,
    utc_now,
)
from backend.app.main import app
from backend.app.storage import save_event, save_event_research_run


def _impact(
    name: str,
    score: int,
    *,
    target_type: TargetType = TargetType.SECTOR,
    confidence: float = 0.8,
) -> TargetImpact:
    rating = Rating.BULLISH if score >= 30 else Rating.BEARISH
    return TargetImpact(
        target_type=target_type,
        target_name=name,
        direction=1 if score > 0 else -1,
        score=score / 100,
        direction_score=score,
        rating=rating,
        confidence=confidence,
        rating_confidence=confidence,
        factors=TransmissionFactors(
            persistence=0.8,
            realization_probability=0.8,
        ),
    )


def _event_run(
    db,
    name: str,
    *,
    published_at,
    impacts: list[TargetImpact],
    status: RunStatus = RunStatus.COMPLETED,
    updated_at=None,
) -> EventResearchRun:
    event = NewsEvent(
        news_item_ids=[],
        headline=name,
        event_type=EventType.MACRO,
        direct_impact=f"{name} direct impact",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=published_at,
        observed_at=published_at,
        as_of=published_at,
    )
    save_event(db, event)
    run = EventResearchRun(
        event_id=event.id,
        status=status,
        as_of=published_at,
        updated_at=updated_at or published_at,
        report=EventReport(
            summary=f"{name} report",
            confidence=0.8,
            news_confidence=0.85,
            evidence_complete=status is RunStatus.COMPLETED,
            impacts=impacts,
        ),
    )
    save_event_research_run(db, run)
    return run


def test_macro_targets_merge_aliases_and_keep_low_confidence_shock_short_term(db):
    now = utc_now()
    aliases = [
        ("Digital Assets", TargetType.SECTOR),
        ("Cryptocurrency Market", TargetType.ECONOMY),
        ("数字资产", TargetType.SECTOR),
        ("加密货币", TargetType.SECTOR),
        ("Global Cryptocurrency Market Sentiment", TargetType.ECONOMY),
    ]
    for index, ((name, target_type), days_ago) in enumerate(
        zip(aliases, (25, 20, 15, 10, 8), strict=True)
    ):
        impacts = [_impact(name, 69, target_type=target_type)]
        if index == 0:
            impacts.append(_impact("Cryptocurrency", 69))
        _event_run(
            db,
            f"positive-{index}",
            published_at=now - timedelta(days=days_ago),
            impacts=impacts,
        )
    shock = _event_run(
        db,
        "low-confidence shock",
        published_at=now,
        impacts=[_impact("加密货币市场", -50, confidence=0.3)],
        status=RunStatus.INSUFFICIENT_EVIDENCE,
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/target-changes?kind=macro&limit=50")

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    item = items[0]
    assert item["key"] == "sector:digital_assets"
    assert item["label"] == "数字资产"
    assert item["target_type"] == "sector"
    assert item["current"]["rating"] == "bearish"
    assert item["current"]["provisional"] is True
    assert item["latest_detail"]["id"] == str(shock.id)
    assert item["trend"]["event_count_90d"] == 6
    assert item["trend"]["eligible_event_count_90d"] == 5
    assert item["trend"]["ignored_event_count_90d"] == 1
    assert item["trend"]["long_term"]["rating"] == "bullish"
    assert item["trend"]["short_term"]["rating"] == "bearish"
    assert item["trend"]["short_term"]["provisional"] is True
    assert item["trend"]["composite"]["rating"] == "bullish"


def test_short_term_window_uses_event_publication_not_rerun_time(db):
    now = utc_now()
    _event_run(
        db,
        "old baseline",
        published_at=now - timedelta(days=20),
        impacts=[_impact("Digital Assets", 60)],
    )
    rerun = _event_run(
        db,
        "old negative news rerun",
        published_at=now - timedelta(days=8),
        updated_at=now,
        impacts=[_impact("加密货币", -60)],
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/target-changes?kind=macro&limit=50")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["latest_detail"]["id"] == str(rerun.id)
    assert item["trend"]["short_term"]["direction_score"] == 0
    assert item["trend"]["short_term"]["rating"] == "watch"
