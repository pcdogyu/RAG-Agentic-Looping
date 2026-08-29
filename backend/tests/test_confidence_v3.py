import inspect
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from backend.app.domain import (
    ActionStage,
    CandidateAsset,
    EventAction,
    EventType,
    Evidence,
    NewsEvent,
    NewsItem,
    Rating,
    SourceQuality,
)
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.confidence_v3 import (
    event_horizon_days,
    news_confidence_score,
    rating_confidence_score,
    rating_for_direction_score,
    select_news_confidence_evidence,
)
from backend.app.services.macro_impacts import ModelEventImpactDraft
from backend.app.services.research import ModelDraftOutput

OBSERVED = datetime(2026, 8, 29, 8, tzinfo=UTC)


def _event(
    *,
    event_type: EventType = EventType.REGULATION,
    stage: ActionStage = ActionStage.ANNOUNCED,
) -> NewsEvent:
    return NewsEvent(
        news_item_ids=[],
        headline="监管机构正式宣布新规则",
        event_type=event_type,
        direct_impact="规则直接影响目标公司的产品收入。",
        source_quality=SourceQuality.OFFICIAL,
        published_at=OBSERVED,
        observed_at=OBSERVED,
        as_of=OBSERVED,
        actions=[
            EventAction(
                actor="监管机构",
                action_type="regulation",
                action_stage=stage,
                action="正式宣布",
                object="新规则",
                scope="目标行业",
            )
        ],
    )


def _evidence(
    *,
    source_quality: SourceQuality = SourceQuality.OFFICIAL,
    group: str = "official:regulator",
    claim: str = "监管机构正式宣布新规则",
    numeric_value: float | None = None,
) -> Evidence:
    return Evidence(
        run_id=uuid4(),
        claim=claim,
        source_name=group,
        source_url=f"https://{group.replace(':', '-')}.example/rule",
        source_quality=source_quality,
        published_at=OBSERVED,
        observed_at=OBSERVED,
        as_of=OBSERVED,
        excerpt=claim,
        independent_group=group,
        numeric_value=numeric_value,
    )


@pytest.mark.parametrize(
    ("score", "rating"),
    [
        (-100, Rating.STRONGLY_BEARISH),
        (-70, Rating.STRONGLY_BEARISH),
        (-69, Rating.BEARISH),
        (-30, Rating.BEARISH),
        (-29, Rating.WATCH),
        (0, Rating.WATCH),
        (29, Rating.WATCH),
        (30, Rating.BULLISH),
        (69, Rating.BULLISH),
        (70, Rating.STRONGLY_BULLISH),
        (100, Rating.STRONGLY_BULLISH),
    ],
)
def test_v3_rating_boundaries(score, rating):
    assert rating_for_direction_score(score) is rating


@pytest.mark.parametrize(
    ("event_type", "days"),
    [
        (EventType.EARNINGS, 30),
        (EventType.SECURITY, 30),
        (EventType.M_AND_A, 180),
        (EventType.PRODUCT, 90),
        (EventType.REGULATION, 90),
        (EventType.MANAGEMENT, 90),
        (EventType.MACRO, 90),
        (EventType.SUPPLY_CHAIN, 90),
        (EventType.TOKENOMICS, 90),
        (EventType.OTHER, 90),
    ],
)
def test_event_type_maps_to_calendar_day_horizon(event_type, days):
    assert event_horizon_days(event_type) == days


def test_news_confidence_uses_five_system_factors():
    event = _event(stage=ActionStage.REALIZED)
    evidence = _evidence()
    item = NewsItem(
        source="Regulator",
        source_quality=SourceQuality.OFFICIAL,
        title=event.headline,
        summary=event.direct_impact,
        url=evidence.source_url,
        published_at=OBSERVED,
        observed_at=OBSERVED,
        as_of=OBSERVED,
        content_hash="official-rule",
    )

    result = news_confidence_score(event, [evidence], [item])

    assert result.confidence == 0.94
    assert result.factors.source_reliability.value == 1
    assert result.factors.originality.value == 1
    assert result.factors.cross_verification.value == 0.7
    assert result.factors.clarity.value == 1
    assert result.factors.timeliness_completeness.value == 1


def test_reprinted_story_counts_as_one_independent_source():
    event = _event()
    first = _evidence(
        source_quality=SourceQuality.PROFESSIONAL,
        group="publisher-a",
        claim="Wire copy of the same report",
    )
    second = first.model_copy(
        update={
            "id": uuid4(),
            "run_id": uuid4(),
            "source_name": "publisher-b",
            "source_url": "https://publisher-b.example/rule",
            "independent_group": "publisher-b",
        }
    )

    result = news_confidence_score(event, [first, second])

    assert result.factors.cross_verification.value == 0.5
    assert "1 个独立来源组" in result.factors.cross_verification.reason


def test_news_confidence_inputs_include_verification_but_exclude_research_data():
    event_story = _evidence(group="publisher:event")
    event_item = NewsItem(
        source="Event publisher",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Event story",
        summary="Event summary",
        url=event_story.source_url,
        published_at=OBSERVED,
        observed_at=OBSERVED,
        as_of=OBSERVED,
        content_hash="event-story",
    )
    web_verification = _evidence(group="web:verification.example")
    fundamentals = _evidence(group="fmp", claim="Revenue increased")
    market = _evidence(group="market:price", claim="Price increased")

    selected = select_news_confidence_evidence(
        [event_story, web_verification, fundamentals, market],
        [event_item],
    )

    assert {item.id for item in selected} == {event_story.id, web_verification.id}


def test_rating_confidence_caps_small_history_and_zeroes_missing_market_data():
    event = _event()
    evidence = [_evidence(numeric_value=12), _evidence(group="filing:issuer", numeric_value=8)]
    candidate = CandidateAsset(
        asset=SEED_ASSETS[0],
        relationship="direct",
        relevance=1,
        mapping_confidence=0.95,
        rationale="新闻直接点名发行人。",
    )

    result = rating_confidence_score(
        direction_score=70,
        event=event,
        candidate=candidate,
        transmission_path=["新规则", "产品收入", "利润"],
        cited_evidence_ids=[item.id for item in evidence],
        evidence=evidence,
        historical_reactions=[1, 1],
    )

    assert result.mapping_distance == 0
    assert result.factors.mapping_strength.value == 0.95
    assert result.factors.causality_certainty.value == 1
    assert result.factors.historical_pattern.value == pytest.approx(2 / 3, abs=0.0001)
    assert result.factors.market_consistency.value == 0


def test_rating_market_consistency_uses_only_matching_calendar_horizon_factor():
    event = _event()
    evidence = [_evidence()]
    candidate = CandidateAsset(
        asset=SEED_ASSETS[0],
        relationship="direct",
        relevance=1,
        mapping_confidence=1,
        rationale="新闻直接点名发行人。",
    )
    factor_summary = {
        "categories": {"market_reaction": -1},
        "category_reliability": {"market_reaction": 1},
        "factors": [
            {
                "key": "asset_return_1d_pct",
                "normalized_signal": -1,
                "reliability": 1,
            },
            {
                "key": "asset_return_horizon_90cd_pct",
                "normalized_signal": 1,
                "reliability": 0.8,
            },
        ],
    }

    result = rating_confidence_score(
        direction_score=70,
        event=event,
        candidate=candidate,
        transmission_path=["新规则", "产品收入", "利润"],
        cited_evidence_ids=[evidence[0].id],
        evidence=evidence,
        factor_summary=factor_summary,
    )

    assert result.factors.market_consistency.value == 0.68


def test_news_and_rating_confidence_are_independent():
    event = _event()
    official = _evidence()
    social = official.model_copy(
        update={"source_quality": SourceQuality.SOCIAL, "independent_group": "social:post"}
    )
    candidate = CandidateAsset(
        asset=SEED_ASSETS[0],
        relationship="direct",
        relevance=0.9,
        mapping_confidence=0.9,
        rationale="新闻直接点名发行人。",
    )

    official_news = news_confidence_score(event, [official]).confidence
    social_news = news_confidence_score(event, [social]).confidence
    official_rating = rating_confidence_score(
        direction_score=40,
        event=event,
        candidate=candidate,
        transmission_path=["规则", "收入"],
        cited_evidence_ids=[official.id],
        evidence=[official],
    ).confidence
    social_rating = rating_confidence_score(
        direction_score=40,
        event=event,
        candidate=candidate,
        transmission_path=["规则", "收入"],
        cited_evidence_ids=[social.id],
        evidence=[social],
    ).confidence

    assert official_news > social_news
    assert official_rating == social_rating
    assert "news_confidence" not in inspect.signature(rating_confidence_score).parameters


def test_llm_schemas_expose_direction_score_without_confidence_or_rating():
    draft_properties = ModelDraftOutput.model_json_schema()["properties"]
    target_properties = ModelEventImpactDraft.model_json_schema()["$defs"][
        "ModelTargetImpactDraft"
    ]["properties"]
    forbidden = {"rating", "confidence", "news_confidence", "rating_confidence"}

    assert "direction_score" in draft_properties
    assert "direction_score" in target_properties
    assert forbidden.isdisjoint(draft_properties)
    assert forbidden.isdisjoint(target_properties)
    with pytest.raises(ValidationError):
        ModelDraftOutput.model_validate(
            {"summary": "test", "direction_score": 10, "confidence": 0.9}
        )
