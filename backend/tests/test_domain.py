from uuid import uuid4

import pytest

from backend.app.domain import (
    EventType,
    ModelDirection,
    NewsEvent,
    Rating,
    SourceQuality,
    model_direction_for,
    model_rating_for,
    rating_for,
    utc_now,
)


@pytest.mark.parametrize(
    ("score", "confidence", "complete", "expected"),
    [
        (80, 0.8, True, Rating.STRONGLY_BULLISH),
        (60, 0.8, True, Rating.STRONGLY_BULLISH),
        (59, 0.8, True, Rating.BULLISH),
        (15, 0.1, False, Rating.BULLISH),
        (14, 0.8, True, Rating.WATCH),
        (0, 0.8, True, Rating.WATCH),
        (-14, 0.8, True, Rating.WATCH),
        (-15, 0.1, False, Rating.BEARISH),
        (-59, 0.8, True, Rating.BEARISH),
        (-60, 0.8, True, Rating.STRONGLY_BEARISH),
        (-80, 0.8, True, Rating.STRONGLY_BEARISH),
        (90, 0.54, True, Rating.STRONGLY_BULLISH),
        (90, 0.9, False, Rating.STRONGLY_BULLISH),
    ],
)
def test_rating_uses_score_boundaries_without_confidence_gate(
    score, confidence, complete, expected
):
    assert rating_for(score, confidence, complete) is expected


@pytest.mark.parametrize(
    ("score", "direction", "rating"),
    [
        (80, ModelDirection.BULLISH, Rating.STRONGLY_BULLISH),
        (35, ModelDirection.BULLISH, Rating.BULLISH),
        (0, ModelDirection.NEUTRAL, Rating.WATCH),
        (-35, ModelDirection.BEARISH, Rating.BEARISH),
        (-80, ModelDirection.BEARISH, Rating.STRONGLY_BEARISH),
    ],
)
def test_ungated_model_opinion_uses_five_ratings(score, direction, rating):
    assert model_direction_for(score) is direction
    assert model_rating_for(score) is rating


@pytest.mark.parametrize("value", ["watch", "neutral", "观望", "官网"])
def test_watch_rating_accepts_display_and_common_model_aliases(value):
    assert Rating(value) is Rating.WATCH


def test_news_event_accepts_nullable_optional_collections_from_go():
    now = utc_now()
    payload = NewsEvent(
        news_item_ids=[uuid4()],
        headline="Cross-runtime event",
        event_type=EventType.OTHER,
        direct_impact="Compatibility check",
        source_quality=SourceQuality.OFFICIAL,
        published_at=now,
    ).model_dump(mode="json")
    nullable_fields = (
        "entities",
        "actions",
        "candidates",
        "industry_ids",
        "analysis_steps",
    )
    for field in nullable_fields:
        payload[field] = None

    restored = NewsEvent.model_validate(payload)

    for field in nullable_fields:
        assert getattr(restored, field) == []
