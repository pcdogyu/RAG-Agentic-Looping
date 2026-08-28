import pytest

from backend.app.domain import (
    ModelDirection,
    Rating,
    model_direction_for,
    model_rating_for,
    rating_for,
)


@pytest.mark.parametrize(
    ("score", "confidence", "complete", "expected"),
    [
        (80, 0.8, True, Rating.STRONGLY_BULLISH),
        (30, 0.8, True, Rating.BULLISH),
        (0, 0.8, True, Rating.WATCH),
        (-30, 0.8, True, Rating.BEARISH),
        (-80, 0.8, True, Rating.STRONGLY_BEARISH),
        (90, 0.54, True, Rating.WATCH),
        (90, 0.9, False, Rating.WATCH),
    ],
)
def test_rating_gate(score, confidence, complete, expected):
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
