import pytest

from backend.app.domain import Rating, rating_for


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
