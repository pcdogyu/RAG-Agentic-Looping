from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.app.domain import Rating, TargetType
from backend.app.services.target_trends import (
    CanonicalTarget,
    TargetObservation,
    aggregate_target_trend,
    canonicalize_target,
    rating_for_score,
)

NOW = datetime(2026, 8, 30, 8, tzinfo=UTC)


def observation(
    score: float,
    *,
    days_ago: float = 0,
    rating_confidence: float = 1,
    news_confidence: float = 1,
    persistence: float = 0.5,
    realization_probability: float = 0.5,
    insufficient_evidence: bool = False,
    provisional: bool = False,
) -> TargetObservation:
    return TargetObservation(
        occurred_at=NOW - timedelta(days=days_ago),
        score=score,
        rating_confidence=rating_confidence,
        news_confidence=news_confidence,
        persistence=persistence,
        realization_probability=realization_probability,
        insufficient_evidence=insufficient_evidence,
        provisional=provisional,
    )


@pytest.mark.parametrize(
    "name",
    [
        "Digital Assets",
        "Cryptocurrency",
        "数字资产",
        "加密货币",
        "Global Cryptocurrency Market Sentiment",
        "全球加密货币市场情绪",
        "Digital Assets - Cryptocurrency",
    ],
)
def test_digital_asset_aliases_share_one_canonical_sector(name: str):
    assert canonicalize_target(name, TargetType.SECTOR) == CanonicalTarget(
        key="sector:digital_assets",
        label="数字资产",
        target_type="sector",
        matched_taxonomy=True,
    )


def test_misclassified_crypto_economy_target_uses_same_canonical_sector():
    target = canonicalize_target("Cryptocurrency Market", TargetType.ECONOMY)
    assert target.key == "sector:digital_assets"
    assert target.target_type == "sector"


@pytest.mark.parametrize("name", ["Cryptocurrency Exchanges", "加密贷款市场"])
def test_narrow_crypto_industries_are_not_fuzzily_merged(name: str):
    target = canonicalize_target(name, TargetType.SECTOR)
    assert target.key != "sector:digital_assets"
    assert target.matched_taxonomy is False


def test_other_bilingual_industries_reuse_master_taxonomy():
    english = canonicalize_target("Global Semiconductor Sector", TargetType.SECTOR)
    chinese = canonicalize_target("全球半导体行业", TargetType.OTHER)
    assert english.key == chinese.key == "industry:semiconductors"
    assert english.label == chinese.label == "半导体"


def test_non_security_asset_id_takes_precedence():
    target = canonicalize_target(
        "Gold Market",
        TargetType.COMMODITY_PRICE,
        asset_id="commodity:gold",
        asset_class="commodity",
    )
    assert target.key == "commodity:gold"


def test_non_industry_macro_type_does_not_use_industry_taxonomy():
    target = canonicalize_target("Oil Market", TargetType.COMMODITY_PRICE)
    assert target.key == "commodity_price:oilmarket"
    assert target.matched_taxonomy is False


def test_low_confidence_event_is_short_term_only():
    trend = aggregate_target_trend(
        [
            observation(
                -80,
                rating_confidence=0.3,
                news_confidence=0.6,
                insufficient_evidence=True,
            )
        ],
        as_of=NOW,
    )
    assert trend.short_term.score == -80
    assert trend.short_term.rating is Rating.STRONGLY_BEARISH
    assert trend.short_term.provisional is True
    assert trend.long_term.score == 0
    assert trend.long_term.provisional is True
    assert trend.long_term.eligible_event_count == 0
    assert trend.long_term.ignored_event_count == 1


def test_one_ordinary_event_cannot_move_long_term_more_than_twenty_points():
    trend = aggregate_target_trend([observation(100)], as_of=NOW)
    assert 0 < trend.long_term.score <= 20
    assert trend.long_term.regime_break is False


def test_multiple_independent_events_accumulate_long_term_movement():
    trend = aggregate_target_trend(
        [observation(69, days_ago=2), observation(69, days_ago=1), observation(69)],
        as_of=NOW,
    )
    assert 20 < trend.long_term.score <= 60
    assert trend.long_term.eligible_event_count == 3


def test_regime_break_can_move_long_term_up_to_forty_five_points():
    trend = aggregate_target_trend(
        [
            observation(
                -100,
                rating_confidence=1,
                news_confidence=1,
                persistence=1,
                realization_probability=1,
            )
        ],
        as_of=NOW,
    )
    assert trend.long_term.score == -45
    assert trend.long_term.regime_break is True


def test_combined_score_is_eighty_percent_long_and_twenty_percent_short():
    trend = aggregate_target_trend(
        [observation(60, days_ago=20), observation(-60)], as_of=NOW
    )
    assert trend.combined.score == pytest.approx(
        0.8 * trend.long_term.score + 0.2 * trend.short_term.score,
        abs=0.01,
    )


def test_combined_equals_long_term_when_there_is_no_current_shock():
    trend = aggregate_target_trend([observation(60, days_ago=20)], as_of=NOW)
    assert trend.short_term.event_count == 0
    assert trend.combined.score == trend.long_term.score


def test_event_time_controls_seven_and_ninety_day_windows():
    trend = aggregate_target_trend(
        [
            observation(60, days_ago=1),
            observation(-60, days_ago=8),
            observation(90, days_ago=91),
        ],
        as_of=NOW,
    )
    assert trend.short_term.event_count == 1
    assert trend.short_term.score == 60
    assert trend.long_term.event_count == 2


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (70, Rating.STRONGLY_BULLISH),
        (30, Rating.BULLISH),
        (29.99, Rating.WATCH),
        (-29.99, Rating.WATCH),
        (-30, Rating.BEARISH),
        (-70, Rating.STRONGLY_BEARISH),
    ],
)
def test_rating_thresholds(score: float, expected: Rating):
    assert rating_for_score(score) is expected
