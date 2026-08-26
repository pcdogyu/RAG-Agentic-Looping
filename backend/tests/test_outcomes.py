from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from backend.app.domain import (
    AssetClass,
    AssetRef,
    Market,
    Outcome,
    Rating,
    Recommendation,
    SignalStatus,
    Thesis,
)
from backend.app.services.outcomes import OutcomeService


class StaticRegistry:
    def __init__(
        self,
        prices: dict[str, list[dict[str, Any]]],
        assets: list[AssetRef],
    ) -> None:
        self.prices = prices
        self.assets = assets
        self.calls: list[tuple[str, datetime | None, datetime | None]] = []

    def provider_for(self, asset: AssetRef) -> StaticRegistry:
        return self

    def get_prices(
        self,
        asset: AssetRef,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((asset.asset_id, start, end))
        return self.prices.get(asset.asset_id, [])

    def resolve_assets(self, query: str) -> list[AssetRef]:
        return [asset for asset in self.assets if asset.symbol.upper() == query.upper()]


def asset(
    symbol: str = "AAPL",
    *,
    asset_id: str | None = None,
    market: Market = Market.US,
    asset_class: AssetClass = AssetClass.EQUITY,
) -> AssetRef:
    return AssetRef(
        asset_id=asset_id or f"equity:XNAS:{symbol}",
        asset_class=asset_class,
        market=market,
        symbol=symbol,
        name=symbol,
        exchange_or_provider="XNAS",
    )


def recommendation(
    target: AssetRef,
    *,
    as_of: datetime = datetime(2025, 1, 1, tzinfo=UTC),
    horizon_days: int = 30,
    score: int = 40,
) -> Recommendation:
    return Recommendation(
        run_id=uuid4(),
        asset=target,
        score=score,
        rating=Rating.BULLISH,
        confidence=0.8,
        bull_probability=0.7,
        base_probability=0.2,
        bear_probability=0.1,
        horizon_days=horizon_days,
        thesis=Thesis(summary="frozen test recommendation"),
        as_of=as_of,
        evidence_complete=True,
    )


def test_outcome_uses_first_prices_at_exact_boundaries_and_ignores_later_rows():
    target = asset()
    benchmark = asset("SPY", asset_id="equity:ARCX:SPY")
    registry = StaticRegistry(
        {
            target.asset_id: [
                {"date": "2025-02-20", "close": 999},
                {"date": "2025-02-05", "close": 200},
                {"date": "2025-01-31", "close": 110},
                {"date": "2025-01-15", "close": 80},
                {"date": "2025-01-01", "close": 100},
                {"date": "2024-12-31", "close": 90},
            ],
            benchmark.asset_id: [
                {"date": "2025-02-05", "close": 100},
                {"date": "2025-01-31", "close": 210},
                {"date": "2025-01-01", "close": 200},
            ],
        },
        [target, benchmark],
    )

    outcome = OutcomeService(registry)._evaluate(
        recommendation(target),
        30,
        observed_at=datetime(2025, 2, 10, tzinfo=UTC),
    )

    assert outcome is not None
    assert outcome.entry_at == datetime(2025, 1, 1, tzinfo=UTC)
    assert outcome.exit_at == datetime(2025, 1, 31, tzinfo=UTC)
    assert outcome.entry_price == 100
    assert outcome.exit_price == 110
    assert outcome.raw_return == pytest.approx(0.10)
    assert outcome.benchmark_status == "available"
    assert outcome.benchmark_return == pytest.approx(0.05)
    assert outcome.alpha == pytest.approx(0.05)
    assert outcome.max_drawdown == pytest.approx(-0.20)
    assert outcome.direction_correct is True


def test_outcome_waits_for_first_market_price_after_target_boundary():
    target = asset()
    registry = StaticRegistry(
        {
            target.asset_id: [
                {"date": "2025-01-01", "close": 100},
                {"date": "2025-01-30", "close": 120},
            ]
        },
        [target],
    )

    outcome = OutcomeService(registry)._evaluate(
        recommendation(target),
        30,
        observed_at=datetime(2025, 2, 10, tzinfo=UTC),
    )

    assert outcome is None


def test_missing_benchmark_is_explicit_without_losing_raw_outcome():
    target = asset()
    registry = StaticRegistry(
        {
            target.asset_id: [
                {"date": "2025-01-01", "close": 100},
                {"date": "2025-01-31", "close": 103},
            ]
        },
        [target],
    )

    outcome = OutcomeService(registry)._evaluate(
        recommendation(target),
        30,
        observed_at=datetime(2025, 2, 10, tzinfo=UTC),
    )

    assert outcome is not None
    assert outcome.raw_return == pytest.approx(0.03)
    assert outcome.benchmark_status == "missing"
    assert outcome.benchmark_return is None
    assert outcome.alpha is None


def test_btc_is_marked_as_self_benchmark_instead_of_fake_zero_alpha():
    btc = asset(
        "BTC",
        asset_id="crypto:coingecko:bitcoin",
        market=Market.CRYPTO,
        asset_class=AssetClass.CRYPTO,
    )
    registry = StaticRegistry(
        {
            btc.asset_id: [
                {"timestamp": 1_735_689_600_000, "price": 100},
                {"timestamp": 1_738_281_600_000, "price": 120},
            ]
        },
        [btc],
    )

    outcome = OutcomeService(registry)._evaluate(
        recommendation(btc),
        30,
        observed_at=datetime(2025, 2, 10, tzinfo=UTC),
    )

    assert outcome is not None
    assert outcome.benchmark_status == "self_benchmark"
    assert outcome.benchmark_return is None
    assert outcome.alpha is None


def test_evaluate_due_only_uses_the_recommendation_declared_horizon(monkeypatch):
    target = asset()
    item = recommendation(
        target,
        as_of=datetime(2024, 1, 1, tzinfo=UTC),
        horizon_days=90,
    )
    service = OutcomeService(StaticRegistry({}, [target]))
    evaluated: list[int] = []
    saved: list[object] = []

    monkeypatch.setattr(
        "backend.app.services.outcomes.list_recommendations",
        lambda _db, limit, **kwargs: [item] if kwargs.get("offset") == 0 else [],
    )
    monkeypatch.setattr("backend.app.services.outcomes.list_outcomes", lambda _db: [])
    monkeypatch.setattr(
        "backend.app.services.outcomes.utc_now",
        lambda: datetime(2025, 1, 1, tzinfo=UTC),
    )

    def fake_evaluate(_recommendation, horizon, *, observed_at):
        evaluated.append(horizon)
        return SimpleNamespace(
            recommendation_id=item.id,
            horizon_days=horizon,
            observed_at=observed_at,
        )

    monkeypatch.setattr(service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(
        "backend.app.services.outcomes.save_outcome", lambda _db, outcome: saved.append(outcome)
    )

    created = service.evaluate_due(object())

    assert evaluated == [90]
    assert created == saved
    assert created[0].horizon_days == 90


def test_evaluate_due_paginates_past_the_latest_thousand(monkeypatch):
    target = asset()
    recommendations = [
        recommendation(
            target,
            as_of=datetime(2024, 1, 1, tzinfo=UTC),
            horizon_days=90,
        )
        for _ in range(1001)
    ]
    service = OutcomeService(StaticRegistry({}, [target]))
    evaluated: list[object] = []

    def paged(_db, limit, *, offset, oldest_first):
        assert oldest_first is True
        return recommendations[offset : offset + limit]

    monkeypatch.setattr("backend.app.services.outcomes.list_recommendations", paged)
    monkeypatch.setattr("backend.app.services.outcomes.list_outcomes", lambda _db: [])
    monkeypatch.setattr(
        "backend.app.services.outcomes.utc_now",
        lambda: datetime(2025, 1, 1, tzinfo=UTC),
    )

    def fake_evaluate(item, _horizon, *, observed_at):
        evaluated.append((item.id, observed_at))
        return None

    monkeypatch.setattr(service, "_evaluate", fake_evaluate)

    assert service.evaluate_due(object()) == []
    assert len(evaluated) == 1001


def test_non_signal_recommendations_do_not_enter_outcome_calibration():
    target = asset()
    registry = StaticRegistry(
        {
            target.asset_id: [
                {"date": "2025-01-01", "close": 100},
                {"date": "2025-01-31", "close": 110},
            ]
        },
        [target],
    )
    item = recommendation(target)
    item.signal_status = SignalStatus.TECHNICAL_FAILURE

    assert (
        OutcomeService(registry)._evaluate(
            item,
            30,
            observed_at=datetime(2025, 2, 10, tzinfo=UTC),
        )
        is None
    )


@pytest.mark.parametrize(
    ("horizon", "expected"),
    [(1, 0.005), (20, 0.02), (80, 0.04), (1000, 0.10)],
)
def test_neutral_band_scales_with_forecast_horizon(horizon: int, expected: float):
    assert OutcomeService._neutral_band(horizon) == pytest.approx(expected)


def test_legacy_outcome_payload_remains_readable():
    legacy = Outcome.model_validate(
        {
            "recommendation_id": str(uuid4()),
            "horizon_days": 20,
            "raw_return": 0.03,
            "benchmark_return": 0.01,
            "alpha": 0.02,
            "direction_correct": True,
            "brier_score": 0.12,
        }
    )

    assert legacy.benchmark_status == "available"
    assert legacy.entry_at is None
    assert legacy.exit_at is None
    assert legacy.entry_price is None
    assert legacy.exit_price is None
