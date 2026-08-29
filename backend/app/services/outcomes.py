from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite, sqrt
from typing import Any, Literal

from sqlalchemy.orm import Session

from backend.app.domain import (
    HorizonUnit,
    Market,
    Outcome,
    Recommendation,
    SignalStatus,
    as_utc,
    utc_now,
)
from backend.app.providers.registry import ProviderRegistry
from backend.app.storage import list_outcomes, list_recommendations, save_outcome


@dataclass(frozen=True)
class PricePoint:
    observed_at: datetime
    close: float


@dataclass(frozen=True)
class BenchmarkResult:
    status: Literal["available", "missing", "self_benchmark"]
    return_value: float | None = None


class OutcomeService:
    """Evaluate each recommendation at the horizon it actually declared.

    Legacy recommendations use a calendar-day forecast horizon. New short-term
    recommendations use trading sessions and are evaluated at the third close
    after the entry observation, so weekends and exchange holidays do not shorten
    the requested market window.
    """

    benchmark_symbols = {
        Market.US: "SPY",
        Market.CN: "000300",
        Market.HK: "HSI",
        Market.CRYPTO: "BTC",
    }

    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    @staticmethod
    def _close(item: dict[str, Any]) -> float | None:
        for key in ("close", "adjClose", "price", "收盘"):
            value = item.get(key)
            if isinstance(value, bool) or value is None:
                continue
            try:
                close = float(value)
            except (TypeError, ValueError):
                continue
            if isfinite(close):
                return close
        return None

    @staticmethod
    def _timestamp(item: dict[str, Any]) -> datetime | None:
        value = next(
            (
                item[key]
                for key in ("date", "datetime", "timestamp", "time", "日期")
                if item.get(key) is not None
            ),
            None,
        )
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, datetime):
            return as_utc(value)
        if isinstance(value, date):
            return datetime.combine(value, time.min, tzinfo=UTC)
        if isinstance(value, int | float):
            seconds = float(value) / 1000 if abs(float(value)) >= 100_000_000_000 else float(value)
            try:
                return datetime.fromtimestamp(seconds, UTC)
            except (OverflowError, OSError, ValueError):
                return None
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                try:
                    numeric = float(raw)
                except ValueError:
                    return None
                seconds = numeric / 1000 if abs(numeric) >= 100_000_000_000 else numeric
                try:
                    return datetime.fromtimestamp(seconds, UTC)
                except (OverflowError, OSError, ValueError):
                    return None
            return as_utc(parsed)
        return None

    @classmethod
    def _price_points(
        cls,
        prices: list[dict[str, Any]],
        *,
        not_after: datetime,
    ) -> list[PricePoint]:
        by_timestamp: dict[datetime, PricePoint] = {}
        boundary = as_utc(not_after)
        for item in prices:
            observed_at = cls._timestamp(item)
            close = cls._close(item)
            if observed_at is None or close is None or close <= 0 or observed_at > boundary:
                continue
            by_timestamp[observed_at] = PricePoint(observed_at=observed_at, close=close)
        return sorted(by_timestamp.values(), key=lambda item: item.observed_at)

    @staticmethod
    def _window(
        points: list[PricePoint], *, start: datetime, target: datetime
    ) -> list[PricePoint]:
        start = as_utc(start)
        target = as_utc(target)
        entry_index = next(
            (index for index, point in enumerate(points) if point.observed_at >= start),
            None,
        )
        if entry_index is None:
            return []
        exit_index = next(
            (
                index
                for index, point in enumerate(points[entry_index + 1 :], start=entry_index + 1)
                if point.observed_at >= target
            ),
            None,
        )
        if exit_index is None:
            return []
        return points[entry_index : exit_index + 1]

    @staticmethod
    def _trading_session_window(
        points: list[PricePoint], *, start: datetime, sessions: int
    ) -> list[PricePoint]:
        """Return entry plus the requested number of subsequent market closes."""

        start = as_utc(start)
        entry_index = next(
            (index for index, point in enumerate(points) if point.observed_at >= start),
            None,
        )
        if entry_index is None:
            return []
        exit_index = entry_index + sessions
        if exit_index >= len(points):
            return []
        return points[entry_index : exit_index + 1]

    def evaluate_due(self, db: Session) -> list[Outcome]:
        existing = {(item.recommendation_id, item.horizon_days) for item in list_outcomes(db)}
        now = utc_now()
        created: list[Outcome] = []
        batch_size = 500
        offset = 0
        while True:
            recommendations = list_recommendations(
                db,
                limit=batch_size,
                offset=offset,
                oldest_first=True,
            )
            if not recommendations:
                break
            for recommendation in recommendations:
                horizon = recommendation.horizon_days
                if (recommendation.id, horizon) in existing:
                    continue
                if (
                    recommendation.horizon_unit is HorizonUnit.CALENDAR_DAYS
                    and as_utc(recommendation.as_of) + timedelta(days=horizon) > now
                ):
                    continue
                outcome = self._evaluate(recommendation, horizon, observed_at=now)
                if outcome:
                    save_outcome(db, outcome)
                    created.append(outcome)
                    existing.add((recommendation.id, horizon))
            offset += len(recommendations)
        return created

    def _evaluate(
        self,
        recommendation: Recommendation,
        horizon: int,
        *,
        observed_at: datetime | None = None,
    ) -> Outcome | None:
        if recommendation.signal_status in {
            SignalStatus.TECHNICAL_FAILURE,
            SignalStatus.INSUFFICIENT_EVIDENCE,
        }:
            return None
        observed_at = as_utc(observed_at or utc_now())
        start = as_utc(recommendation.as_of)
        target = start + timedelta(days=horizon)
        if (
            recommendation.horizon_unit is HorizonUnit.CALENDAR_DAYS
            and target > observed_at
        ):
            return None

        provider = self.registry.provider_for(recommendation.asset)
        prices = provider.get_prices(recommendation.asset, start=start, end=observed_at)
        points = self._price_points(prices, not_after=observed_at)
        window = (
            self._trading_session_window(points, start=start, sessions=horizon)
            if recommendation.horizon_unit is HorizonUnit.TRADING_SESSIONS
            else self._window(points, start=start, target=target)
        )
        if len(window) < 2:
            return None

        entry = window[0]
        exit_point = window[-1]
        raw_return = exit_point.close / entry.close - 1
        benchmark = self._benchmark_return(
            recommendation,
            entry_at=entry.observed_at,
            exit_at=exit_point.observed_at,
            observed_at=observed_at,
        )
        benchmark_return = benchmark.return_value
        alpha = raw_return - benchmark_return if benchmark_return is not None else None

        neutral_band = self._neutral_band(horizon)
        actual = (1.0, 0.0, 0.0) if raw_return > neutral_band else (0.0, 0.0, 1.0)
        if -neutral_band <= raw_return <= neutral_band:
            actual = (0.0, 1.0, 0.0)
        predicted = (
            recommendation.bull_probability,
            recommendation.base_probability,
            recommendation.bear_probability,
        )
        brier = sum(
            (forecast - realized) ** 2
            for forecast, realized in zip(predicted, actual, strict=True)
        ) / 3
        neutral_threshold = 30 if recommendation.scoring_version == "llm-direction-v3" else 15
        if abs(recommendation.direction_score or recommendation.score) < neutral_threshold:
            direction_correct = abs(raw_return) <= neutral_band
        elif (recommendation.direction_score or recommendation.score) > 0:
            direction_correct = raw_return > neutral_band
        else:
            direction_correct = raw_return < -neutral_band

        peak = entry.close
        max_drawdown = 0.0
        for point in window:
            peak = max(peak, point.close)
            max_drawdown = min(max_drawdown, point.close / peak - 1)
        return Outcome(
            recommendation_id=recommendation.id,
            horizon_days=horizon,
            entry_at=entry.observed_at,
            exit_at=exit_point.observed_at,
            entry_price=entry.close,
            exit_price=exit_point.close,
            raw_return=raw_return,
            benchmark_status=benchmark.status,
            benchmark_return=benchmark_return,
            alpha=alpha,
            direction_correct=direction_correct,
            brier_score=brier,
            max_drawdown=max_drawdown,
            observed_at=observed_at,
        )

    @staticmethod
    def _neutral_band(horizon: int) -> float:
        """Scale the old 2%/20-day neutral band by square-root time."""

        return min(0.10, max(0.005, 0.02 * sqrt(horizon / 20)))

    def _benchmark_return(
        self,
        recommendation: Recommendation,
        *,
        entry_at: datetime,
        exit_at: datetime,
        observed_at: datetime,
    ) -> BenchmarkResult:
        benchmark_symbol = self.benchmark_symbols.get(recommendation.asset.market)
        if not benchmark_symbol:
            return BenchmarkResult(status="missing")
        matches = self.registry.resolve_assets(benchmark_symbol)
        exact_matches = [asset for asset in matches if asset.symbol.upper() == benchmark_symbol]
        if not exact_matches:
            return BenchmarkResult(status="missing")
        asset = exact_matches[0]
        if asset.asset_id == recommendation.asset.asset_id:
            return BenchmarkResult(status="self_benchmark")

        prices = self.registry.provider_for(asset).get_prices(
            asset,
            start=entry_at,
            end=observed_at,
        )
        points = self._price_points(prices, not_after=observed_at)
        window = self._window(points, start=entry_at, target=exit_at)
        if len(window) < 2:
            return BenchmarkResult(status="missing")
        return BenchmarkResult(
            status="available",
            return_value=window[-1].close / window[0].close - 1,
        )
