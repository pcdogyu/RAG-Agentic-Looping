from __future__ import annotations

from datetime import timedelta

from sqlalchemy.orm import Session

from backend.app.domain import AssetClass, Market, Outcome, Recommendation, utc_now
from backend.app.providers.registry import ProviderRegistry
from backend.app.storage import list_outcomes, list_recommendations, save_outcome


class OutcomeService:
    horizons = (1, 5, 20, 60, 120)

    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    @staticmethod
    def _close(item: dict) -> float | None:
        for key in ("close", "adjClose", "price", "收盘"):
            value = item.get(key)
            if isinstance(value, int | float):
                return float(value)
        return None

    def evaluate_due(self, db: Session) -> list[Outcome]:
        existing = {(item.recommendation_id, item.horizon_days) for item in list_outcomes(db)}
        now = utc_now()
        created: list[Outcome] = []
        for recommendation in list_recommendations(db, limit=1000):
            for horizon in self.horizons:
                if (recommendation.id, horizon) in existing:
                    continue
                if recommendation.as_of + timedelta(days=horizon) > now:
                    continue
                outcome = self._evaluate(recommendation, horizon)
                if outcome:
                    save_outcome(db, outcome)
                    created.append(outcome)
        return created

    def _evaluate(self, recommendation: Recommendation, horizon: int) -> Outcome | None:
        provider = self.registry.provider_for(recommendation.asset)
        end = recommendation.as_of + timedelta(days=horizon + 7)
        prices = provider.get_prices(recommendation.asset, start=recommendation.as_of, end=end)
        closes = [value for item in prices if (value := self._close(item)) is not None]
        if len(closes) < 2 or closes[0] <= 0:
            return None
        raw_return = closes[-1] / closes[0] - 1
        benchmark = self._benchmark_return(recommendation, horizon)
        actual = (1.0, 0.0, 0.0) if raw_return > 0.02 else (0.0, 0.0, 1.0)
        if -0.02 <= raw_return <= 0.02:
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
        if abs(recommendation.score) < 20:
            direction_correct = abs(raw_return) <= 0.02
        else:
            direction_correct = (recommendation.score > 0 and raw_return > 0) or (
                recommendation.score < 0 and raw_return < 0
            )
        peak = closes[0]
        max_drawdown = 0.0
        for close in closes:
            peak = max(peak, close)
            if peak:
                max_drawdown = min(max_drawdown, close / peak - 1)
        return Outcome(
            recommendation_id=recommendation.id,
            horizon_days=horizon,
            raw_return=raw_return,
            benchmark_return=benchmark,
            alpha=raw_return - benchmark,
            direction_correct=direction_correct,
            brier_score=brier,
            max_drawdown=max_drawdown,
        )

    def _benchmark_return(self, recommendation: Recommendation, horizon: int) -> float:
        # Benchmark hooks are explicit; zero is returned when a provider cannot supply the index.
        benchmark_symbol = {
            Market.US: "SPY",
            Market.CN: "000300",
            Market.HK: "HSI",
            Market.CRYPTO: "BTC",
        }[recommendation.asset.market]
        if (
            recommendation.asset.asset_class is AssetClass.CRYPTO
            and recommendation.asset.symbol == "BTC"
        ):
            return 0.0
        matches = self.registry.resolve_assets(benchmark_symbol)
        if not matches:
            return 0.0
        asset = matches[0]
        prices = self.registry.provider_for(asset).get_prices(
            asset,
            start=recommendation.as_of,
            end=recommendation.as_of + timedelta(days=horizon + 7),
        )
        closes = [value for item in prices if (value := self._close(item)) is not None]
        return closes[-1] / closes[0] - 1 if len(closes) >= 2 and closes[0] else 0.0
