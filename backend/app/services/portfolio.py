from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AssetClass,
    AssetRef,
    Market,
    OrderSide,
    PaperOrder,
    PortfolioSnapshot,
    Position,
    Rating,
    Recommendation,
    utc_now,
)
from backend.app.providers.registry import ProviderRegistry
from backend.app.storage import list_orders, save_order


class PortfolioError(ValueError):
    pass


class PortfolioService:
    def __init__(self, registry: ProviderRegistry, settings: Settings | None = None) -> None:
        self.registry = registry
        self.settings = settings or get_settings()

    def current_price(self, recommendation: Recommendation) -> float:
        asset = recommendation.asset
        provider = self.registry.provider_for(asset)
        if asset.asset_class is AssetClass.CRYPTO:
            prices = provider.get_prices(asset, start=utc_now(), end=utc_now())
            if prices:
                return float(prices[-1].get("price", 0))
            metrics = provider.get_crypto_metrics(asset)
            return float(metrics.get("market", {}).get("current_price", {}).get("usd", 0) or 0)
        if hasattr(provider, "get_quote"):
            quote = provider.get_quote(asset)
            return float(quote.get("price") or quote.get("previousClose") or 0)
        prices = provider.get_prices(asset)
        if not prices:
            return 0
        latest = prices[-1]
        return float(latest.get("收盘") or latest.get("close") or 0)

    def snapshot(self, db: Session, prices: dict[str, float] | None = None) -> PortfolioSnapshot:
        prices = prices or {}
        quantity: dict[str, float] = defaultdict(float)
        cost: dict[str, float] = defaultdict(float)
        assets = {}
        cash = self.settings.initial_cash
        for order in list_orders(db):
            assets[order.asset.asset_id] = order.asset
            fx = self._fx_to_usd(order.currency)
            signed = order.quantity if order.side is OrderSide.BUY else -order.quantity
            quantity[order.asset.asset_id] += signed
            cash -= signed * order.price * fx
            cash -= order.fee * fx
            if order.side is OrderSide.BUY:
                cost[order.asset.asset_id] += order.quantity * order.price * fx + order.fee * fx

        raw_positions: list[tuple[Any, float, float, float]] = []
        market_total = 0.0
        for asset_id, qty in quantity.items():
            if qty <= 0:
                continue
            asset = assets[asset_id]
            price = prices.get(asset_id, 0)
            if price <= 0:
                try:
                    dummy = Recommendation(
                        run_id="00000000-0000-0000-0000-000000000000",
                        asset=asset,
                        score=0,
                        rating=Rating.WATCH,
                        confidence=0,
                        bull_probability=0.33,
                        base_probability=0.34,
                        bear_probability=0.33,
                        thesis={"summary": ""},
                        as_of=utc_now(),
                    )
                    price = self.current_price(dummy)
                except Exception:
                    pass
                if price <= 0:
                    price = cost[asset_id] / qty / self._fx_to_usd(asset.currency)
            market_value = qty * price * self._fx_to_usd(asset.currency)
            market_total += market_value
            raw_positions.append((asset, qty, price, market_value))

        nav = cash + market_total
        positions = []
        crypto_value = 0.0
        for asset, qty, price, market_value in raw_positions:
            if asset.asset_class is AssetClass.CRYPTO:
                crypto_value += market_value
            positions.append(
                Position(
                    asset=asset,
                    quantity=qty,
                    average_cost=cost[asset.asset_id] / qty,
                    last_price=price,
                    market_value_usd=market_value,
                    unrealized_pnl_usd=market_value - cost[asset.asset_id],
                    weight=market_value / nav if nav else 0,
                )
            )
        return PortfolioSnapshot(
            cash_usd=cash,
            nav_usd=nav,
            crypto_weight=crypto_value / nav if nav else 0,
            positions=positions,
            as_of=utc_now(),
        )

    def create_from_recommendation(
        self,
        db: Session,
        recommendation: Recommendation,
        price: float,
        target_weight: float | None = None,
    ) -> PaperOrder:
        if recommendation.asset.asset_class not in {AssetClass.EQUITY, AssetClass.CRYPTO}:
            raise PortfolioError("paper execution is not supported for commodity or FX assets")
        if recommendation.scoring_version == "target-transmission-v2":
            if (
                recommendation.impact is None
                or not recommendation.impact.execution_supported
                or recommendation.impact.trade_status.value != "tradeable"
                or abs(recommendation.impact.score) < 0.25
            ):
                raise PortfolioError("target impact does not meet the v2 trading gate")
        if recommendation.rating not in {Rating.BULLISH, Rating.STRONGLY_BULLISH}:
            raise PortfolioError(
                "only bullish recommendations can open a position"
            )
        if recommendation.confidence < 0.55:
            raise PortfolioError("recommendation confidence is below 55%")
        if price <= 0:
            raise PortfolioError("price must be positive")
        portfolio = self.snapshot(db)
        asset = recommendation.asset
        max_weight = (
            self.settings.max_crypto_weight
            if asset.asset_class is AssetClass.CRYPTO
            else self.settings.max_equity_weight
        )
        weight = min(target_weight or max_weight, max_weight)
        existing_weight = next(
            (
                position.weight
                for position in portfolio.positions
                if position.asset.asset_id == asset.asset_id
            ),
            0.0,
        )
        increment_weight = max(0.0, weight - existing_weight)
        if asset.asset_class is AssetClass.CRYPTO:
            remaining_crypto = self.settings.max_total_crypto_weight - portfolio.crypto_weight
            increment_weight = min(increment_weight, max(0, remaining_crypto))
        max_cash_to_use = max(
            0,
            portfolio.cash_usd - portfolio.nav_usd * self.settings.minimum_cash_weight,
        )
        usd_to_use = min(portfolio.nav_usd * increment_weight, max_cash_to_use)
        if usd_to_use <= 0:
            raise PortfolioError("cash or asset-class risk limit reached")
        fx = self._fx_to_usd(asset.currency)
        raw_quantity = usd_to_use / (price * fx)
        quantity = self._round_quantity(asset, raw_quantity)
        if quantity <= 0:
            raise PortfolioError("target allocation is smaller than one exchange lot")
        bps = (
            self.settings.crypto_cost_bps
            if asset.asset_class is AssetClass.CRYPTO
            else self.settings.equity_cost_bps
        )
        fee = quantity * price * bps / 10_000
        order = PaperOrder(
            recommendation_id=recommendation.id,
            asset=asset,
            side=OrderSide.BUY,
            quantity=quantity,
            price=price,
            currency=asset.currency,
            fee=fee,
        )
        save_order(db, order)
        return order

    @staticmethod
    def _round_quantity(asset: AssetRef, quantity: float) -> float:
        if asset.asset_class is AssetClass.CRYPTO:
            return math.floor(quantity * 100_000_000) / 100_000_000
        lot = 100 if asset.market is Market.CN else asset.lot_size
        return math.floor(quantity / lot) * lot

    @staticmethod
    def _fx_to_usd(currency: str) -> float:
        # Conservative fallback rates. Production deployments should provide an FX adapter.
        return {"USD": 1.0, "CNY": 0.14, "HKD": 0.128}.get(currency.upper(), 1.0)
