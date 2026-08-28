from uuid import uuid4

import pytest

from backend.app.config import Settings
from backend.app.domain import Recommendation, Thesis, rating_for, utc_now
from backend.app.providers.registry import SEED_ASSETS, ProviderRegistry
from backend.app.services.portfolio import PortfolioError, PortfolioService


def recommendation(confidence=0.8, complete=True):
    asset = SEED_ASSETS[0]
    score = 70
    return Recommendation(
        run_id=uuid4(),
        asset=asset,
        score=score,
        rating=rating_for(score, confidence, complete),
        confidence=confidence,
        bull_probability=0.7,
        base_probability=0.2,
        bear_probability=0.1,
        thesis=Thesis(summary="Evidence-backed test thesis"),
        as_of=utc_now(),
        evidence_complete=complete,
    )


def test_order_respects_equity_limit(db):
    service = PortfolioService(ProviderRegistry(Settings(fmp_access_token="")))
    rec = recommendation()
    order = service.create_from_recommendation(db, rec, price=200)
    assert order.quantity == 40
    snapshot = service.snapshot(db, prices={rec.asset.asset_id: 200})
    assert snapshot.positions[0].weight <= 0.081
    assert snapshot.cash_usd > snapshot.nav_usd * 0.10


def test_incomplete_recommendation_can_trade_when_rating_confidence_is_sufficient(db):
    service = PortfolioService(ProviderRegistry(Settings(fmp_access_token="")))
    order = service.create_from_recommendation(
        db,
        recommendation(complete=False),
        price=200,
    )

    assert order.quantity == 40


def test_low_confidence_recommendation_cannot_trade(db):
    service = PortfolioService(ProviderRegistry(Settings(fmp_access_token="")))
    with pytest.raises(PortfolioError, match="below 55%"):
        service.create_from_recommendation(db, recommendation(confidence=0.54), price=200)


def test_repeated_order_cannot_exceed_single_asset_limit(db):
    service = PortfolioService(ProviderRegistry(Settings(fmp_access_token="")))
    rec = recommendation()
    service.create_from_recommendation(db, rec, price=200)
    with pytest.raises(PortfolioError, match="risk limit"):
        service.create_from_recommendation(db, rec, price=200)
