from backend.app.config import Settings
from backend.app.db import AssetRow, IndustryRow
from backend.app.domain import AssetClass, AssetRef, Market
from backend.app.providers.cache import cache
from backend.app.providers.crypto import CryptoProvider
from backend.app.services.asset_universe import AssetUniverseService, universe_status


def asset(
    asset_id: str,
    market: Market,
    symbol: str,
    name: str,
    *,
    industry_id: str = "industry:semiconductors",
) -> AssetRef:
    return AssetRef(
        asset_id=asset_id,
        asset_class=AssetClass.CRYPTO if market is Market.CRYPTO else AssetClass.EQUITY,
        market=market,
        symbol=symbol,
        name=name,
        exchange_or_provider="test",
        sector_id=(
            "sector:digital_assets"
            if market is Market.CRYPTO
            else "sector:information_technology"
        ),
        industry_id=(
            "industry:cryptocurrency"
            if market is Market.CRYPTO
            else industry_id
        ),
        instrument_type="crypto" if market is Market.CRYPTO else "common_stock",
    )


class Provider:
    def __init__(self, values=None, error: Exception | None = None):
        self.values = values or []
        self.error = error

    def list_equity_universe(self):
        if self.error:
            raise self.error
        return self.values

    def all_assets(self):
        if self.error:
            raise self.error
        return self.values


class Registry:
    def __init__(self, *, fail_us: bool = False):
        listed = [
            asset("equity:XSHG:688981", Market.CN, "688981", "中芯国际"),
            asset("equity:XHKG:00981", Market.HK, "00981", "中芯国际"),
        ]
        self.akshare = Provider(listed)
        self.fmp = Provider(
            [asset("equity:XNAS:NVDA", Market.US, "NVDA", "NVIDIA")],
            RuntimeError("US provider unavailable") if fail_us else None,
        )
        self.crypto = Provider(
            [
                asset("crypto:coingecko:bitcoin", Market.CRYPTO, "BTC", "Bitcoin"),
                asset("crypto:coingecko:small-coin", Market.CRYPTO, "SMALL", "Small Coin"),
            ]
        )


def test_universe_sync_persists_all_markets_and_complete_crypto_directory(db):
    result = AssetUniverseService(db, Registry()).sync()

    assert result["status"] == "completed"
    assert db.query(AssetRow).filter(AssetRow.active.is_(True)).count() == 5
    assert db.get(AssetRow, "crypto:coingecko:small-coin") is not None
    assert db.query(IndustryRow).filter(IndustryRow.level == 2).count() >= 20
    status = universe_status(db)
    assert status["active_counts"] == {"CN": 1, "CRYPTO": 2, "HK": 1, "US": 1}
    assert {item["market"] for item in status["markets"]} == {
        "CN",
        "CRYPTO",
        "HK",
        "US",
    }


def test_universe_sync_is_failure_isolated_and_preserves_other_markets(db):
    result = AssetUniverseService(db, Registry(fail_us=True)).sync()

    assert result["status"] == "completed_with_errors"
    assert result["markets"]["US"]["status"] == "failed"
    assert "US provider unavailable" in result["markets"]["US"]["error"]
    assert db.get(AssetRow, "equity:XSHG:688981") is not None
    assert db.get(AssetRow, "crypto:coingecko:small-coin") is not None


def test_universe_sync_deactivates_missing_provider_assets(db):
    service = AssetUniverseService(db, Registry())
    service.sync([Market.CRYPTO])
    bitcoin = db.get(AssetRow, "crypto:coingecko:bitcoin")
    bitcoin.manual_active = False
    bitcoin.active = False
    bitcoin.manual_industry_id = "industry:software"
    bitcoin.industry_id = "industry:software"
    db.add(bitcoin)
    db.commit()
    service.registry.crypto.values = [
        asset("crypto:coingecko:bitcoin", Market.CRYPTO, "BTC", "Bitcoin")
    ]

    service.sync([Market.CRYPTO])

    bitcoin = db.get(AssetRow, "crypto:coingecko:bitcoin")
    assert bitcoin.active is False
    assert bitcoin.industry_id == "industry:software"
    assert db.get(AssetRow, "crypto:coingecko:small-coin").active is False


def test_crypto_provider_uses_complete_coingecko_identity_directory(monkeypatch):
    payload = [
        {"id": f"coin-{index}", "symbol": f"c{index}", "name": f"Coin {index}"}
        for index in range(125)
    ]

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    provider = CryptoProvider(Settings(coingecko_base_url="https://coins.example.test"))
    monkeypatch.setattr(provider.client, "get", lambda *args, **kwargs: Response())
    monkeypatch.setattr(cache, "remember", lambda key, ttl, loader: loader())

    assets = provider.all_assets()

    assert len(assets) == 125
    assert assets[-1].asset_id == "crypto:coingecko:coin-124"
    assert all(item.industry_id == "industry:cryptocurrency" for item in assets)
