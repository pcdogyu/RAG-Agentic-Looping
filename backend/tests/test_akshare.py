import socket
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

from backend.app.domain import AssetClass, AssetRef, Market
from backend.app.providers.akshare_provider import (
    TIME_NORMALIZATION_MARKER,
    AkShareProvider,
    _request_address_family,
)


class FakeFrame:
    def head(self, limit):
        return self

    def to_dict(self, orient):
        assert orient == "records"
        return [
            {
                "标题": "北京时间新闻",
                "链接": "https://example.com/shanghai-time",
                "发布时间": "2026-08-22 17:30:00",
                "来源": "东方财富/AkShare",
            }
        ]


def test_akshare_ipv4_context_restores_urllib3_address_family():
    from urllib3.util import connection

    original = connection.allowed_gai_family
    with _request_address_family(True):
        assert connection.allowed_gai_family() == socket.AF_INET
    assert connection.allowed_gai_family is original


def test_akshare_naive_timestamp_is_interpreted_as_shanghai_time(monkeypatch):
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_info_global_em=lambda: FakeFrame()),
    )

    items = AkShareProvider().discover_news(
        since=datetime(2026, 8, 22, 9, 0, tzinfo=UTC), limit=10
    )

    assert len(items) == 1
    assert items[0].published_at == datetime(2026, 8, 22, 9, 30, tzinfo=UTC)
    assert items[0].raw_metadata["time_normalization"] == TIME_NORMALIZATION_MARKER


class SecurityFrame:
    def __init__(self, rows):
        self.rows = rows

    def to_dict(self, orient):
        assert orient == "records"
        return self.rows


def test_akshare_builds_and_resolves_cn_hk_security_master(monkeypatch):
    provider = AkShareProvider()
    assets = [
        *[
            AssetRef.model_validate(item)
            for item in provider._a_share_records(
                SecurityFrame([{"code": "300308", "name": "中际旭创"}])
            )
        ],
        *[
            AssetRef.model_validate(item)
            for item in provider._hk_share_records(
                SecurityFrame([{"代码": "700", "名称": "腾讯控股"}])
            )
        ],
    ]
    monkeypatch.setattr(provider, "_listed_assets", lambda: assets)

    cn = provider.resolve_assets("章建平调仓中际旭创")
    hk = provider.resolve_assets("腾讯控股发布业绩")

    assert cn[0].asset_id == "equity:XSHE:300308"
    assert cn[0].currency == "CNY"
    assert hk[0].asset_id == "equity:XHKG:00700"
    assert hk[0].currency == "HKD"


def test_akshare_does_not_cache_an_empty_security_master(monkeypatch):
    class FakeCache:
        def __init__(self):
            self.values = {}
            self.set_calls = []

        def key(self, namespace, value):
            return f"{namespace}:{value['version']}"

        def get(self, key):
            return self.values.get(key, [])

        def set(self, key, value, ttl_seconds):
            self.values[key] = value
            self.set_calls.append((key, ttl_seconds))

    calls = 0

    def a_share_master():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary upstream failure")
        return SecurityFrame([{"code": "600499", "name": "科达制造"}])

    fake_cache = FakeCache()
    monkeypatch.setattr("backend.app.providers.akshare_provider.cache", fake_cache)
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=a_share_master,
            stock_hk_spot_em=lambda: SecurityFrame([]),
        ),
    )
    provider = AkShareProvider()

    assert provider._listed_assets() == []
    assert fake_cache.set_calls == []

    assets = provider._listed_assets()
    assert [(item.symbol, item.name) for item in assets] == [("600499", "科达制造")]
    assert fake_cache.set_calls == [
        ("akshare-a-share-security-master:2", 24 * 60 * 60)
    ]


def test_akshare_partial_a_share_cache_does_not_poison_hk_master(monkeypatch):
    class FakeCache:
        def __init__(self):
            self.values = {}

        def key(self, namespace, value):
            return f"{namespace}:{value['version']}"

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, _ttl_seconds):
            self.values[key] = value

    calls = {"a": 0, "hk": 0}

    def a_share_master():
        calls["a"] += 1
        return SecurityFrame([{"code": "600499", "name": "科达制造"}])

    def hk_share_master():
        calls["hk"] += 1
        if calls["hk"] == 1:
            raise RuntimeError("temporary HK upstream failure")
        return SecurityFrame([{"代码": "9988", "名称": "阿里巴巴-W"}])

    monkeypatch.setattr(
        "backend.app.providers.akshare_provider.cache",
        FakeCache(),
    )
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=a_share_master,
            stock_hk_spot_em=hk_share_master,
        ),
    )
    provider = AkShareProvider()

    assert [item.symbol for item in provider._listed_assets()] == ["600499"]
    assert [item.symbol for item in provider._listed_assets()] == ["600499", "09988"]
    assert calls == {"a": 1, "hk": 2}
    assert provider.resolve_assets("9988")[0].symbol == "09988"


def test_akshare_hk_master_falls_back_to_sina_directory(monkeypatch):
    calls = []
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: SecurityFrame([]),
            stock_hk_spot_em=lambda: (_ for _ in ()).throw(
                ConnectionError("Eastmoney closed the connection")
            ),
            stock_hk_spot=lambda: calls.append("sina")
            or SecurityFrame(
                [{"代码": "1", "中文名称": "长和", "英文名称": "CKH HOLDINGS"}]
            ),
        ),
    )
    provider = AkShareProvider()

    assets = provider._listed_assets()

    assert calls == ["sina"]
    assert [(item.symbol, item.name) for item in assets] == [("00001", "长和")]


def test_akshare_collects_a_share_business_financial_and_valuation_data(monkeypatch):
    calls = []

    def frame(name, rows):
        calls.append(name)
        return SecurityFrame(rows)

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_zyjs_ths=lambda symbol: frame(
                f"business:{symbol}",
                [{"股票代码": symbol, "主营业务": "算力设备", "产品名称": "服务器"}],
            ),
            stock_zygc_em=lambda symbol: frame(
                f"composition:{symbol}",
                [{"报告日期": "2025-12-31", "分类类型": "按产品分类", "主营构成": "服务器"}],
            ),
            stock_financial_analysis_indicator_em=lambda symbol, indicator: frame(
                f"financials:{symbol}:{indicator}",
                [{
                    "REPORT_DATE": "2025-12-31",
                    "REPORT_DATE_NAME": "2025年报",
                    "TOTALOPERATEREVE": 1000,
                    "PARENTNETPROFIT": 120,
                    "ROEJQ": 12.5,
                }],
            ),
            stock_value_em=lambda symbol: frame(
                f"valuation:{symbol}",
                [{"数据日期": "2026-08-24", "PE(TTM)": 20.1, "市净率": 3.2, "市销率": 2.4}],
            ),
            stock_individual_info_em=lambda symbol: frame(
                f"info:{symbol}",
                [{"item": "行业", "value": "通信设备"}, {"item": "总市值", "value": 50_000}],
            ),
        ),
    )
    asset = AssetRef(
        asset_id="equity:XSHE:301389",
        asset_class=AssetClass.EQUITY,
        market=Market.CN,
        symbol="301389",
        name="示例公司",
        exchange_or_provider="XSHE",
        currency="CNY",
    )

    data = AkShareProvider().get_fundamentals(asset)

    assert data["provider"] == "akshare"
    assert data["business_profile"]["主营业务"] == "算力设备"
    assert data["business_composition"][0]["主营构成"] == "服务器"
    assert data["financial_indicators"][0]["PARENTNETPROFIT"] == 120
    assert data["valuation"][0]["PE(TTM)"] == 20.1
    assert data["company_info"] == {"行业": "通信设备", "总市值": 50_000}
    assert "financials:301389.SZ:按报告期" in calls
    assert "composition:SZ301389" in calls
