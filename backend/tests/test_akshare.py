import sys
from datetime import UTC, datetime
from types import SimpleNamespace

from backend.app.domain import AssetRef
from backend.app.providers.akshare_provider import TIME_NORMALIZATION_MARKER, AkShareProvider


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
            self.value = []
            self.set_calls = []

        def key(self, namespace, value):
            return f"{namespace}:{value['version']}"

        def get(self, key):
            return self.value

        def set(self, key, value, ttl_seconds):
            self.value = value
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
    assert fake_cache.set_calls == [("akshare-security-master:1", 24 * 60 * 60)]
