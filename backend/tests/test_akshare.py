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
