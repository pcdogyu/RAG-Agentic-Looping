import sys
from datetime import UTC, datetime
from types import SimpleNamespace

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
