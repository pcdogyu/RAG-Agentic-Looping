import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from server import _primitive, discover_news, equity_universe


class _Frame:
    def __init__(self, rows):
        self.rows = rows

    def notna(self):
        return self

    def where(self, *_args):
        return self

    def to_dict(self, *, orient):
        assert orient == "records"
        return self.rows


class PrimitiveTests(unittest.TestCase):
    def test_date_is_serialized(self):
        self.assertEqual(_primitive(date(2026, 8, 29)), "2026-08-29")

    def test_news_is_filtered_and_normalized_to_utc(self):
        module = SimpleNamespace(
            stock_info_global_em=lambda: _Frame(
                [
                    {
                        "标题": "新消息",
                        "链接": "https://example.com/new",
                        "发布时间": "2026-09-01T08:00:00",
                        "来源": "东方财富",
                    },
                    {
                        "标题": "旧消息",
                        "链接": "https://example.com/old",
                        "发布时间": "2026-08-31T07:00:00",
                    },
                ]
            )
        )
        with patch.dict("sys.modules", {"akshare": module}):
            result = discover_news({"since": "2026-09-01T00:00:00Z", "limit": 10})
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["items"][0]["source"], "东方财富")
        self.assertEqual(result["items"][0]["published_at"], "2026-09-01T00:00:00+00:00")

    def test_equity_universe_normalizes_cn_hk_and_beijing_exchanges(self):
        module = SimpleNamespace(
            stock_info_a_code_name=lambda: _Frame([
                {"code": "600000", "name": "沪市公司"},
                {"code": "430001", "name": "北交所公司"},
            ]),
            stock_hk_spot_em=lambda: _Frame([{"代码": "9988", "名称": "阿里巴巴"}]),
        )
        with patch.dict("sys.modules", {"akshare": module}):
            result = equity_universe({})
        self.assertEqual(
            [item["asset_id"] for item in result["items"]],
            ["equity:XSHG:600000", "equity:XBEI:430001", "equity:XHKG:09988"],
        )

    def test_equity_universe_rejects_unsupported_market(self):
        with self.assertRaisesRegex(ValueError, "market must be CN or HK"):
            equity_universe({"market": "US"})

    def test_equity_universe_falls_back_to_official_cn_directories(self):
        def unavailable():
            raise ConnectionError("Eastmoney unavailable")

        module = SimpleNamespace(
            stock_info_a_code_name=unavailable,
            stock_info_sh_name_code=lambda symbol: _Frame(
                [{"证券代码": "688001" if symbol == "科创板" else "600001", "证券简称": symbol}]
            ),
            stock_info_sz_name_code=lambda symbol: _Frame(
                [{"A股代码": "000001", "A股简称": symbol}]
            ),
            stock_info_bj_name_code=lambda: _Frame(
                [{"证券代码": "430001", "证券简称": "北交所"}]
            ),
        )
        with patch.dict("sys.modules", {"akshare": module}):
            result = equity_universe({"market": "CN"})
        self.assertEqual(len(result["items"]), 4)
        self.assertEqual(result["items"][-1]["asset_id"], "equity:XBEI:430001")


if __name__ == "__main__":
    unittest.main()
