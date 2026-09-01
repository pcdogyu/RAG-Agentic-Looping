import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from server import _primitive, discover_news


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


if __name__ == "__main__":
    unittest.main()
