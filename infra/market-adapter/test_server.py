import unittest
from datetime import date

from server import _primitive


class PrimitiveTests(unittest.TestCase):
    def test_date_is_serialized(self):
        self.assertEqual(_primitive(date(2026, 8, 29)), "2026-08-29")


if __name__ == "__main__":
    unittest.main()
