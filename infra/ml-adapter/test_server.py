import unittest

from server import texts


class PayloadTests(unittest.TestCase):
    def test_texts_are_validated(self):
        self.assertEqual(texts({"texts": ["a", "b"]}), ["a", "b"])
        with self.assertRaises(ValueError):
            texts({"texts": []})


if __name__ == "__main__":
    unittest.main()
