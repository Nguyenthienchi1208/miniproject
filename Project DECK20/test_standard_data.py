import unittest
from resudata import StandardData

class TestStandardData(unittest.TestCase):
    def instand_pipe(self):
        self.pipeline = StandardData()

    def test_salary_vnd(self):
        result = self.pipeline.salary_usd_vnd("30-50 triệu")
        self.assertIsInstance(result[0], float)
        self.assertIsInstance(result[1], float)
        self.assertEqual(result[2], "VND")

    def test_salary_usd(self):
        result = self.pipeline.salary_usd_vnd("1000-2000 USD")
        self.assertIsInstance(result[0], float)
        self.assertIsInstance(result[1], float)
        self.assertEqual(result[2], "USD")

    def test_expand_address(self):
        result = self.pipeline.expand_address("Hà Nội:Ba Đình")
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], tuple)
        self.assertEqual(result, [("Hà Nội", "Ba Đình")])

if __name__ == '__main__':
    unittest.main()
