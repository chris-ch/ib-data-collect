import unittest

import ibdataloader


class BasicTest(unittest.TestCase):
    def test_product_type(self):
        etf_type = ibdataloader.ProductType('etf')
        self.assertEqual(ibdataloader.ProductType.ETF, etf_type)


if __name__ == '__main__':
    unittest.main()
