"""Known answers and negative controls for the demo's independent oracle."""
from decimal import Decimal
import unittest

from oracle import CASES, assert_rows, expected, naive_join_revenue


class OracleTest(unittest.TestCase):
    def test_hand_calculated_totals(self):
        row = expected(CASES[0])[0]
        self.assertEqual(row[:7], [850, 175, 675, 8, 6, 17, Decimal('106.25')])
        self.assertEqual(naive_join_revenue(), 2150)

    def test_non_additive_customer_count(self):
        self.assertEqual(sum(r[5] for r in expected(CASES[2])), 8)
        self.assertEqual(expected(CASES[0])[0][4], 6)

    def test_no_refunds_and_inclusive_dates(self):
        self.assertEqual(expected(CASES[5])[0][:4], ['West', 60, 0, 60])
        rows = expected(CASES[8])
        self.assertEqual([r[:4] for r in rows], [['2026-01', 50, 0, 50], ['2026-02', 150, 30, 120]])
        self.assertEqual(expected(CASES[9]), [])

    def test_comparison_rejects_null_dropped_duplicate_and_wrong_value(self):
        for actual in ([[None]], [], [[1], [1]], [[2]]):
            with self.subTest(actual=actual), self.assertRaises(AssertionError):
                assert_rows(actual, [[1]], 'negative control')

    def test_decimal_tolerance_is_bounded(self):
        assert_rows([['0.205882']], [[Decimal(175) / Decimal(850)]], 'rounded ratio')
        with self.assertRaises(AssertionError):
            assert_rows([['0.2058']], [[Decimal(175) / Decimal(850)]], 'incorrect ratio')


if __name__ == '__main__':
    unittest.main()
