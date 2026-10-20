"""Adversarial acceptance checks: don't accept plausible but mislabelled data."""
import unittest
from decimal import Decimal

from demo import KNOWN_LIMITATIONS, ordered_rows
from oracle import assert_rows


class VerificationTest(unittest.TestCase):
    def test_named_columns_protect_against_different_api_orders(self):
        self.assertEqual(ordered_rows({'columns': ['Count', 'Revenue'], 'rows': [[8, 850]]},
                                      ['Revenue', 'Count']), [[850, 8]])
        for columns in (['Revenue', 'Revenue'], ['Revenue'], ['Revenue', 'Surprise']):
            with self.subTest(columns=columns), self.assertRaises(AssertionError):
                ordered_rows({'columns': columns, 'rows': [[850, 8]]}, ['Revenue', 'Count'])

    def test_truncation_is_not_agreement(self):
        with self.assertRaises(AssertionError):
            ordered_rows({'cols': ['Revenue'], 'rows': [[850]], 'truncated': True}, ['Revenue'])
        with self.assertRaises(AssertionError):
            ordered_rows({'cols': ['Revenue'], 'rows': [[850, 999]]}, ['Revenue'])

    def test_empty_metadata_limitation_cannot_mask_nonempty_values(self):
        self.assertEqual(ordered_rows({'cols': ['Revenue', 'Revenue_2'], 'rows': []}, ['Revenue']), [])
        self.assertTrue(KNOWN_LIMITATIONS)
        with self.assertRaises(AssertionError):
            ordered_rows({'cols': ['Revenue', 'Revenue_2'], 'rows': [[850, 850]]}, ['Revenue'])

    def test_counts_are_exact_and_decimal_scales_are_bounded(self):
        with self.assertRaises(AssertionError):
            assert_rows([[8.1]], [[8]], 'count', ['Order Count'])
        assert_rows([[113.3333]], [[Decimal(340) / 3]], 'AOV', ['Average Order Value'])
        with self.assertRaises(AssertionError):
            assert_rows([[113.33]], [[Decimal(340) / 3]], 'AOV', ['Average Order Value'])


if __name__ == '__main__':
    unittest.main()
