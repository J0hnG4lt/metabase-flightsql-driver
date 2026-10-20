"""Run in the pinned Flight image; exercises actual Arrow and SQL types."""
from decimal import Decimal
import unittest
from unittest.mock import patch

import pyarrow as pa
import sqlglot

from flight_compat import align_by_name, quoted_catalog_dispatch, typed_scalar_probe


class CompatibilityTest(unittest.TestCase):
    def test_values_follow_advertised_names_not_compiler_positions(self):
        actual = pa.table({'Revenue': [850], 'Order Count': [8], 'Refunds': [175]})
        schema = pa.schema([('Revenue', pa.int64()), ('Refunds', pa.int64()), ('Order Count', pa.int64())])
        self.assertEqual(align_by_name(actual, schema).to_pylist(),
                         [{'Revenue': 850, 'Refunds': 175, 'Order Count': 8}])
        self.assertEqual(align_by_name(actual, schema).column_names, schema.names)
        with self.assertRaises(ValueError):
            align_by_name(actual.drop_columns(['Refunds']), schema)

    def test_numeric_probe_and_quoted_text_remain_distinct(self):
        table = typed_scalar_probe(sqlglot.parse_one("SELECT 1 AS n, '1' AS s, 1.25 AS d"))
        self.assertEqual(table.to_pylist(), [{'n': 1, 's': '1', 'd': Decimal('1.25')}])
        self.assertTrue(pa.types.is_integer(table.schema.field('n').type))
        self.assertTrue(pa.types.is_string(table.schema.field('s').type))

    def test_quoted_metadata_projection_and_filter_are_applied(self):
        metadata = pa.table({'catalog_name': ['orionbelt'] * 2,
                             'db_schema_name': ['commerce', 'unrelated'],
                             'table_name': ['model'] * 2,
                             'column_name': ['Revenue', 'Wrong'], 'data_type': ['decimal', 'varchar'],
                             'is_nullable': ['YES', 'NO'], 'ordinal_position': [1, 1]})
        query = '''SELECT column_name AS name, UPPER(data_type) AS "database-type",
                   table_schema AS "table-schema", NULL AS "field-comment"
                   FROM "information_schema"."columns"
                   WHERE LOWER(table_schema) IN ('commerce') ORDER BY ordinal_position'''
        with patch('flight_compat.server_catalog.catalog_columns_table', return_value=metadata):
            table, applied = quoted_catalog_dispatch(None, query, None)
        self.assertTrue(applied)
        self.assertEqual(table.to_pylist(), [{'name': 'Revenue', 'database-type': 'DECIMAL',
                                             'table-schema': 'commerce', 'field-comment': None}])


if __name__ == '__main__':
    unittest.main()
