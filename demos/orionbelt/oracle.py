"""Independent acceptance oracle. No SQL, model parsing or OrionBelt imports."""
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
METRICS = ['Revenue', 'Refunds', 'Net Revenue', 'Order Count', 'Customer Count',
           'Units', 'Average Order Value', 'Refund Rate']
SCALES = {'Revenue': 2, 'Refunds': 2, 'Net Revenue': 2,
          'Order Count': 0, 'Customer Count': 0, 'Units': 0,
          'Average Order Value': 4, 'Refund Rate': 6}
CASES = [
    {'name': 'all-orders', 'dimensions': []},
    {'name': 'by-region', 'dimensions': ['Region']},
    {'name': 'by-month', 'dimensions': ['Month']},
    {'name': 'month-and-region', 'dimensions': ['Month', 'Region']},
    {'name': 'north', 'dimensions': [], 'region': 'North'},
    {'name': 'no-refunds-west', 'dimensions': ['Region'], 'region': 'West'},
    {'name': 'january', 'dimensions': ['Region'], 'start': '2026-01-01', 'end': '2026-01-31'},
    {'name': 'february-north', 'dimensions': [], 'region': 'North',
     'start': '2026-02-01', 'end': '2026-02-28'},
    {'name': 'inclusive-boundaries', 'dimensions': ['Month'],
     'start': '2026-01-31', 'end': '2026-02-01'},
    {'name': 'empty-filter', 'dimensions': ['Region'], 'region': 'Missing'},
]


def expected(case, fixture=None):
    data = fixture or json.loads((HERE / 'fixture.json').read_text())
    refunds, units = defaultdict(Decimal), defaultdict(int)
    for _, order_id, amount in data['refunds']:
        refunds[order_id] += Decimal(amount)
    for _, order_id, quantity in data['items']:
        units[order_id] += quantity
    groups = defaultdict(list)
    for order in data['orders']:
        _, date, region, _, _ = order
        if case.get('region') and region != case['region']:
            continue
        if case.get('start') and date < case['start']:
            continue
        if case.get('end') and date > case['end']:
            continue
        values = {'Region': region, 'Month': date[:7]}
        groups[tuple(values[d] for d in case['dimensions'])].append(order)
    rows = []
    for key, orders in sorted(groups.items()):
        revenue = sum((Decimal(o[4]) for o in orders), Decimal(0))
        returned = sum((refunds[o[0]] for o in orders), Decimal(0))
        count = len(orders)
        rows.append([*key, revenue, returned, revenue - returned, count,
                     len({o[3] for o in orders}), sum(units[o[0]] for o in orders),
                     revenue / count, returned / revenue if revenue else None])
    return rows


def assert_rows(actual, wanted, label, columns=None):
    """Compare complete result sets; retain nulls, duplicates and numeric precision."""
    if len(actual) != len(wanted):
        raise AssertionError(f'{label}: expected {len(wanted)} rows, got {len(actual)}: {actual}')
    for i, (got, expected_row) in enumerate(zip(actual, wanted)):
        if len(got) != len(expected_row):
            raise AssertionError(f'{label}: row {i} width mismatch')
        for j, (value, target) in enumerate(zip(got, expected_row)):
            if isinstance(target, (Decimal, int)):
                scale = SCALES.get(columns[j]) if columns else None
                different = value is None
                if not different:
                    number = Decimal(str(value))
                    if scale == 0:
                        different = number != Decimal(target)
                    elif scale is None:
                        different = abs(number - Decimal(target)) > Decimal('0.000001')
                    else:
                        quantum = Decimal(1).scaleb(-scale)
                        # The REST API returns unrounded ratios; Flight advertises
                        # fixed decimal scales. Compare at the business precision.
                        different = (number.quantize(quantum, rounding=ROUND_HALF_UP)
                                     != Decimal(target).quantize(quantum, rounding=ROUND_HALF_UP))
                if different:
                    raise AssertionError(f'{label}: [{i},{j}] expected {target}, got {value}')
            elif value != target:
                raise AssertionError(f'{label}: [{i},{j}] expected {target!r}, got {value!r}')


def naive_join_revenue():
    """Deliberately wrong LEFT JOIN total; a negative control for fanout."""
    data = json.loads((HERE / 'fixture.json').read_text())
    return sum(Decimal(order[4])
               * max(1, sum(item[1] == order[0] for item in data['items']))
               * max(1, sum(refund[1] == order[0] for refund in data['refunds']))
               for order in data['orders'])
