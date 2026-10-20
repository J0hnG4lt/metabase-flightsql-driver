"""Create immutable demo data inside the OrionBelt image (DuckDB is installed)."""
import hashlib
import json
from pathlib import Path
import sys

import duckdb


def main():
    source = Path(__file__).with_name('fixture.json').read_bytes()
    digest = hashlib.sha256(source).hexdigest()
    target = Path(sys.argv[1])
    if target.exists():
        with duckdb.connect(str(target), read_only=True) as db:
            if db.execute('SELECT sha256 FROM fixture_version').fetchone() != (digest,):
                raise RuntimeError('Fixture changed; choose a new isolated compose project/volume.')
        print('Fixture already seeded, hash matches.')
        return
    data = json.loads(source)
    with duckdb.connect(str(target)) as db:
        db.execute('BEGIN')
        db.execute('CREATE TABLE orders(order_id INTEGER PRIMARY KEY, order_date DATE, '
                   'region VARCHAR, customer VARCHAR, amount DECIMAL(18,2), month VARCHAR)')
        db.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)',
                       [row + [row[1][:7]] for row in data['orders']])
        db.execute('CREATE TABLE items(item_id INTEGER PRIMARY KEY, order_id INTEGER, quantity INTEGER)')
        db.executemany('INSERT INTO items VALUES (?, ?, ?)', data['items'])
        db.execute('CREATE TABLE refunds(refund_id INTEGER PRIMARY KEY, order_id INTEGER, amount DECIMAL(18,2))')
        db.executemany('INSERT INTO refunds VALUES (?, ?, ?)', data['refunds'])
        # Separate facts share a leaf dimension. A view preserves orders without
        # refund events so asking for Refunds alone still returns zero for West.
        db.execute('CREATE VIEW order_context AS SELECT order_id, order_date, month, region FROM orders')
        db.execute('CREATE VIEW covered_refunds AS SELECT r.refund_id, o.order_id, r.amount '
                   'FROM orders o LEFT JOIN refunds r USING (order_id)')
        db.execute('CREATE TABLE fixture_version(sha256 VARCHAR)')
        db.execute('INSERT INTO fixture_version VALUES (?)', [digest])
        db.execute('COMMIT')
    print('Seeded 8 orders, 13 items, 6 refunds; fixture sha256=' + digest)


if __name__ == '__main__':
    main()
