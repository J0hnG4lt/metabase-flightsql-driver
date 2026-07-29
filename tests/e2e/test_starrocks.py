"""Apache StarRocks backend (docker-compose.starrocks.yaml) — a second
MySQL-dialect Flight SQL backend (StarRocks forked from the Doris lineage).

Unlike Doris, StarRocks' all-in-one BE comes alive reliably. It still hits a
container-networking wall under podman-on-Windows: the FE advertises its
Flight SQL data endpoint as 127.0.0.1:9408 (the all-in-one wires FE<->BE over
loopback), which a separate Metabase container can't reach. So these tests are
gated on STARROCKS_READY (written by scripts/setup_starrocks.py) and run where
the Flight SQL endpoint is actually reachable (native Linux / host networking /
localhost client).

Auth: default root with empty password over the FE Flight SQL port (9408) —
this is the case that surfaced the driver's username-with-empty-password auth
fix. StarRocks "databases" surface as Metabase schemas.
"""
import time

import pytest

from conftest import ENV, port_open

requires_starrocks = pytest.mark.skipif(
    ENV.get("STARROCKS_READY") != "1" or not port_open("localhost", 9408),
    reason="StarRocks not ready (docker-compose.starrocks.yaml + "
           "scripts/setup_starrocks.py; container-to-container Flight SQL is "
           "blocked under podman-on-Windows — use Linux/host networking)")

pytestmark = requires_starrocks

STARROCKS = {"host": "starrocks", "port": 9408, "use-token": False,
             "username": "root", "password": "",
             "useEncryption": False, "disableCertificateVerification": True}


def schemas_of(tables):
    return {t["schema"] for t in tables}


def test_starrocks_root_empty_password_connects(mb, db_factory):
    """Regression for the username-with-empty-password auth fix — StarRocks
    root has no password, and the driver must still send user=root."""
    db_id = db_factory("sr-root", STARROCKS)
    res = mb.native(db_id, "SELECT COUNT(*) FROM sales.orders")
    assert res.get("status") == "completed", res.get("error")
    assert res["data"]["rows"][0][0] == 8


def test_starrocks_wrong_password_rejected(mb, db_factory):
    db_id = db_factory("sr-badpass", {**STARROCKS, "password": "wrong"},
                       expect_ok=False)
    assert db_id is None


def test_starrocks_multi_schema_sync(mb, db_factory):
    db_id = db_factory("sr-multischema", STARROCKS)
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: len({t["schema"] for t in ts}) >= 3)
    assert {"sales", "hr", "analytics"} <= schemas_of(tables), schemas_of(tables)


def test_starrocks_schema_filter_inclusion(mb, db_factory):
    db_id = db_factory("sr-filter", {**STARROCKS,
                                     "schema-filters-type": "inclusion",
                                     "schema-filters-patterns": "hr"})
    tables = mb.wait_synced_tables(db_id)
    assert tables and schemas_of(tables) == {"hr"}, schemas_of(tables)


def test_starrocks_write_ctas_and_query(mb, db_factory):
    db_id = db_factory("sr-write", {**STARROCKS, "enable-uploads": True})
    tbl = "sales.driver_write_probe"
    try:
        r1 = mb.native(db_id,
                       f"CREATE TABLE IF NOT EXISTS {tbl} (id INT, label VARCHAR(20)) "
                       "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                       'PROPERTIES ("replication_num"="1")')
        assert r1.get("status") == "completed", r1.get("error")
        r2 = mb.native(db_id, f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b'),(3,'c')")
        assert r2.get("status") == "completed", r2.get("error")
        deadline = time.time() + 30
        n = 0
        while time.time() < deadline:
            r3 = mb.native(db_id, f"SELECT COUNT(*) FROM {tbl}")
            if r3.get("status") == "completed":
                n = r3["data"]["rows"][0][0]
                if n == 3:
                    break
            time.sleep(3)
        assert n == 3, f"expected 3 rows, got {n}"
    finally:
        mb.native(db_id, f"DROP TABLE IF EXISTS {tbl}")
