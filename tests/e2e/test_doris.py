"""Apache Doris backend (docker-compose.doris.yaml) — the first MySQL-dialect
Flight SQL backend in the matrix.

Doris exercises paths nothing else does: MySQL SQL dialect, `SET ROLE`-capable
RBAC, full DDL/DML writes, and the `useServerPrepStmts=false` quirk. It is also
the canary for dialect divergence — DuckDB/DataFusion-flavored SQL the driver
emits by default may misbehave here, which is exactly what scopes the future
`sql-dialect` connection property (ROADMAP A1).

Auth: default `root` with an empty password over the FE Flight SQL port (8070).
Doris "databases" surface as Metabase schemas.
"""
import time

import pytest

from conftest import ENV, port_open

# Gate on DORIS_READY (written by scripts/setup_doris.py only when a BE is
# alive AND sample data loaded), not just the FE port — the FE Flight SQL port
# opens well before the BE is usable, so port-open alone would run these tests
# against a half-started cluster.
requires_doris = pytest.mark.skipif(
    ENV.get("DORIS_READY") != "1" or not port_open("localhost", 8070),
    reason="Doris not ready (run docker-compose.doris.yaml + scripts/setup_doris.py; "
           "note: the BE does not run under podman-on-Windows — use Linux/CI)")

pytestmark = requires_doris

# useServerPrepStmts=false is REQUIRED — Arrow Flight SQL JDBC + Doris do not
# support prepared-statement parameter passing.
DORIS = {"host": "doris", "port": 8070, "use-token": False,
         "username": "root", "password": "",
         "additional-options": "useServerPrepStmts=false",
         "useEncryption": False, "disableCertificateVerification": True}


def schemas_of(tables):
    return {t["schema"] for t in tables}


def test_doris_userpass_connects_and_queries(mb, db_factory):
    db_id = db_factory("doris-userpass", DORIS)
    res = mb.native(db_id, "SELECT COUNT(*) FROM sales.orders")
    assert res.get("status") == "completed", res.get("error")
    assert res["data"]["rows"][0][0] == 8


def test_doris_wrong_password_rejected(mb, db_factory):
    db_id = db_factory("doris-badpass", {**DORIS, "password": "definitely-wrong"},
                       expect_ok=False)
    assert db_id is None


def test_doris_multi_schema_sync(mb, db_factory):
    db_id = db_factory("doris-multischema", DORIS)
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: len({t["schema"] for t in ts}) >= 3)
    assert {"sales", "hr", "analytics"} <= schemas_of(tables), schemas_of(tables)
    names = {(t["schema"], t["name"]) for t in tables}
    assert ("sales", "orders") in names and ("hr", "employees") in names


def test_doris_schema_filter_inclusion(mb, db_factory):
    db_id = db_factory("doris-filter-hr", {**DORIS,
                                           "schema-filters-type": "inclusion",
                                           "schema-filters-patterns": "hr"})
    tables = mb.wait_synced_tables(db_id)
    assert tables and schemas_of(tables) == {"hr"}, schemas_of(tables)


def test_doris_write_ctas_and_query(mb, db_factory):
    """Doris is fully writable over Flight SQL (DDL + DML)."""
    db_id = db_factory("doris-write", {**DORIS, "enable-uploads": True})
    tbl = "sales.driver_write_probe"
    try:
        # explicit DDL (Doris needs key + distribution)
        r1 = mb.native(db_id,
                       f"CREATE TABLE IF NOT EXISTS {tbl} (id INT, label VARCHAR(20)) "
                       "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                       'PROPERTIES ("replication_num"="1")')
        assert r1.get("status") == "completed", r1.get("error")
        r2 = mb.native(db_id, f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b'),(3,'c')")
        assert r2.get("status") == "completed", r2.get("error")
        # Doris ingestion is async-ish; poll the count
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


def test_doris_writable_advertises_upload_feature(mb, db_factory):
    db_id = db_factory("doris-features", {**DORIS, "enable-uploads": True})
    _, full = mb.get(f"/api/database/{db_id}")
    feats = full.get("features") or []
    assert "uploads" in feats and "transforms/table" in feats
