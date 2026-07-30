"""Dremio (OSS) backend (docker-compose.dremio.yaml) — a native lakehouse
engine that speaks Arrow Flight SQL and stores tables as Apache Iceberg.

Why this backend matters in the matrix:
  - It is the first backend that reads/writes **Apache Iceberg** (Dremio's
    default table format for filesystem sources), so it proves Metabase can
    read Iceberg through this driver end-to-end.
  - It exercises the **Bearer/PAT** auth branch against a real server that
    issues Personal Access Tokens (only InfluxDB 3 did before).
  - Unlike Doris/StarRocks, single-node Dremio is one JVM that serves Flight
    results inline (no advertised 127.0.0.1 endpoint), so it actually works
    from a separate Metabase container — hence this job is NOT continue-on-error.

Seed/setup: scripts/setup_dremio.py (admin bootstrap + `wh` NAS source + three
Iceberg tables wh.sales.orders / wh.hr.employees / wh.analytics.events + a PAT).
"""
import pytest

from conftest import ENV, port_open

# Gate on DREMIO_READY (written by setup_dremio.py only after the admin user,
# source, and Iceberg tables all exist), not just the open Flight port.
requires_dremio = pytest.mark.skipif(
    ENV.get("DREMIO_READY") != "1" or not port_open("localhost", 32010),
    reason="Dremio not ready (docker-compose.dremio.yaml + scripts/setup_dremio.py)")

requires_pat = pytest.mark.skipif(
    "DREMIO_PAT" not in ENV,
    reason="no DREMIO_PAT in .env — Dremio OSS does not expose the PAT REST "
           "endpoint (Enterprise feature; the Bearer/use-token branch is "
           "covered against InfluxDB 3 instead)")

pytestmark = requires_dremio

DREMIO = {"host": "dremio", "port": 32010, "use-token": False,
          "username": "dremio", "password": "dremio123",
          "useEncryption": False, "disableCertificateVerification": True}


def schemas_of(tables):
    return {t["schema"] for t in tables}


def test_dremio_userpass_connects_and_queries(mb, db_factory):
    db_id = db_factory("dremio-userpass", DREMIO)
    res = mb.native(db_id, "SELECT COUNT(*) FROM wh.sales.orders")
    assert res.get("status") == "completed", res.get("error")
    assert res["data"]["rows"][0][0] == 8


def test_dremio_wrong_password_rejected(mb, db_factory):
    db_id = db_factory("dremio-badpass", {**DREMIO, "password": "definitely-wrong"},
                       expect_ok=False)
    assert db_id is None


@requires_pat
def test_dremio_pat_bearer_auth(mb, db_factory):
    """Personal Access Token over the use-token (authorization=Bearer) branch."""
    details = {"host": "dremio", "port": 32010, "use-token": True,
               "token-value": ENV["DREMIO_PAT"],
               "useEncryption": False, "disableCertificateVerification": True}
    db_id = db_factory("dremio-pat", details)
    res = mb.native(db_id, "SELECT COUNT(*) FROM wh.sales.orders")
    assert res.get("status") == "completed", res.get("error")
    assert res["data"]["rows"][0][0] == 8


def test_dremio_reads_iceberg_table(mb, db_factory):
    """Headline: the CTAS tables are Iceberg, so this is Metabase reading an
    Iceberg table via Dremio through the Flight SQL driver."""
    db_id = db_factory("dremio-iceberg", DREMIO)
    res = mb.native(
        db_id,
        "SELECT dept, COUNT(*) AS n FROM wh.hr.employees GROUP BY dept ORDER BY dept")
    assert res.get("status") == "completed", res.get("error")
    counts = {r[0]: r[1] for r in res["data"]["rows"]}
    assert counts.get("Engineering") == 2 and counts.get("Research") == 2, counts


def test_dremio_multi_schema_sync(mb, db_factory):
    db_id = db_factory("dremio-multischema", DREMIO)
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: len({t["schema"] for t in ts}) >= 3)
    names = {t["name"] for t in tables}
    assert {"orders", "employees", "events"} <= names, names
    assert len(schemas_of(tables)) >= 3, schemas_of(tables)
    # at least one schema should carry the `sales` folder path
    assert any("sales" in s for s in schemas_of(tables)), schemas_of(tables)


def test_dremio_schema_filter_inclusion(mb, db_factory):
    """Adaptive: learn the exact schema string Dremio reports for the hr table
    (e.g. 'wh.hr'), then confirm an inclusion filter to it isolates that schema
    — robust regardless of how Dremio qualifies nested folders."""
    probe = db_factory("dremio-probe", DREMIO)
    tables = mb.wait_synced_tables(
        probe, predicate=lambda ts: any(t["name"] == "employees" for t in ts))
    hr_schema = next(t["schema"] for t in tables if t["name"] == "employees")

    filt = db_factory("dremio-filter-hr", {**DREMIO,
                                           "schema-filters-type": "inclusion",
                                           "schema-filters-patterns": hr_schema})
    ftables = mb.wait_synced_tables(filt)
    assert ftables and schemas_of(ftables) == {hr_schema}, schemas_of(ftables)


def test_dremio_ctas_iceberg_write_and_query(mb, db_factory):
    """Dremio is writable over Flight SQL; CTAS materializes a new Iceberg
    table which we then read back."""
    db_id = db_factory("dremio-write", DREMIO)
    tbl = "wh.sales.driver_write_probe"
    try:
        r1 = mb.native(db_id,
                       f"CREATE TABLE {tbl} AS "
                       "SELECT * FROM (VALUES (1),(2),(3)) AS t(x)")
        assert r1.get("status") == "completed", r1.get("error")
        r2 = mb.native(db_id, f"SELECT COUNT(*) FROM {tbl}")
        assert r2["data"]["rows"][0][0] == 3
    finally:
        mb.native(db_id, f"DROP TABLE IF EXISTS {tbl}")
