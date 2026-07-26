"""Catalog + multi-schema + schema-filters behavior across every backend.

Each backend has a different catalog/schema topology:
  gizmo (GizmoSQL/DuckDB) : catalogs memory (sales/hr/analytics + main),
                            warehouse (archive/reports), staging (imports)
  spice (Spice.ai)        : catalog `spice`, schemas public + runtime
                            (runtime is in the driver's excluded-schemas)
  influxdb3               : schemas iox (data) + system (internals)
  quack-on-demand         : DuckLake catalog per tenant (demo: acme_tpch)

Tests live-discover information_schema first so they document actual
semantics rather than assumptions; optional stacks auto-skip.
"""
import pytest

from conftest import ENV, GIZMO_DETAILS, SPICE_HOST, port_open

requires_influxdb3 = pytest.mark.skipif(
    "INFLUXDB3_TOKEN" not in ENV or not port_open("localhost", 8181),
    reason="influxdb3 not running/seeded")

requires_quack = pytest.mark.skipif(
    not port_open("localhost", 31341),
    reason="quack-on-demand not running")


def schemas_of(tables):
    return {t["schema"] for t in tables}


# --------------------------------------------------------------- gizmo ----

def test_gizmo_staging_catalog_scopes_sync(mb, db_factory):
    db_id = db_factory("cs-gizmo-staging", {**GIZMO_DETAILS, "catalog": "staging"})
    tables = mb.wait_synced_tables(db_id)
    # DuckDB attaches a per-catalog temp schema alongside the data schema
    assert tables and "imports" in schemas_of(tables) \
        and schemas_of(tables) <= {"imports", "temp"}, schemas_of(tables)


def test_gizmo_nonexistent_catalog_syncs_nothing(mb, db_factory):
    db_id = db_factory("cs-gizmo-nocat", {**GIZMO_DETAILS, "catalog": "does_not_exist"})
    tables = mb.wait_synced_tables(db_id, timeout_s=45)
    assert tables == [], [t["name"] for t in tables]


def test_gizmo_catalog_plus_schema_filter_compose(mb, db_factory):
    """catalog=memory AND inclusion filter hr -> only hr schema tables."""
    db_id = db_factory("cs-gizmo-cat-plus-filter",
                       {**GIZMO_DETAILS, "catalog": "memory",
                        "schema-filters-type": "inclusion",
                        "schema-filters-patterns": "hr"})
    tables = mb.wait_synced_tables(db_id)
    assert tables and schemas_of(tables) == {"hr"}, schemas_of(tables)


def test_gizmo_exclusion_filter(mb, db_factory):
    db_id = db_factory("cs-gizmo-exclude",
                       {**GIZMO_DETAILS, "catalog": "memory",
                        "schema-filters-type": "exclusion",
                        "schema-filters-patterns": "hr,analytics,main"})
    tables = mb.wait_synced_tables(db_id)
    assert tables and schemas_of(tables) == {"sales"}, schemas_of(tables)


# --------------------------------------------------------------- spice ----

SPICE_APIKEY = {**SPICE_HOST, "use-token": False, "password": "1234567890"}


def test_spice_catalog_and_runtime_schema_excluded(mb, db_factory):
    """Spice datasets live in catalog `spice`, schema public; the runtime
    schema (task_history, metrics) is excluded by the driver."""
    db_id = db_factory("cs-spice-catalog", {**SPICE_APIKEY, "catalog": "spice"})
    tables = mb.wait_synced_tables(
        db_id, timeout_s=180,
        predicate=lambda ts: any(t["name"] == "yellow_taxis" for t in ts))
    assert any(t["name"] == "yellow_taxis" for t in tables), schemas_of(tables)
    assert "runtime" not in schemas_of(tables), schemas_of(tables)


def test_spice_nonexistent_catalog_syncs_nothing(mb, db_factory):
    db_id = db_factory("cs-spice-nocat", {**SPICE_APIKEY, "catalog": "does_not_exist"})
    tables = mb.wait_synced_tables(db_id, timeout_s=45)
    assert tables == [], [t["name"] for t in tables]


def test_spice_multi_schema_topology(mb, db_factory):
    """The demo pod defines schema-qualified datasets: public.yellow_taxis,
    transport.trips, finance.taxi_fares."""
    db_id = db_factory("cs-spice-multischema", {**SPICE_APIKEY})
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: len({t["schema"] for t in ts}) >= 3)
    assert {"public", "transport", "finance"} <= schemas_of(tables), schemas_of(tables)


def test_spice_exclusion_filter_removes_public(mb, db_factory):
    db_id = db_factory("cs-spice-exclude",
                       {**SPICE_APIKEY,
                        "schema-filters-type": "exclusion",
                        "schema-filters-patterns": "public"})
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: len({t["schema"] for t in ts}) >= 2)
    assert schemas_of(tables) == {"transport", "finance"}, schemas_of(tables)


def test_spice_inclusion_filter(mb, db_factory):
    db_id = db_factory("cs-spice-include-finance",
                       {**SPICE_APIKEY,
                        "schema-filters-type": "inclusion",
                        "schema-filters-patterns": "finance"})
    tables = mb.wait_synced_tables(db_id)
    assert tables and schemas_of(tables) == {"finance"}, schemas_of(tables)


# ----------------------------------------------------------- influxdb3 ----

INFLUX_BASE = {"host": "influxdb3", "port": 8181, "use-token": True,
               "additional-options": "database=demo",
               "useEncryption": False, "disableCertificateVerification": True}


@requires_influxdb3
def test_influxdb3_schema_topology_and_exclusion(mb, db_factory):
    """Unfiltered sync exposes both iox (data) and system schemas; excluding
    `system` leaves only the data schema."""
    details = {**INFLUX_BASE, "token-value": ENV["INFLUXDB3_TOKEN"]}
    db_id = db_factory("cs-influx-all", details)
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "weather" for t in ts))
    assert {"iox", "system"} <= schemas_of(tables), schemas_of(tables)

    db_id = db_factory("cs-influx-nosystem",
                       {**details, "schema-filters-type": "exclusion",
                        "schema-filters-patterns": "system"})
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "weather" for t in ts))
    assert schemas_of(tables) == {"iox"}, schemas_of(tables)


@requires_influxdb3
def test_influxdb3_inclusion_filter(mb, db_factory):
    db_id = db_factory("cs-influx-iox-only",
                       {**INFLUX_BASE, "token-value": ENV["INFLUXDB3_TOKEN"],
                        "schema-filters-type": "inclusion",
                        "schema-filters-patterns": "iox"})
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "weather" for t in ts))
    assert schemas_of(tables) == {"iox"}, schemas_of(tables)


# ------------------------------------------------------ quack-on-demand ----

# Demo-mode provisioning (from the container's startup banner):
#   acme-admin / demo-acme-admin  -> everything in tenant acme, unmasked
#   alice / demo-alice            -> RBAC demo: c_phone masked to '***',
#                                    only BUILDING-segment customer rows
# tenant/pool ride the driver's additional-options -> gRPC header passthrough.
QUACK_ADMIN = {"host": "quack-on-demand", "port": 31338, "use-token": False,
               "username": "acme-admin", "password": "demo-acme-admin",
               # db= gives the session a database context (bare SELECT 1
               # statements from restricted users fail without it)
               "additional-options": "tenant=acme&pool=bi&db=acme_tpch",
               "useEncryption": True, "disableCertificateVerification": True}

# NOTE: quack's RBAC-restricted demo user `alice` (row filter + column
# masking) has only table-scoped grants and cannot run a bare `SELECT 1`.
# Metabase's connection health-check IS `SELECT 1`, so such users can't be
# registered as Metabase databases at all — a server-RBAC + BI-tool
# interaction, not a driver issue. Only fully-privileged service accounts
# (here acme-admin) are usable from Metabase; scope permissions at that user.


@requires_quack
def test_quack_discovers_ducklake_catalog(mb, db_factory):
    """The demo tenant serves TPC-H in DuckLake catalog acme_tpch.tpch1."""
    db_id = db_factory("cs-quack-all", QUACK_ADMIN)
    res = mb.native(db_id,
                    "SELECT COUNT(*) FROM acme_tpch.tpch1.customer")
    assert res.get("status") == "completed", res.get("error")
    assert res["data"]["rows"][0][0] > 0
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "customer" for t in ts))
    assert any(t["name"] == "customer" for t in tables), schemas_of(tables)


@requires_quack
def test_quack_catalog_scoping(mb, db_factory):
    db_id = db_factory("cs-quack-catalog", {**QUACK_ADMIN, "catalog": "acme_tpch"})
    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "customer" for t in ts))
    assert tables and schemas_of(tables) <= {"tpch1", "temp", "main"}, schemas_of(tables)
