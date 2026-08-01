"""Metabase Actions (write-back) on a full-DML Flight SQL backend (GizmoSQL).

Actions let dashboards write back to the database via buttons/forms. This
driver supports them (basic model create/update/delete + custom SQL actions)
on writable, full-DML backends — GizmoSQL/DuckDB here; Dremio and Doris also
qualify. They're gated behind the per-connection `enable-actions` toggle plus
Metabase's `database-enable-actions` setting.

The parent `:sql-jdbc` `perform-action!*` machinery is reused; the driver
supplies the Flight-SQL-specific pieces (autocommit `do-nested-transaction`,
a cast type map, and an inline-literal `select-created-row`).

Primary keys are marked explicitly (semantic type Entity Key) because PK
auto-detection varies across Flight SQL backends — see the CRUD tutorial.
"""
import time

import pytest

from conftest import ENV, GIZMO_DETAILS, port_open

requires_gizmo = pytest.mark.skipif(
    not port_open("localhost", 31337),
    reason="gizmosql not running (base stack)")

pytestmark = requires_gizmo

ACTIONS_DETAILS = {**GIZMO_DETAILS, "enable-uploads": True, "enable-actions": True}


def _native(mb, db_id, sql):
    return mb.native(db_id, sql)


@pytest.fixture()
def actions_db(mb, db_factory):
    """A GizmoSQL connection with Actions enabled (connection toggle + DB
    setting), plus a fresh auto-increment `todos` table synced with its PK
    marked. Yields (db_id, table_id, field_ids)."""
    db_id = db_factory("gizmo-actions", ACTIONS_DETAILS)
    # DB-level setting requires the :actions feature, which the fresh
    # enable-actions connection already advertises.
    status, _ = mb.req("PUT", f"/api/database/{db_id}",
                       {"settings": {"database-enable-actions": True}})
    assert status == 200

    _native(mb, db_id, "DROP TABLE IF EXISTS main.todos_t")
    _native(mb, db_id, "DROP SEQUENCE IF EXISTS main.seq_todos_t")
    _native(mb, db_id, "CREATE SEQUENCE main.seq_todos_t START 1")
    r = _native(mb, db_id, "CREATE TABLE main.todos_t ("
                "id INTEGER PRIMARY KEY DEFAULT nextval('main.seq_todos_t'), "
                "task VARCHAR, done BOOLEAN DEFAULT false)")
    assert r.get("status") == "completed", r.get("error")

    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "todos_t" for t in ts))
    t = next(t for t in tables if t["name"] == "todos_t")
    fields = {f["name"]: f["id"] for f in t["fields"]}
    # mark the PK (Entity Key) — needed for basic model actions
    mb.req("PUT", f"/api/field/{fields['id']}", {"semantic_type": "type/PK"})
    yield db_id, t["id"], fields
    _native(mb, db_id, "DROP TABLE IF EXISTS main.todos_t")


def test_actions_advertised_only_when_enabled(mb, db_factory):
    on = db_factory("gizmo-act-on", ACTIONS_DETAILS)
    _, full = mb.get(f"/api/database/{on}")
    feats = set(full.get("features") or [])
    assert {"actions", "actions/custom"} <= feats, feats

    off = db_factory("gizmo-act-off", {**GIZMO_DETAILS, "enable-uploads": True})
    _, full2 = mb.get(f"/api/database/{off}")
    assert "actions" not in (full2.get("features") or [])


def test_basic_crud_actions(mb, actions_db):
    db_id, table_id, _ = actions_db

    def rows():
        r = mb.native(db_id, "SELECT id, task, done FROM main.todos_t ORDER BY id")
        return r["data"]["rows"]

    # model + implicit create/update/delete actions
    _, card = mb.post("/api/card", {"name": "Todos model", "type": "model",
        "dataset_query": {"database": db_id, "type": "query", "query": {"source-table": table_id}},
        "display": "table", "visualization_settings": {}})
    mid = card["id"]
    acts = {}
    for kind in ("create", "update", "delete"):
        _, a = mb.post("/api/action", {"name": f"{kind} todo", "type": "implicit",
                                       "kind": f"row/{kind}", "model_id": mid, "database_id": db_id})
        acts[kind] = a["id"]

    # CREATE
    for task in ("buy milk", "walk dog"):
        status, _ = mb.post(f"/api/action/{acts['create']}/execute",
                            {"parameters": {"task": task, "done": False}})
        assert status == 200, task
    assert rows() == [[1, "buy milk", False], [2, "walk dog", False]]

    # UPDATE row 1 -> done
    status, _ = mb.post(f"/api/action/{acts['update']}/execute",
                        {"parameters": {"id": 1, "done": True}})
    assert status == 200
    assert rows() == [[1, "buy milk", True], [2, "walk dog", False]]

    # DELETE row 2
    status, _ = mb.post(f"/api/action/{acts['delete']}/execute", {"parameters": {"id": 2}})
    assert status == 200
    assert rows() == [[1, "buy milk", True]]


def test_custom_sql_action(mb, actions_db):
    """A custom (native SQL) insert action with a template-tag parameter."""
    db_id, table_id, _ = actions_db
    _, card = mb.post("/api/card", {"name": "Todos model 2", "type": "model",
        "dataset_query": {"database": db_id, "type": "query", "query": {"source-table": table_id}},
        "display": "table", "visualization_settings": {}})
    query = {"database": db_id, "type": "native",
             "native": {"query": "INSERT INTO main.todos_t (task, done) VALUES ({{task}}, false)",
                        "template-tags": {"task": {"id": "task", "name": "task",
                                                   "display-name": "Task", "type": "text"}}}}
    status, act = mb.post("/api/action", {"name": "Quick add", "type": "query",
                                          "model_id": card["id"], "database_id": db_id,
                                          "dataset_query": query,
                                          "parameters": [{"id": "task", "type": "string/=",
                                                          "target": ["variable", ["template-tag", "task"]]}]})
    assert status == 200, act
    # execute params are keyed by the parameter id
    status, _ = mb.post(f"/api/action/{act['id']}/execute", {"parameters": {"task": "custom row"}})
    assert status in (200, 204), status
    r = mb.native(db_id, "SELECT COUNT(*) FROM main.todos_t WHERE task = 'custom row'")
    assert r["data"]["rows"][0][0] == 1


def test_readonly_backend_has_no_actions(mb):
    """Spice (read-only) must not advertise actions even if the toggle is off."""
    dbs = mb.databases()
    if "flight" not in dbs:
        pytest.skip("spice (flight) connection not present")
    _, full = mb.get(f"/api/database/{dbs['flight']['id']}")
    assert "actions" not in (full.get("features") or [])
