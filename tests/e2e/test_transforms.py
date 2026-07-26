"""Data Studio transforms (:transforms/table) on a writable backend.

A transform runs a saved query as CREATE TABLE AS on the warehouse. Gated by
the same per-connection "Writable backend" toggle as uploads, so read-only
backends never appear in Data Studio's compatible-database list.
"""
import time

import pytest


@pytest.fixture(scope="module")
def gizmo_writable(mb):
    """Ensure the gizmo connection advertises transforms."""
    gizmo = mb.databases()["gizmo"]
    if not gizmo["details"].get("enable-uploads"):
        status, _ = mb.req("PUT", f"/api/database/{gizmo['id']}",
                           {"details": {**gizmo["details"], "enable-uploads": True}})
        assert status == 200
    _, full = mb.get(f"/api/database/{gizmo['id']}")
    assert "transforms/table" in (full.get("features") or []), \
        "gizmo should advertise transforms/table when writable"
    return gizmo["id"]


def test_transforms_gated_by_connection(mb, gizmo_writable):
    """Read-only backends must not advertise transforms."""
    _, flight = mb.get(f"/api/database/{mb.databases()['flight']['id']}")
    assert "transforms/table" not in (flight.get("features") or [])


def test_ctas_transform_runs_and_creates_table(mb, gizmo_writable):
    gid = gizmo_writable
    _, meta = mb.get(f"/api/database/{gid}/metadata")
    orders = next(t for t in meta["tables"]
                  if t["name"] == "orders" and t["schema"] == "sales")
    fields = {f["name"]: f["id"] for f in orders["fields"]}

    target_name = "transform_revenue_by_status"
    source_query = {
        "database": gid, "type": "query",
        "query": {"source-table": orders["id"],
                  "aggregation": [["sum", ["field", fields["total_amount"], None]]],
                  "breakout": [["field", fields["status"], None]]}}

    status, tr = mb.post("/api/transform", {
        "name": "E2E revenue by status",
        "source": {"type": "query", "query": source_query},
        "target": {"type": "table", "database": gid, "schema": "main",
                   "name": target_name}})
    assert status in (200, 201), (status, tr)
    transform_id = tr["id"]
    try:
        status, _ = mb.post(f"/api/transform/{transform_id}/run")
        assert status in (200, 202), status

        # poll the transform's run state to completion
        deadline = time.time() + 90
        state = None
        while time.time() < deadline:
            _, t = mb.get(f"/api/transform/{transform_id}")
            state = (t.get("last_run") or {}).get("status")
            if state in ("succeeded", "failed"):
                break
            time.sleep(4)
        assert state == "succeeded", f"transform run state={state}"

        # the CTAS output table exists and holds the aggregated rows
        res = mb.native(gid, f'SELECT COUNT(*) FROM "main"."{target_name}"')
        assert res.get("status") == "completed"
        assert res["data"]["rows"][0][0] > 0
    finally:
        mb.req("DELETE", f"/api/transform/{transform_id}/table")
        mb.req("DELETE", f"/api/transform/{transform_id}")
        mb.native(gid, f'DROP TABLE IF EXISTS "main"."{target_name}"')
