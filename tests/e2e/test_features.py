"""Metabase feature coverage: every dashboard card, metadata refreshes, MBQL
paths, segments, v2 metrics, pivot."""
import time

import pytest


@pytest.fixture(scope="module")
def gizmo_meta(mb):
    dbs = mb.databases()
    gizmo_id = dbs["gizmo"]["id"]
    _, meta = mb.get(f"/api/database/{gizmo_id}/metadata")
    orders = next(t for t in meta["tables"]
                  if t["name"] == "orders" and t["schema"] == "sales")
    fields = {f["name"]: f for f in orders["fields"]}
    return {"db_id": gizmo_id, "orders": orders, "fields": fields}


def test_all_dashboard_cards_execute(mb):
    _, dash = mb.get("/api/dashboard/2")
    card_ids = sorted({dc["card_id"] for dc in dash["dashcards"] if dc.get("card_id")})
    assert len(card_ids) >= 30
    fails = []
    for cid in card_ids:
        _, res = mb.post(f"/api/card/{cid}/query")
        if not res or res.get("status") != "completed":
            fails.append((cid, str((res or {}).get("error"))[:80]))
    assert not fails, fails


def test_metadata_refreshes_succeed(mb):
    for name in ("gizmo", "flight"):
        db = mb.databases()[name]
        _, before = mb.get(f"/api/database/{db['id']}/metadata")
        n_before = len(before.get("tables") or [])
        s1, _ = mb.post(f"/api/database/{db['id']}/sync_schema")
        s2, _ = mb.post(f"/api/database/{db['id']}/rescan_values")
        assert s1 == 200 and s2 == 200
        deadline = time.time() + 120
        while time.time() < deadline:
            time.sleep(8)
            _, meta = mb.get(f"/api/database/{db['id']}/metadata")
            _, info = mb.get(f"/api/database/{db['id']}")
            if (meta.get("tables") and
                    info.get("initial_sync_status") == "complete"):
                break
        # note: the count may legitimately SHRINK vs the pre-refresh metadata
        # (e.g. in-memory backends reset on container restart retire tables)
        assert len(meta.get("tables") or []) > 0 and n_before > 0
        assert info["initial_sync_status"] == "complete"


def test_mbql_temporal_breakout_and_date_filter(mb, gizmo_meta):
    f = gizmo_meta["fields"]["order_date"]
    _, res = mb.post("/api/dataset", {
        "type": "query", "database": gizmo_meta["db_id"],
        "query": {"source-table": gizmo_meta["orders"]["id"],
                  "aggregation": [["count"]],
                  "breakout": [["field", f["id"], {"temporal-unit": "month"}]],
                  "filter": ["between", ["field", f["id"], None],
                             "2024-01-01", "2024-12-31"]}})
    assert res.get("status") == "completed" and res["data"]["rows"]


def test_mbql_relative_date_filter(mb, gizmo_meta):
    f = gizmo_meta["fields"]["order_date"]
    _, res = mb.post("/api/dataset", {
        "type": "query", "database": gizmo_meta["db_id"],
        "query": {"source-table": gizmo_meta["orders"]["id"],
                  "aggregation": [["count"]],
                  "filter": ["time-interval", ["field", f["id"], None], -10, "year"]}})
    assert res.get("status") == "completed"


def test_segment_roundtrip(mb, gizmo_meta):
    status_f = gizmo_meta["fields"]["status"]
    status, seg = mb.post("/api/segment", {
        "name": "e2e-delivered", "table_id": gizmo_meta["orders"]["id"],
        "definition": {"source-table": gizmo_meta["orders"]["id"],
                       "filter": ["=", ["field", status_f["id"], None], "Delivered"]}})
    assert status == 200 and seg.get("id")
    try:
        _, res = mb.post("/api/dataset", {
            "type": "query", "database": gizmo_meta["db_id"],
            "query": {"source-table": gizmo_meta["orders"]["id"],
                      "aggregation": [["count"]],
                      "filter": ["segment", seg["id"]]}})
        assert res.get("status") == "completed"
        assert res["data"]["rows"][0][0] == 6
    finally:
        mb.req("DELETE", f"/api/segment/{seg['id']}",
               {"revision_message": "cleanup"})


def test_metric_v2_roundtrip(mb, gizmo_meta):
    total_f = gizmo_meta["fields"]["total_amount"]
    status, metric = mb.post("/api/card", {
        "name": "e2e-total-revenue", "type": "metric", "display": "scalar",
        "visualization_settings": {},
        "dataset_query": {"type": "query", "database": gizmo_meta["db_id"],
                          "query": {"source-table": gizmo_meta["orders"]["id"],
                                    "aggregation": [["sum", ["field", total_f["id"], None]]]}}})
    assert status in (200, 202) and metric.get("id")
    try:
        _, res = mb.post(f"/api/card/{metric['id']}/query")
        assert res.get("status") == "completed"
        assert res["data"]["rows"][0][0] is not None
    finally:
        mb.req("DELETE", f"/api/card/{metric['id']}")


def test_pivot_endpoint(mb, gizmo_meta):
    _, meta = mb.get(f"/api/database/{gizmo_meta['db_id']}/metadata")
    cust = next(t for t in meta["tables"] if t["name"] == "customers")
    fields = {f["name"]: f for f in cust["fields"]}
    _, res = mb.post("/api/dataset/pivot", {
        "type": "query", "database": gizmo_meta["db_id"],
        "query": {"source-table": cust["id"],
                  "aggregation": [["count"]],
                  "breakout": [["field", fields["country"]["id"], None],
                               ["field", fields["city"]["id"], None]]}})
    assert res.get("status") == "completed"
