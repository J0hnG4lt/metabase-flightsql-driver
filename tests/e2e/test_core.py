"""Core smoke: connections, dashboard shape, native queries, token storage shapes."""
from conftest import SPICE_HOST


def test_demo_connections_exist(mb):
    dbs = mb.databases()
    assert "gizmo" in dbs and "flight" in dbs


def test_dashboard_shape(demo_dashboard):
    assert len(demo_dashboard["dashcards"]) == 32
    assert len(demo_dashboard["parameters"]) == 5


def test_native_queries_both_backends(mb):
    dbs = mb.databases()
    res = mb.native(dbs["gizmo"]["id"], "SELECT COUNT(*) FROM sales.orders")
    assert res.get("status") == "completed" and res["data"]["rows"][0][0] > 0
    res = mb.native(dbs["flight"]["id"], "SELECT COUNT(*) FROM yellow_taxis")
    assert res.get("status") == "completed" and res["data"]["rows"][0][0] > 0


def test_ui_style_token_value_connection(mb, db_factory):
    """Regression: secret-typed token arrives as token-value from the UI."""
    db_id = db_factory("t-ui-token", {**SPICE_HOST, "use-token": True,
                                      "token-value": "1234567890"})
    res = mb.native(db_id, "SELECT 1 AS ok")
    assert res.get("status") == "completed", res.get("error")


def test_legacy_plain_token_connection(mb, db_factory):
    """Back-compat: REST-created details with a plain :token key still work."""
    db_id = db_factory("t-legacy-token", {**SPICE_HOST, "token": "1234567890"})
    res = mb.native(db_id, "SELECT 1 AS ok")
    assert res.get("status") == "completed", res.get("error")


def test_filtered_query_via_dashboard_parameter(mb, demo_dashboard):
    card_id, mapping = next((dc["card_id"], pm) for dc in demo_dashboard["dashcards"]
                            for pm in dc.get("parameter_mappings") or []
                            if pm.get("parameter_id") == "status")
    parameter = next(p for p in demo_dashboard["parameters"] if p["id"] == mapping["parameter_id"])
    status, res = mb.post(f"/api/card/{card_id}/query", {
        "parameters": [{"id": parameter["id"], "type": parameter["type"], "value": ["Delivered"],
                        "target": mapping["target"]}]})
    assert status in (200, 202) and res.get("status") == "completed", (status, res)
    assert res["data"]["rows"]
