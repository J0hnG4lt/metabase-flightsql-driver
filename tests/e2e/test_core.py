"""Core smoke: connections, dashboard shape, native queries, token storage shapes."""
from conftest import SPICE_HOST


def test_demo_connections_exist(mb):
    dbs = mb.databases()
    assert "gizmo" in dbs and "flight" in dbs


def test_dashboard_shape(mb):
    _, dashboards = mb.get("/api/dashboard")
    rows = dashboards["data"] if isinstance(dashboards, dict) and "data" in dashboards else dashboards
    target = next((d for d in rows if "FlightSQL" in (d.get("name") or "")), None)
    assert target, "test dashboard missing — run scripts/metabase_setup.py"
    _, dash = mb.get(f"/api/dashboard/{target['id']}")
    assert len(dash["dashcards"]) == 32
    assert len(dash["parameters"]) == 5


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


def test_filtered_query_via_dashboard_parameter(mb):
    _, dash = mb.get("/api/dashboard/2")
    card_id = next(dc["card_id"] for dc in dash["dashcards"]
                   for pm in dc.get("parameter_mappings") or []
                   if pm.get("parameter_id") == "status")
    _, res = mb.post(f"/api/card/{card_id}/query", {
        "parameters": [{"type": "string/=", "value": ["Delivered"],
                        "target": ["dimension", ["template-tag", "status"]]}]})
    assert res.get("status") == "completed" and len(res["data"]["rows"]) > 0
