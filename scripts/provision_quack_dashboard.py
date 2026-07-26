"""Register the quack-on-demand + influxdb3 connections and build a TPC-H
sample dashboard on quack. Run after scripts/metabase_setup.py (which creates
the admin user, gizmo/spice connections and their dashboards) on a running
Metabase 63 stack.
"""
import json
import os
import time
import urllib.error
import urllib.request

BASE = "http://localhost:3000"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = dict(l.strip().split("=", 1) for l in open(os.path.join(REPO, ".env"))
           if "=" in l and not l.startswith("#"))
KEY = ENV["METABASE_API_KEY"]


def req(method, path, body=None, timeout=180):
    r = urllib.request.Request(
        BASE + path, method=method,
        headers={"x-api-key": KEY, "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


def databases():
    _, dbs = req("GET", "/api/database")
    rows = dbs["data"] if isinstance(dbs, dict) and "data" in dbs else dbs
    return {d["name"]: d for d in rows}


def ensure_connection(name, details):
    existing = databases().get(name)
    if existing:
        print(f"  {name}: already present (id {existing['id']})")
        return existing["id"]
    status, res = req("POST", "/api/database",
                      {"name": name, "engine": "arrow-flight-sql", "details": details})
    if status == 200 and res.get("id"):
        print(f"  {name}: registered (id {res['id']})")
        return res["id"]
    print(f"  {name}: FAILED {status} {str(res)[:160]}")
    return None


def wait_tables(db_id, want, timeout_s=180):
    req("POST", f"/api/database/{db_id}/sync_schema")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(6)
        _, meta = req("GET", f"/api/database/{db_id}/metadata")
        tables = meta.get("tables") or []
        if any(t["name"] == want for t in tables):
            return tables
    return []


print("== registering extra backends ==")
influx = ensure_connection("influxdb3", {
    "host": "influxdb3", "port": 8181, "use-token": True,
    "token-value": ENV.get("INFLUXDB3_TOKEN", ""),
    "additional-options": "database=demo",
    "schema-filters-type": "exclusion", "schema-filters-patterns": "system",
    "useEncryption": False, "disableCertificateVerification": True,
}) if ENV.get("INFLUXDB3_TOKEN") else None

quack = ensure_connection("quack-on-demand", {
    "host": "quack-on-demand", "port": 31338, "use-token": False,
    "username": "acme-admin", "password": "demo-acme-admin",
    "additional-options": "tenant=acme&pool=bi&db=acme_tpch",
    "useEncryption": True, "disableCertificateVerification": True,
})

if not quack:
    raise SystemExit("quack-on-demand connection failed; is the service up?")

print("== enabling writes (uploads + transforms) on gizmo ==")
gizmo = databases().get("gizmo")
if gizmo and not gizmo["details"].get("enable-uploads"):
    req("PUT", f"/api/database/{gizmo['id']}",
        {"details": {**gizmo["details"], "enable-uploads": True}})
    req("PUT", "/api/setting/uploads-settings",
        {"value": {"db_id": gizmo["id"], "schema_name": "main", "table_prefix": None}})
    print("  gizmo: uploads + transforms enabled, upload target = main")

print("== syncing quack TPC-H ==")
tables = wait_tables(quack, "customer")
if not tables:
    raise SystemExit("quack sync produced no tables")
by = {(t["schema"], t["name"]): t for t in tables}
print(f"  quack tables: {sorted(t['name'] for t in tables)}")


def col(schema, table, name):
    t = by.get((schema, table))
    if not t:
        return None
    _, full = req("GET", f"/api/table/{t['id']}/query_metadata")
    return next((f["id"] for f in full.get("fields", []) if f["name"] == name), None)


def find_table(name):
    return next((t for t in tables if t["name"] == name), None)


cust = find_table("customer")
orders = find_table("orders")
lineitem = find_table("lineitem")
nation = find_table("nation")

cards = []


def card(name, display, query, viz=None):
    status, res = req("POST", "/api/card", {
        "name": name, "display": display, "type": "question",
        "visualization_settings": viz or {},
        "dataset_query": {"type": "query", "database": quack, "query": query}})
    if status in (200, 202) and res.get("id"):
        cards.append(res["id"])
        print(f"  card: {name} (id {res['id']})")
        return res["id"]
    print(f"  card FAILED {name}: {status} {str(res)[:150]}")
    return None


def native(name, sql, display="table", viz=None):
    status, res = req("POST", "/api/card", {
        "name": name, "display": display, "type": "question",
        "visualization_settings": viz or {},
        "dataset_query": {"type": "native", "database": quack,
                          "native": {"query": sql}}})
    if status in (200, 202) and res.get("id"):
        cards.append(res["id"])
        print(f"  card: {name} (id {res['id']})")
        return res["id"]
    print(f"  card FAILED {name}: {status} {str(res)[:150]}")
    return None


print("== building quack TPC-H dashboard ==")
if cust:
    seg_field = col("tpch1", "customer", "c_mktsegment")
    if seg_field:
        card("Customers by market segment", "bar",
             {"source-table": cust["id"], "aggregation": [["count"]],
              "breakout": [["field", seg_field, None]]},
             {"graph.dimensions": ["c_mktsegment"], "graph.metrics": ["count"]})
    acct_field = col("tpch1", "customer", "c_acctbal")
    if acct_field and seg_field:
        card("Avg account balance by segment", "row",
             {"source-table": cust["id"],
              "aggregation": [["avg", ["field", acct_field, None]]],
              "breakout": [["field", seg_field, None]]})

native("Total customers (scalar)", "SELECT COUNT(*) AS n FROM acme_tpch.tpch1.customer",
       "scalar")
native("Revenue by order priority", """
    SELECT o_orderpriority, ROUND(SUM(o_totalprice)) AS revenue
    FROM acme_tpch.tpch1.orders
    GROUP BY o_orderpriority ORDER BY o_orderpriority
""", "bar", {"graph.dimensions": ["o_orderpriority"], "graph.metrics": ["revenue"]})
native("Orders per month (2024)", """
    SELECT date_trunc('month', o_orderdate) AS month, COUNT(*) AS orders
    FROM acme_tpch.tpch1.orders
    WHERE o_orderdate >= DATE '1995-01-01' AND o_orderdate < DATE '1996-01-01'
    GROUP BY 1 ORDER BY 1
""", "line", {"graph.dimensions": ["month"], "graph.metrics": ["orders"],
              "graph.x_axis.scale": "timeseries"})
native("Top 10 nations by customer count", """
    SELECT n.n_name AS nation, COUNT(*) AS customers
    FROM acme_tpch.tpch1.customer c
    JOIN acme_tpch.tpch1.nation n ON c.c_nationkey = n.n_nationkey
    GROUP BY 1 ORDER BY customers DESC LIMIT 10
""", "row")
native("Shipped quantity by ship mode", """
    SELECT l_shipmode, SUM(l_quantity) AS qty
    FROM acme_tpch.tpch1.lineitem
    GROUP BY l_shipmode ORDER BY qty DESC
""", "pie")
native("Total order value (scalar)", "SELECT ROUND(SUM(o_totalprice)) AS total FROM acme_tpch.tpch1.orders",
       "scalar")

cards = [c for c in cards if c]

print("== creating dashboard ==")
status, dash = req("POST", "/api/dashboard", {
    "name": "TPC-H on quack-on-demand (DuckLake)",
    "description": "Multi-tenant DuckLake lakehouse (acme_tpch.tpch1) served over Arrow Flight SQL"})
dash_id = dash["id"]
dashcards = []
for n, cid in enumerate(cards):
    _, c = req("GET", f"/api/card/{cid}")
    dashcards.append({"id": -(n + 1), "card_id": cid,
                      "row": (n // 3) * 6, "col": (n % 3) * 8,
                      "size_x": 8, "size_y": 6, "series": [],
                      "parameter_mappings": [],
                      "visualization_settings": c.get("visualization_settings", {})})
req("PUT", f"/api/dashboard/{dash_id}", {"dashcards": dashcards})
print(f"  dashboard {dash_id}: {len(dashcards)} cards")

print("== verifying quack cards ==")
fails = []
for cid in cards:
    _, res = req("POST", f"/api/card/{cid}/query")
    if not res or res.get("status") != "completed":
        fails.append((cid, str((res or {}).get("error"))[:80]))
print("FAILURES:" if fails else f"all {len(cards)} quack cards OK", fails or "")
print(f"\nquack dashboard: {BASE}/dashboard/{dash_id}")
