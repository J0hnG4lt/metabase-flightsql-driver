"""Compatibility smoke test: prove the built driver jar loads and works on the
Metabase version currently running.

Assumes (see .github/workflows/build.yaml `compat-smoke`):
  - Metabase is up on http://localhost:3000 with the jar in /plugins
  - a GizmoSQL backend is reachable from Metabase as gizmosql:31337
  - .env holds METABASE_API_KEY (written by ci_bootstrap.py)

Checks, in order:
  1. the `arrow-flight-sql` engine is registered  -> the jar LOADED
  2. a GizmoSQL connection can be created          -> auth/connect works
  3. sync + a query return rows                    -> the runtime works

Exits non-zero (with a clear message) on any failure.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path

BASE = os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/")


@lru_cache(maxsize=1)
def api_key():
    if os.environ.get("METABASE_API_KEY"):
        return os.environ["METABASE_API_KEY"]
    env = Path(__file__).resolve().parent.parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("METABASE_API_KEY="):
                return line.split("=", 1)[1].strip()
    sys.exit("METABASE_API_KEY not found in .env (run ci_bootstrap.py first)")


def req(method, path, body=None, timeout=120):
    r = urllib.request.Request(
        BASE + path, method=method,
        headers={"x-api-key": api_key(), "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


def main():
    # 1) the plugin must have registered its engine
    _, props = req("GET", "/api/session/properties")
    engines = (props or {}).get("engines") or {}
    if "arrow-flight-sql" not in engines:
        sys.exit("FAIL: `arrow-flight-sql` engine is not registered — the driver "
                 "jar did not load on this Metabase version")
    print("OK: arrow-flight-sql driver loaded and registered")

    # 2) create a connection to GizmoSQL
    details = {"host": "gizmosql", "port": 31337, "use-token": False,
               "username": "gizmosql", "password": "gizmosql_password",
               "useEncryption": False, "disableCertificateVerification": True}
    status, db = req("POST", "/api/database",
                     {"name": "smoke-gizmo", "engine": "arrow-flight-sql",
                      "details": details})
    if status != 200 or not isinstance(db, dict) or not db.get("id"):
        sys.exit(f"FAIL: could not create connection: status={status} resp={str(db)[:300]}")
    db_id = db["id"]
    print(f"OK: connection created (database {db_id})")

    try:
        # Sync must actually expose table/field metadata, not just accept the request.
        req("POST", f"/api/database/{db_id}/sync_schema")
        orders = None
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            _, metadata = req("GET", f"/api/database/{db_id}/metadata")
            orders = next((t for t in (metadata or {}).get("tables", [])
                           if t["name"] == "orders" and t["schema"] == "sales"
                           and t.get("fields")), None)
            if orders:
                break
            time.sleep(3)
        if not orders:
            sys.exit("FAIL: sales.orders metadata did not sync")
        fields = {f["name"]: f for f in orders["fields"]}
        print("OK: sales.orders table and field metadata synced")

        def query(body, expected, label):
            status, result = req("POST", "/api/dataset", dict(body, database=db_id))
            if (status != 202 and status != 200) or not isinstance(result, dict) or result.get("status") != "completed":
                sys.exit(f"FAIL: {label}: status={status}, result={str(result)[:500]}")
            rows = result["data"]["rows"]
            if rows != expected:
                sys.exit(f"FAIL: {label}: expected {expected}, got {rows}")
            print(f"OK: {label}: {rows}")

        query({"type": "native", "native": {"query": "SELECT COUNT(*) FROM sales.orders"}},
              [[10]], "native known-answer count")
        query({"type": "query", "query": {
            "source-table": orders["id"], "aggregation": [["count"]],
            "filter": ["between", ["field", fields["order_date"]["id"], None],
                       "2024-03-01", "2024-03-07"]}}, [[5]], "MBQL absolute-date filter")
        query({"type": "query", "query": {
            "source-table": orders["id"], "aggregation": [["count"]],
            "filter": ["=", ["field", fields["status"]["id"], None], "Delivered"]}},
              [[6]], "MBQL text filter")
        print("OK: driver runtime compatibility checks passed")
    finally:
        status, _ = req("DELETE", f"/api/database/{db_id}")
        if status not in (200, 204):
            raise RuntimeError(f"Could not clean up smoke database {db_id}: HTTP {status}")


if __name__ == "__main__":
    main()
