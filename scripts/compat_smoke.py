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
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://localhost:3000"


def api_key():
    env = Path(__file__).resolve().parent.parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("METABASE_API_KEY="):
                return line.split("=", 1)[1].strip()
    sys.exit("METABASE_API_KEY not found in .env (run ci_bootstrap.py first)")


KEY = api_key()


def req(method, path, body=None, timeout=120):
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

    # 3) sync + query
    req("POST", f"/api/database/{db_id}/sync_schema")
    rows = None
    deadline = time.time() + 120
    while time.time() < deadline:
        time.sleep(6)
        status, res = req("POST", "/api/dataset",
                          {"type": "native", "database": db_id,
                           "native": {"query": "SELECT COUNT(*) AS n FROM sales.orders"}})
        if isinstance(res, dict) and res.get("status") == "completed":
            rows = res["data"]["rows"]
            break
    if not rows:
        sys.exit("FAIL: query did not complete — driver loaded but could not run a query")
    print(f"OK: query returned {rows} — the jar works on this Metabase version")


if __name__ == "__main__":
    main()
