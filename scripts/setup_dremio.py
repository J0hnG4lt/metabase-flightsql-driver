"""Bootstrap a single-node Dremio (docker-compose.dremio.yaml) and seed sample
Apache Iceberg tables, then mark it usable for the pytest e2e suite.

What it does:
  1. waits for the Dremio web/REST server on :9047
  2. creates the first admin user (idempotent)
  3. logs in for a session token
  4. adds a writable NAS source `wh` -> /warehouse
  5. CTAS three Iceberg tables across folders sales/hr/analytics (Dremio writes
     Iceberg format by default for filesystem sources), so they surface as
     Metabase schemas `wh.sales` / `wh.hr` / `wh.analytics`
  6. creates a Personal Access Token (for the Bearer/use-token auth test)
  7. writes DREMIO_READY=1 (and DREMIO_PAT=<pat>) to .env

Usage: python scripts/setup_dremio.py
Requires the dremio service running (docker-compose.dremio.yaml).

Unlike Doris/StarRocks (Java FE + separate C++ BE, all-in-one image advertises
a 127.0.0.1 Flight endpoint), Dremio single-node is one JVM that serves Flight
SQL results inline — so it works from a separate Metabase container everywhere.
"""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://localhost:9047"
ADMIN_USER = "dremio"
ADMIN_PASS = "dremio123"  # Dremio requires >=8 chars incl. a letter and a digit
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
CLI = os.environ.get("CONTAINER_CLI") or ("podman" if shutil.which("podman") else "docker")

# Iceberg tables to create (name -> the SELECT that CTAS materializes). Inline
# VALUES keep this self-contained (no external files/schema coupling).
TABLES = {
    "wh.sales.orders":
        "SELECT * FROM (VALUES "
        "(1,'widget',9.99),(2,'gadget',19.99),(3,'gizmo',4.50),(4,'sprocket',12.00),"
        "(5,'cog',7.25),(6,'lever',3.10),(7,'pulley',15.75),(8,'valve',22.40)"
        ") AS t(id, item, price)",
    "wh.hr.employees":
        "SELECT * FROM (VALUES "
        "(1,'Ada','Engineering',120000),(2,'Linus','Engineering',110000),"
        "(3,'Grace','Research',130000),(4,'Alan','Research',125000),"
        "(5,'Edsger','Ops',115000)"
        ") AS t(id, name, dept, salary)",
    "wh.analytics.events":
        "SELECT * FROM (VALUES "
        "(1,'click',100),(2,'view',250),(3,'purchase',12),(4,'signup',33)"
        ") AS t(id, kind, cnt)",
}
EXPECTED_ORDERS = 8


def _req(method, path, body=None, token=None, timeout=60):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "_dremio" + token
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else None)
        except Exception:
            return e.code, raw.decode(errors="replace")[:300]
    except urllib.error.URLError as e:
        return 0, str(e)


def wait_up():
    """Dremio's first boot (metadata store init) can take 1-2 minutes."""
    print("waiting for Dremio REST on :9047 (first boot can take ~1-2 min)...")
    for _ in range(90):
        status, _ = _req("GET", "/apiv2/server_status")
        if status == 200:
            return True
        time.sleep(5)
    return False


def container_sh(args, as_root=False, check=False):
    base = [CLI, "exec"] + (["-u", "root"] if as_root else []) + ["dremio"]
    return subprocess.run(base + args, capture_output=True, text=True, check=check)


def bootstrap_admin():
    body = {"userName": ADMIN_USER, "firstName": "Dre", "lastName": "Mio",
            "email": "dremio@example.com", "createdAt": int(time.time() * 1000),
            "password": ADMIN_PASS}
    # The first-user call requires the literal `_dremionull` auth header.
    status, res = _req("PUT", "/apiv2/bootstrap/firstuser", body, token="null")
    if status == 200:
        print("admin user created")
    else:
        # Already bootstrapped on a previous run — fine, we'll just log in.
        print(f"firstuser bootstrap returned {status} (already set up? continuing)")


def login():
    status, res = _req("POST", "/apiv2/login",
                       {"userName": ADMIN_USER, "password": ADMIN_PASS})
    if status == 200 and isinstance(res, dict) and res.get("token"):
        return res["token"]
    sys.exit(f"login failed: {status} {res}")


def create_source(token, name, path):
    body = {"entityType": "source", "type": "NAS", "name": name,
            "config": {"path": path}}
    status, res = _req("POST", "/api/v3/catalog", body, token=token)
    if status in (200, 201):
        print(f"source {name} -> {path} created")
    elif status == 409 or (isinstance(res, dict) and "already" in str(res).lower()):
        print(f"source {name} already exists")
    else:
        print(f"source {name}: unexpected {status} {res}")


def run_sql(token, statement, timeout=180):
    status, res = _req("POST", "/api/v3/sql", {"sql": statement}, token=token)
    if status not in (200, 201) or not isinstance(res, dict) or "id" not in res:
        return False, f"submit failed: {status} {res}"
    job = res["id"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, j = _req("GET", f"/api/v3/job/{job}", token=token)
        state = (j or {}).get("jobState")
        if state == "COMPLETED":
            return True, job
        if state in ("FAILED", "CANCELED", "CANCELLATION_REQUESTED"):
            return False, (j or {}).get("errorMessage", f"job {state}")
        time.sleep(2)
    return False, "job timeout"


def query_scalar(token, sql):
    ok, job = run_sql(token, sql)
    if not ok:
        return None
    _, res = _req("GET", f"/api/v3/job/{job}/results?limit=1", token=token)
    try:
        row = res["rows"][0]
        return list(row.values())[0]
    except Exception:
        return None


def create_pat(token):
    status, u = _req("GET", f"/api/v3/user/by-name/{ADMIN_USER}", token=token)
    if status != 200 or not isinstance(u, dict) or "id" not in u:
        print(f"could not resolve user id ({status}); skipping PAT")
        return None
    status, r = _req("POST", f"/api/v3/user/{u['id']}/token",
                     {"label": "metabase-e2e",
                      "millisecondsToExpire": 7 * 24 * 3600 * 1000}, token=token)
    if status in (200, 201) and isinstance(r, dict) and r.get("token"):
        print("PAT created (DREMIO_PAT)")
        return r["token"]
    # Dremio OSS does not expose the PAT REST endpoint (404) — it's an
    # Enterprise/Cloud feature. Expected; the token test skips.
    print(f"PAT creation returned {status} (Dremio OSS has no PAT endpoint); "
          "token-auth test will skip")
    return None


def _set_env_flag(key, value):
    lines = ENV_PATH.read_text().splitlines() if ENV_PATH.exists() else []
    lines = [l for l in lines if not l.startswith(f"{key}=")]
    lines.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(lines) + "\n")


def main():
    if not wait_up():
        sys.exit("Dremio web/REST never came up on :9047")

    # Make the warehouse writable by the dremio uid. Then clear any stale
    # Iceberg files from a previous run and re-create the schema folders (as the
    # dremio user) so nested CTAS can resolve them. The clear step keeps this
    # script idempotent: Dremio's metadata store is not persisted by this
    # profile, so a restart forgets the catalog while the warehouse volume keeps
    # the files — without the clear, CTAS fails with "Folder already exists".
    container_sh(["chmod", "-R", "777", "/warehouse"], as_root=True)
    container_sh(["rm", "-rf", "/warehouse/sales", "/warehouse/hr",
                  "/warehouse/analytics"], as_root=True)
    container_sh(["mkdir", "-p", "/warehouse/sales", "/warehouse/hr",
                  "/warehouse/analytics"])

    bootstrap_admin()
    token = login()
    create_source(token, "wh", "/warehouse")

    ok_all = True
    for name, select in TABLES.items():
        ok, info = run_sql(token, f"CREATE TABLE {name} AS {select}")
        print(f"CTAS {name}: {'ok' if ok else 'FAILED -> ' + str(info)}")
        ok_all = ok_all and ok

    n = query_scalar(token, "SELECT COUNT(*) FROM wh.sales.orders")
    print(f"wh.sales.orders row count: {n}")
    if not ok_all or n != EXPECTED_ORDERS:
        sys.exit("Dremio seed did not complete cleanly (see CTAS output above)")

    pat = create_pat(token)

    _set_env_flag("DREMIO_READY", "1")
    if pat:
        _set_env_flag("DREMIO_PAT", pat)
    print("Dremio ready: host=dremio port=32010 (Arrow Flight SQL), "
          f"user={ADMIN_USER}, pass={ADMIN_PASS}")
    print("  schemas: wh.sales / wh.hr / wh.analytics (Iceberg tables)")
    print(f"  DREMIO_READY=1{' + DREMIO_PAT' if pat else ''} written to .env")


if __name__ == "__main__":
    main()
