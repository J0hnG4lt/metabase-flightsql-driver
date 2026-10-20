"""Shared fixtures for the driver e2e suite.

Requires a running compose stack (podman-compose up -d) that has been set up
with scripts/metabase_setup.py (which writes METABASE_API_KEY to .env).
Optional stacks enable extra tests when present:
  - TLS profile (docker-compose.tls.yaml + scripts/generate_tls_certs.sh)
  - influxdb3 (seeded via scripts/setup_influxdb3.py)
"""
import json
import os
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BASE = os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/")


def load_env():
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        pytest.exit(".env not found — run scripts/metabase_setup.py first", 1)
    out = {}
    for line in env_path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.strip().split("=", 1)
            out[k] = v
    return out


ENV = load_env()


def port_open(host, port, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def pytest_addoption(parser):
    parser.addoption("--require-backend", action="append", default=[],
                     choices=("influxdb3", "dremio"),
                     help="Fail setup instead of skipping an unavailable backend; repeatable.")


def pytest_sessionstart(session):
    for backend in session.config.getoption("--require-backend"):
        if backend == "influxdb3":
            ready = bool(ENV.get("INFLUXDB3_TOKEN")) and port_open("localhost", 8181)
        else:
            ready = ENV.get("DREMIO_READY") == "1" and port_open("localhost", 32010)
        if not ready:
            raise pytest.UsageError(
                f"Required backend {backend} is not ready; run scripts/setup_{backend}.py "
                "and inspect the container logs. Refusing to skip its tests.")


class MetabaseClient:
    def __init__(self, api_key):
        self.api_key = api_key

    def req(self, method, path, body=None, timeout=180):
        r = urllib.request.Request(
            BASE + path, method=method,
            headers={"x-api-key": self.api_key, "Content-Type": "application/json"},
            data=json.dumps(body).encode() if body is not None else None)
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as e:
            body = e.read()
            try:
                return e.code, json.loads(body or b"null")
            except (ValueError, UnicodeDecodeError):
                return e.code, {"error": body.decode("utf-8", errors="replace")}

    def get(self, path):
        return self.req("GET", path)

    def post(self, path, body=None):
        return self.req("POST", path, body)

    def native(self, db_id, sql):
        _, res = self.post("/api/dataset", {"type": "native", "database": db_id,
                                            "native": {"query": sql}})
        return res or {}

    def databases(self):
        _, dbs = self.get("/api/database")
        rows = dbs["data"] if isinstance(dbs, dict) and "data" in dbs else dbs
        return {d["name"]: d for d in rows}

    def wait_synced_tables(self, db_id, timeout_s=120, predicate=None):
        self.post(f"/api/database/{db_id}/sync_schema")
        deadline = time.time() + timeout_s
        tables = []
        while time.time() < deadline:
            time.sleep(6)
            _, meta = self.get(f"/api/database/{db_id}/metadata")
            tables = meta.get("tables") or []
            if tables and (predicate is None or predicate(tables)):
                return tables
        return tables


@pytest.fixture(scope="session")
def mb():
    return MetabaseClient(ENV["METABASE_API_KEY"])


@pytest.fixture(scope="session")
def demo_dashboard(mb):
    status, dashboards = mb.get("/api/dashboard")
    assert status == 200, dashboards
    rows = dashboards["data"] if isinstance(dashboards, dict) else dashboards
    matches = [d for d in rows if d.get("name") == "FlightSQL Driver Test Dashboard v2"]
    assert matches, "test dashboard missing — run scripts/metabase_setup.py"
    # Setup can run repeatedly without resetting the application database.
    latest = max(matches, key=lambda d: d["id"])
    status, dashboard = mb.get(f"/api/dashboard/{latest['id']}")
    assert status == 200, dashboard
    return dashboard


@pytest.fixture()
def db_factory(mb):
    """Create throwaway driver connections; deletes them after the test."""
    created = []

    def create(name, details, expect_ok=True):
        status, res = mb.post("/api/database", {
            "name": name, "engine": "arrow-flight-sql", "details": details})
        ok = status == 200 and isinstance(res, dict) and res.get("id")
        if ok:
            created.append(res["id"])
        if expect_ok:
            assert ok, f"connection {name} rejected: status={status} resp={str(res)[:300]}"
            return res["id"]
        return res.get("id") if ok else None

    yield create
    for db_id in created:
        mb.req("DELETE", f"/api/database/{db_id}")


GIZMO_DETAILS = {"host": "gizmosql", "port": 31337, "username": "gizmosql",
                 "password": "gizmosql_password", "use-token": False,
                 "useEncryption": False, "disableCertificateVerification": True}

SPICE_HOST = {"host": "spiced-container", "port": 50051,
              "useEncryption": False, "disableCertificateVerification": True}

requires_tls_stack = pytest.mark.skipif(
    not (REPO_ROOT / "tls" / "ca-cert.pem").exists() or not port_open("localhost", 31338),
    reason="TLS profile not running (scripts/generate_tls_certs.sh + docker-compose.tls.yaml)")

requires_influxdb3 = pytest.mark.skipif(
    "INFLUXDB3_TOKEN" not in ENV or not port_open("localhost", 8181),
    reason="influxdb3 not running/seeded (scripts/setup_influxdb3.py)")

requires_spiced_anon = pytest.mark.skipif(
    not port_open("localhost", 50052),
    reason="spiced-anon service not running")
