"""Exercise Metabase 64's official MCP and pinned CLI through Flight SQL.

Run after ci_bootstrap.py on an isolated development instance. Authenticates
with that instance's test API key; does not perform a human OAuth consent flow.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import requests

from compat_smoke import BASE, api_key, req
from dev import ROOT


def rpc_payload(text, request_id):
    """Decode JSON or an SSE message (including multi-line data records)."""
    if text.lstrip().startswith("{"):
        return json.loads(text)
    for event in text.replace("\r\n", "\n").split("\n\n"):
        data = "\n".join(line[5:].lstrip() for line in event.splitlines() if line.startswith("data:"))
        if data:
            payload = json.loads(data)
            if payload.get("id") == request_id:
                return payload
    raise RuntimeError("MCP response did not contain the requested JSON-RPC message")


class McpClient:
    def __init__(self, base, key):
        self.url = base + "/api/metabase-mcp"
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": key, "Accept": "application/json, text/event-stream"})
        self.counter = 0

    def call(self, method, params, notification=False):
        body = {"jsonrpc": "2.0", "method": method, "params": params}
        if not notification:
            self.counter += 1
            body["id"] = self.counter
        response = self.session.post(self.url, json=body, timeout=120)
        response.raise_for_status()
        if response.headers.get("Mcp-Session-Id"):
            self.session.headers["Mcp-Session-Id"] = response.headers["Mcp-Session-Id"]
        if notification:
            return None
        payload = rpc_payload(response.text, self.counter)
        if "error" in payload:
            raise RuntimeError(f"MCP {method} failed: {payload['error']}")
        return payload["result"]

    def initialize(self):
        result = self.call("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                          "clientInfo": {"name": "flightsql-compat", "version": "1"}})
        self.session.headers["MCP-Protocol-Version"] = result["protocolVersion"]
        self.call("notifications/initialized", {}, notification=True)


def run_cli(arguments):
    env = dict(os.environ, METABASE_URL=BASE, METABASE_API_KEY=api_key())
    result = subprocess.run([sys.executable, str(ROOT / "scripts/metabase_cli.py"), *arguments],
                            cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", timeout=180)
    if result.returncode:
        # Avoid echoing untrusted command output with possible credential contents.
        raise RuntimeError(f"Official CLI failed (exit {result.returncode}); rerun the command locally for details")
    return json.loads(result.stdout)


def main():
    status, props = req("GET", "/api/session/properties")
    if status != 200 or not props.get("version", {}).get("tag", "").startswith("v0.64."):
        sys.exit("This agent integration smoke targets Metabase 64")
    # Check the server advertises OAuth, independently of test API-key auth.
    discovery = requests.get(BASE + "/.well-known/oauth-authorization-server", timeout=30)
    discovery.raise_for_status()
    oauth = discovery.json()
    for name in ("authorization_endpoint", "token_endpoint", "registration_endpoint"):
        if not oauth.get(name):
            raise RuntimeError(f"OAuth discovery is missing {name}")
    print("OK: native OAuth discovery")
    mcp = McpClient(BASE, api_key())
    mcp.initialize()
    tools = mcp.call("tools/list", {})["tools"]
    names = {tool["name"] for tool in tools}
    if not {"execute_sql", "execute_query", "learn"} <= names:
        raise RuntimeError("Expected Metabase 64 MCP query/learning tools are missing")
    print(f"OK: native MCP initialization and {len(tools)} tools discovered")
    status, database = req("POST", "/api/database", {
        "name": "agent-smoke-gizmo", "engine": "arrow-flight-sql",
        "details": {"host": "gizmosql", "port": 31337, "username": "gizmosql",
                    "password": "gizmosql_password", "use-token": False, "useEncryption": False}})
    if status != 200 or not database.get("id"):
        raise RuntimeError("Could not create the agent smoke test connection")
    db_id = database["id"]
    try:
        result = mcp.call("tools/call", {"name": "execute_sql", "arguments": {
            "database_id": db_id, "sql": "SELECT COUNT(*) AS n FROM sales.orders",
            "template_tag_values": None, "prompt": "Flight SQL compatibility regression: count seeded orders",
            "validate_only": False, "row_limit": 10}})
        if result.get("isError"):
            raise RuntimeError("MCP execute_sql returned a tool error")
        structured = result.get("structuredContent")
        if not structured:
            structured = next((json.loads(c["text"]) for c in result.get("content", [])
                               if c.get("type") == "text" and c.get("text", "").lstrip().startswith("{")), {})
        if structured.get("rows") != [[10]]:
            raise RuntimeError(f"MCP count mismatch: {str(structured)[:600]}")
        print("OK: native MCP execute_sql returned 10 seeded orders through Flight SQL")
        listing = run_cli(["database", "list", "--json"])
        if db_id not in {item["id"] for item in listing["data"]}:
            raise RuntimeError("CLI did not list the Flight SQL database")
        (ROOT / ".logs").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="cli-smoke-", dir=ROOT / ".logs") as directory:
            payload = Path(directory) / "query.json"
            payload.write_text(json.dumps({"type": "native", "database": db_id,
                                          "native": {"query": "SELECT COUNT(*) AS n FROM sales.orders"}}), encoding="utf-8")
            result = run_cli(["query", "--file", str(payload), "--json"])
        if result.get("data", {}).get("rows") != [[10]]:
            raise RuntimeError(f"CLI count mismatch: {str(result)[:600]}")
        print("OK: official CLI listed the connection and queried 10 seeded orders")
    finally:
        status, _ = req("DELETE", f"/api/database/{db_id}")
        if status not in (200, 204):
            raise RuntimeError(f"Could not clean up agent smoke database {db_id}")
        mcp.session.close()


if __name__ == "__main__":
    main()
