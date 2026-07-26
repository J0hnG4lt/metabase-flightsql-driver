"""Operational features through the driver: query caching, x-rays
(automagic dashboards), and alert delivery via SMTP (maildev)."""
import json
import time
import urllib.request

import pytest

from conftest import port_open

requires_maildev = pytest.mark.skipif(
    not port_open("localhost", 1080), reason="maildev not running")


@pytest.fixture()
def root_cache_config(mb):
    """Cache-everything root strategy; removed afterwards."""
    status, res = mb.req("PUT", "/api/cache", {
        "model": "root", "model_id": 0,
        "strategy": {"type": "ttl", "min_duration_ms": 0, "multiplier": 100000}})
    assert status == 200, res
    yield
    mb.req("DELETE", "/api/cache", {"model": "root", "model_id": 0})


def test_query_caching_second_run_is_cached(mb, root_cache_config):
    _, dash = mb.get("/api/dashboard/2")
    card_id = next(dc["card_id"] for dc in dash["dashcards"] if dc.get("card_id"))
    _, r1 = mb.post(f"/api/card/{card_id}/query")
    assert r1.get("status") == "completed"
    _, r2 = mb.post(f"/api/card/{card_id}/query")
    assert r2.get("status") == "completed"
    assert r2.get("cached"), "second execution was not served from cache"


def test_xray_table_generates_dashboard(mb):
    dbs = mb.databases()
    _, meta = mb.get(f"/api/database/{dbs['gizmo']['id']}/metadata")
    orders = next(t for t in meta["tables"]
                  if t["name"] == "orders" and t["schema"] == "sales")
    status, xray = mb.get(f"/api/automagic-dashboards/table/{orders['id']}")
    assert status == 200
    cards = [dc for dc in xray.get("dashcards", []) if dc.get("card")]
    assert cards, "x-ray produced no cards"


@requires_maildev
def test_alert_email_delivery(mb):
    """rows-condition alert on a Flight SQL card -> rendered -> delivered."""
    _, dash = mb.get("/api/dashboard/2")
    card_id = next(dc["card_id"] for dc in dash["dashcards"]
                   for pm in dc.get("parameter_mappings") or []
                   if pm.get("parameter_id") == "status")

    # ensure SMTP points at maildev (idempotent)
    status, _ = mb.req("PUT", "/api/email", {
        "email-smtp-host": "maildev", "email-smtp-port": 1025,
        "email-smtp-security": "none",
        "email-from-address": "metabase@flightsql.local"})
    assert status == 200

    def mail_count():
        with urllib.request.urlopen("http://localhost:1080/email", timeout=10) as r:
            return len(json.load(r))

    before = mail_count()
    status, res = mb.post("/api/pulse/test", {
        "name": "e2e-alert", "alert_condition": "rows", "alert_first_only": False,
        "cards": [{"id": card_id, "include_csv": False, "include_xls": False}],
        "channels": [{"channel_type": "email", "enabled": True,
                      "recipients": [{"id": 1, "email": "admin@metabase.local"}],
                      "details": {}, "schedule_type": "daily", "schedule_hour": 8}],
        "skip_if_empty": False})
    assert status == 200, res

    deadline = time.time() + 60
    while time.time() < deadline:
        if mail_count() > before:
            return
        time.sleep(4)
    pytest.fail("alert email was not delivered within 60s")
