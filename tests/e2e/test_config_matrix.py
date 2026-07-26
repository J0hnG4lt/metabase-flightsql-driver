"""Every connector option exercised with live connections."""
from conftest import GIZMO_DETAILS, SPICE_HOST


def test_catalog_scopes_sync(mb, db_factory):
    db_id = db_factory("t-catalog", {**GIZMO_DETAILS, "catalog": "warehouse"})
    tables = mb.wait_synced_tables(db_id)
    schemas = {t["schema"] for t in tables}
    assert tables and schemas <= {"archive", "reports"}, schemas


def test_schema_filters_inclusion(mb, db_factory):
    db_id = db_factory("t-schema-filter", {**GIZMO_DETAILS,
                                           "schema-filters-type": "inclusion",
                                           "schema-filters-patterns": "sales"})
    tables = mb.wait_synced_tables(db_id)
    assert tables and {t["schema"] for t in tables} == {"sales"}


def test_spice_api_key_as_blank_user_password(mb, db_factory):
    db_id = db_factory("t-apikey-pw", {**SPICE_HOST, "use-token": False,
                                       "password": "1234567890"})
    assert mb.native(db_id, "SELECT 1").get("status") == "completed"


def test_connect_timeout_and_additional_options(mb, db_factory):
    db_id = db_factory("t-timeout-opts", {**GIZMO_DETAILS,
                                          "connect-timeout-millis": 5000,
                                          "additional-options": "threadPoolSize=2&retainAuth=true"})
    assert mb.native(db_id, "SELECT COUNT(*) FROM sales.orders").get("status") == "completed"


def test_anonymous_rejected_by_api_key_server(mb, db_factory):
    db_id = db_factory("t-anon-fail", {**SPICE_HOST, "use-token": False},
                       expect_ok=False)
    assert db_id is None, "anonymous connection unexpectedly accepted"


def test_use_token_off_ignores_stale_token(mb, db_factory):
    db_factory("t-toggle-precedence", {**GIZMO_DETAILS,
                                       "token-value": "stale-should-be-ignored"})
