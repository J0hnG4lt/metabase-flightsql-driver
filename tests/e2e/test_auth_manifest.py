"""Manifest expansion shape + normalize-db-details backfill."""


def test_manifest_fields_shape(mb):
    _, props = mb.get("/api/session/properties")
    fields = props["engines"]["arrow-flight-sql"]["details-fields"]
    by_name = {}
    for f in fields:
        by_name.setdefault(f["name"], f)

    assert by_name["use-token"]["type"] == "boolean"
    assert "auth-method" not in by_name, "select types are not linter-authorable"
    assert by_name["connect-timeout-millis"].get("type", "string") == "string"
    assert by_name["username"]["visible-if"] == {"use-token": False}
    assert by_name["token-value"]["visible-if"]["use-token"] is True
    assert by_name["tls-root-certs-options"]["visible-if"]["advanced-options"] is True


def test_normalize_backfills_use_token(mb):
    dbs = mb.databases()
    assert dbs["flight"]["details"].get("use-token") is True
    assert dbs["gizmo"]["details"].get("use-token") is False
