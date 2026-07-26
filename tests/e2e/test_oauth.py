"""OAuth2 profile (docker-compose.oauth.yaml): Keycloak-minted JWTs against
GizmoSQL with signature/issuer/audience verification and role enforcement.

Key finding encoded below: GizmoSQL Core accepts EXTERNAL JWTs only via the
Flight handshake (username literally "token", JWT as password). The JDBC
``oauth.*`` client_credentials flow does fetch and send the Keycloak token as
a bearer header (verified in server logs), but Core's bearer-header path only
accepts its own session tokens — header-borne external JWTs are an
Enterprise (JWKS) capability. Use Username="token" + the OAuth token as
Password with this server.
"""
import json
import urllib.parse
import urllib.request

import pytest

from conftest import REPO_ROOT, port_open

requires_oauth_stack = pytest.mark.skipif(
    not (REPO_ROOT / "oauth" / "signing-cert.pem").exists()
    or not port_open("localhost", 8180)
    or not port_open("localhost", 31340),
    reason="OAuth profile not running (scripts/generate_oauth_config.py + docker-compose.oauth.yaml)")

pytestmark = requires_oauth_stack

KC_TOKEN_URL = "http://localhost:8180/realms/flightsql/protocol/openid-connect/token"
IN_NETWORK_TOKEN_URL = "http://keycloak:8080/realms/flightsql/protocol/openid-connect/token"

GIZMO_OAUTH = {"host": "gizmosql-oauth", "port": 31337, "use-token": False,
               "useEncryption": False, "disableCertificateVerification": True}


def keycloak_token(client_id, secret):
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": secret}).encode()
    with urllib.request.urlopen(KC_TOKEN_URL, data, timeout=30) as r:
        return json.load(r)["access_token"]


def jwt_details(jwt):
    return {**GIZMO_OAUTH, "username": "token", "password": jwt}


def test_keycloak_jwt_admin_connects_and_writes(mb, db_factory):
    jwt = keycloak_token("flightsql-m2m", "m2m-demo-secret-0123456789")
    db_id = db_factory("t-kc-admin", jwt_details(jwt))
    res = mb.native(db_id, "SELECT COUNT(*) FROM sales.orders")
    assert res.get("status") == "completed" and res["data"]["rows"][0][0] > 0
    res = mb.native(db_id, "CREATE TABLE main.oauth_admin_probe(id INT)")
    assert res.get("status") == "completed", res.get("error")
    mb.native(db_id, "DROP TABLE main.oauth_admin_probe")


def test_keycloak_jwt_readonly_role_is_select_only(mb, db_factory):
    jwt = keycloak_token("flightsql-readonly", "readonly-demo-secret-0123456789")
    db_id = db_factory("t-kc-readonly", jwt_details(jwt))
    res = mb.native(db_id, "SELECT COUNT(*) FROM sales.customers")
    assert res.get("status") == "completed" and res["data"]["rows"][0][0] > 0
    res = mb.native(db_id, "CREATE TABLE main.readonly_probe(id INT)")
    assert res.get("status") == "failed", "readonly role unexpectedly allowed DDL"


def test_tampered_jwt_rejected(mb, db_factory):
    jwt = keycloak_token("flightsql-m2m", "m2m-demo-secret-0123456789")
    bad = jwt[:-20] + "A" * 20
    db_id = db_factory("t-kc-tampered", jwt_details(bad), expect_ok=False)
    assert db_id is None


def test_jdbc_oauth_bearer_header_not_accepted_by_core(mb, db_factory):
    """Pins the known GizmoSQL Core limitation (see module docstring): the
    JDBC oauth.* flow sends the token as a bearer HEADER, which Core's
    session-token validator rejects (reason=invalid_issuer in server logs)
    even though issuer/audience/signature config matches. If this test ever
    FAILS (i.e. the connection succeeds), Core gained external-bearer
    support — update the docs and promote this to a positive test."""
    opts = ("oauth.flow=client_credentials"
            f"&oauth.tokenUri={IN_NETWORK_TOKEN_URL}"
            "&oauth.clientId=flightsql-m2m"
            "&oauth.clientSecret=m2m-demo-secret-0123456789")
    db_id = db_factory("t-kc-oauth-header", {**GIZMO_OAUTH, "additional-options": opts},
                       expect_ok=False)
    assert db_id is None
