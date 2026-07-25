"""TLS/mTLS via the docker-compose.tls.yaml profile. Cert paths are
container-local to Metabase (/opt/flightsql-tls)."""
from conftest import requires_tls_stack

pytestmark = requires_tls_stack

TLS_DIR = "/opt/flightsql-tls"
BASE = {"port": 31337, "username": "gizmosql", "password": "gizmosql_password",
        "use-token": False, "useEncryption": True}


def q(mb, db_id):
    return mb.native(db_id, "SELECT COUNT(*) FROM sales.orders").get("status") == "completed"


def test_tls_skip_verification(mb, db_factory):
    db_id = db_factory("t-tls-skip", {**BASE, "host": "gizmosql-tls",
                                      "disableCertificateVerification": True})
    assert q(mb, db_id)


def test_tls_ca_validated(mb, db_factory):
    db_id = db_factory("t-tls-ca", {**BASE, "host": "gizmosql-tls",
                                    "disableCertificateVerification": False,
                                    "tls-root-certs-path": f"{TLS_DIR}/ca-cert.pem"})
    assert q(mb, db_id)


def test_strict_verification_without_ca_rejected(mb, db_factory):
    db_id = db_factory("t-tls-no-ca", {**BASE, "host": "gizmosql-tls",
                                       "disableCertificateVerification": False},
                       expect_ok=False)
    assert db_id is None


def test_mtls_with_client_certificate(mb, db_factory):
    db_id = db_factory("t-mtls", {**BASE, "host": "gizmosql-mtls",
                                  "disableCertificateVerification": False,
                                  "tls-root-certs-path": f"{TLS_DIR}/ca-cert.pem",
                                  "client-cert-path": f"{TLS_DIR}/client-cert.pem",
                                  "client-key-path": f"{TLS_DIR}/client-key.pem"})
    assert q(mb, db_id)


def test_mtls_without_client_certificate_rejected(mb, db_factory):
    db_id = db_factory("t-mtls-no-cert", {**BASE, "host": "gizmosql-mtls",
                                          "disableCertificateVerification": True},
                       expect_ok=False)
    assert db_id is None
