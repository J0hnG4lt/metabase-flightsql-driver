"""Generate the OAuth test profile config into ./oauth/:

- signing-key.pem / signing-cert.pem : deterministic RS256 keypair. The
  private key is embedded into the Keycloak realm import (so the realm signs
  tokens with a key we know BEFORE Keycloak ever boots), and the certificate
  is handed to GizmoSQL for JWT signature verification
  (TOKEN_SIGNATURE_VERIFY_CERT_PATH) — no startup-order coupling.
- realm.json : Keycloak realm `flightsql` with two client-credentials
  clients: flightsql-m2m (role=admin claim) and flightsql-readonly
  (role=readonly claim), both with an audience mapper adding `gizmosql`.

Usage: python scripts/generate_oauth_config.py   (requires openssl on PATH)
"""
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OAUTH = ROOT / "oauth"
OAUTH.mkdir(exist_ok=True)

KEY = OAUTH / "signing-key.pem"
CERT = OAUTH / "signing-cert.pem"

env = {**os.environ, "MSYS_NO_PATHCONV": "1"}


def run(*args):
    subprocess.run(args, check=True, env=env, capture_output=True, text=True)


if not KEY.exists():
    run("openssl", "genpkey", "-algorithm", "RSA",
        "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(KEY))
    run("openssl", "req", "-new", "-x509", "-key", str(KEY), "-out", str(CERT),
        "-days", "825", "-subj", "/CN=flightsql-token-signing")
    os.chmod(KEY, 0o644)
    os.chmod(CERT, 0o644)
    print("generated signing keypair")
else:
    print("signing keypair already present")

private_key_pem = KEY.read_text()
certificate_pem = CERT.read_text()


def client(client_id, secret, role):
    return {
        "clientId": client_id,
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "serviceAccountsEnabled": True,
        "standardFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "secret": secret,
        "protocolMappers": [
            {
                "name": "gizmosql-audience",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.custom.audience": "gizmosql",
                    "access.token.claim": "true",
                },
            },
            {
                "name": "gizmosql-role",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-hardcoded-claim-mapper",
                "config": {
                    "claim.name": "role",
                    "claim.value": role,
                    "jsonType.label": "String",
                    "access.token.claim": "true",
                },
            },
        ],
    }


realm = {
    "realm": "flightsql",
    "enabled": True,
    "accessTokenLifespan": 3600,
    "clients": [
        client("flightsql-m2m", "m2m-demo-secret-0123456789", "admin"),
        client("flightsql-readonly", "readonly-demo-secret-0123456789", "readonly"),
    ],
    "components": {
        "org.keycloak.keys.KeyProvider": [
            {
                "name": "imported-rsa",
                "providerId": "rsa",
                "subComponents": {},
                "config": {
                    "privateKey": [private_key_pem],
                    "certificate": [certificate_pem],
                    "active": ["true"],
                    "enabled": ["true"],
                    "priority": ["200"],
                    "algorithm": ["RS256"],
                },
            }
        ]
    },
}

(OAUTH / "realm.json").write_text(json.dumps(realm, indent=2))
print(f"wrote {OAUTH / 'realm.json'}")
print("clients: flightsql-m2m (role=admin), flightsql-readonly (role=readonly); audience=gizmosql")
