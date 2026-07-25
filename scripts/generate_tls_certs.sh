#!/usr/bin/env bash
# Generates a local CA, a GizmoSQL server certificate, and an mTLS client
# certificate into ./tls/ for the TLS e2e profile (docker-compose.tls.yaml).
# Requires openssl (bundled with Git Bash on Windows).
set -euo pipefail

# Git Bash on Windows rewrites arguments that look like POSIX paths (e.g.
# -subj "/CN=x" -> "C:/Program Files/Git/CN=x"); disable that conversion.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
mkdir -p tls
cd tls

DAYS=825

echo "==> CA"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out ca-key.pem
openssl req -new -x509 -key ca-key.pem -out ca-cert.pem -days "$DAYS" \
  -subj "/CN=flightsql-driver-test-ca"

echo "==> Server cert (SANs cover the compose hostnames + localhost)"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out server-key.pem
openssl req -new -key server-key.pem -out server.csr \
  -subj "/CN=gizmosql-tls"
cat > server-ext.cnf <<'EOF'
subjectAltName = DNS:gizmosql-tls, DNS:gizmosql-mtls, DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days "$DAYS" -extfile server-ext.cnf

echo "==> Client cert (mTLS; key emitted as PKCS#8, which the Arrow JDBC driver expects)"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out client-key.pem
openssl req -new -key client-key.pem -out client.csr \
  -subj "/CN=flightsql-driver-test-client"
cat > client-ext.cnf <<'EOF'
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem -days "$DAYS" -extfile client-ext.cnf

rm -f server.csr client.csr server-ext.cnf client-ext.cnf ca-cert.srl

# The gizmosql container runs as a non-root user; keys must be readable.
chmod 644 ./*.pem

echo "==> Done:"
ls -la
