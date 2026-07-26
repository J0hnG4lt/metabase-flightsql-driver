# Driver e2e suite

API-level end-to-end tests against the compose stack. Stdlib + pytest only.

```bash
# 1. Base stack + setup (writes METABASE_API_KEY to .env)
podman-compose up -d          # (pip podman-compose on Windows; see main README)
python scripts/metabase_setup.py

# 2. Optional: TLS/mTLS profile
./scripts/generate_tls_certs.sh
podman-compose -f docker-compose.yaml -f docker-compose.tls.yaml up -d

# 3. Optional: seed InfluxDB 3 (token -> .env)
python scripts/setup_influxdb3.py

# 4. Run
python -m pytest tests/e2e -v
```

| Module | Covers |
|---|---|
| `test_core.py` | connections, dashboard shape, native queries, UI-vs-legacy token storage shapes, dashboard-parameter filtered query |
| `test_config_matrix.py` | catalog scoping, schema-filters, API-key-as-password, connect timeout, additional-options, anonymous-vs-api-key negative, toggle precedence |
| `test_auth_manifest.py` | manifest field expansion (types, visible-if, advanced gating), normalize-db-details backfill |
| `test_features.py` | every dashboard card (11 visual types), sync_schema/rescan_values, MBQL temporal breakout + relative filters, segments, v2 metrics, pivot |
| `test_tls.py` | TLS skip-verify, CA validation via tlsRootCerts, mTLS client certs, strict-mode negatives (auto-skips without the TLS profile) |
| `test_new_backends.py` | anonymous auth (spiced-anon), InfluxDB 3 bearer + `database` header + sync + negative (auto-skips when not running) |
| `test_operations.py` | query caching (cached flag on repeat run), x-ray dashboard generation, rows-alert email delivery via maildev (auto-skips without maildev) |
| `test_uploads.py` | CSV upload → typed table + model → query → append-csv round trip on GizmoSQL; per-connection feature gating; clean 422 on read-only backends |
| `test_oauth.py` | Keycloak-minted JWTs: admin role read/write, readonly role SELECT-only, tampered-token rejection, and a pinned test documenting that GizmoSQL Core rejects header-borne external bearers from the JDBC `oauth.*` flow (auto-skips without the OAuth profile) |

There is also a Clojure test layer under the repo-root `test/` directory: pure unit tests for the connection spec builder plus Metabase's shared driver harness (test extensions) — run by the `driver-test-suite` CI job, or locally from a Metabase checkout with `DRIVERS=arrow-flight-sql clojure -X:dev:drivers:drivers-dev:test :only '[metabase.driver.arrow-flight-sql-test]'`.

Known gap (needs a server we don't run locally): OAuth2 `oauth.*` flows — Dremio-class; see the backend matrix in the main README.
