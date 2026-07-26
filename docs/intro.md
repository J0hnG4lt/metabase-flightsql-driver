# Introduction to metabase-flightsql-driver

One Metabase driver, every [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server. The bundled compose stack demonstrates it against four backends with different auth models and capabilities.

## Quick start

```bash
podman-compose up -d              # pip install podman-compose on Windows (see README troubleshooting)
python scripts/metabase_setup.py  # admin user, API key -> .env, connections, demo dashboards
```

Metabase: http://localhost:3000 (`admin@metabase.local` / `Metabase123!`).

> **Metabase 63+ note:** the official image runs JDK 25, which needs extra JVM flags for Arrow — the compose file sets them. See *"Java / JVM requirements"* in the main README before deploying anywhere else.

## The backends

| Connection | Server | Auth used | Capabilities exercised |
|---|---|---|---|
| `gizmo` | GizmoSQL (DuckDB) | username/password | 3 catalogs (`memory`/`warehouse`/`staging`), multi-schema, **writes: CSV uploads + Data Studio transforms**, JWT roles via the OAuth profile |
| `flight` | Spice.ai OSS | API key (as password) | federation/acceleration; `spice` catalog with `public`/`transport`/`finance` schemas |
| `influxdb3` | InfluxDB 3 Core | bearer token + `database=<db>` gRPC header | time-series, read-only, `system` schema filtered via schema-filters |
| `quack-on-demand` | Starlake quack-on-demand | username/password over TLS + `tenant`/`pool` params | multi-tenant DuckLake serving |

## Connection recipes

**Authentication** is a toggle: *off* = username/password (leave username empty for Spice API keys; use the literal username `token` for GizmoSQL external JWTs), *on* = bearer token (InfluxDB 3, Dremio PATs). Everything credential-like is stored as a Metabase secret.

- **Catalog** scopes sync to one catalog (e.g. `warehouse` on gizmo, `spice` on Spice).
- **Schemas** (include/exclude patterns) filter what syncs — e.g. exclude `system` on InfluxDB 3.
- **Advanced**: CA certificate / mTLS client cert+key (PEM secrets), connect timeout, *Writable backend* toggle (enables CSV uploads and Data Studio table transforms — only for servers accepting DDL/DML), and **Additional options** — the escape hatch for anything the Arrow JDBC driver understands (`threadPoolSize`, `retainAuth`, `oauth.*`) plus unknown params which are forwarded as gRPC headers (`database=demo` for InfluxDB 3, `tenant=acme&pool=bi` for quack-on-demand).

## Optional profiles

```bash
# TLS/mTLS (gizmosql-tls :31338 CA-signed, gizmosql-mtls :31339 requires client certs)
./scripts/generate_tls_certs.sh
podman-compose -f docker-compose.yaml -f docker-compose.tls.yaml up -d

# OAuth2 (Keycloak realm minting role-scoped JWTs; gizmosql-oauth :31340 verifies them)
python scripts/generate_oauth_config.py
podman-compose -f docker-compose.yaml -f docker-compose.oauth.yaml up -d keycloak gizmosql-oauth

# InfluxDB 3 seed (admin token -> .env, demo database with weather/cpu/sensors)
python scripts/setup_influxdb3.py
```

## Testing

```bash
python -m pytest tests/e2e -v     # ~40 API-level tests; optional stacks auto-skip
```

CI additionally runs Metabase's own shared driver test harness (Clojure test extensions under `test/`) against a GizmoSQL service. See `tests/e2e/README.md`.

## Walkthrough screenshots

> Taken on an earlier Metabase/driver version — the connection form now shows the auth toggle and TLS fields, but the flow is unchanged.

Add the connection, then sync and explore:

![connection](/docs/connection.png)
![database-sync](/docs/database-sync.png)
![sql-editor](/docs/sql-editor.png)
![browse-data](/docs/browse-data.png)
![visual-editor](/docs/visual-editor.png)
