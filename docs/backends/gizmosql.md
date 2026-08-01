# Backend reference: GizmoSQL (DuckDB)

GizmoSQL is DuckDB exposed over Arrow Flight SQL. It is the most feature-complete
backend in the matrix — **writable**, so it drives the
[CSV uploads](../tutorials/csv-uploads.md),
[transformations-in-metabase](../tutorials/transformations-in-metabase.md), and
[local/embedded analytics](../tutorials/local-embedded-analytics.md) tutorials —
and it supports the widest range of auth modes.

## Deploy

Part of the base stack (`gizmosql`, port **31337**). `gizmosql/init.sql` seeds
three schemas — `sales`, `hr`, `analytics`.

## Connection

| Field | Value |
|---|---|
| Host | `gizmosql` |
| Port | `31337` |
| Username / Password | `gizmosql` / `gizmosql_password` |
| Writable backend (enable CSV uploads) | on (for uploads/transforms) |

## Auth modes

| Mode | How | Notes |
|---|---|---|
| Username / password | Username `gizmosql`, Password `gizmosql_password` | Flight handshake basic auth |
| External JWT | Username literally `token`, JWT in Password | GizmoSQL's bring-your-own-JWT convention |
| mTLS | TLS profile (`gizmosql-tls`) + client cert/key PEM secrets | see [auth-cookbook](../tutorials/auth-cookbook.md) |
| OAuth 2.0 (client credentials) | `oauth.*` in Additional options | the JDBC flow fetches + sends the token via handshake |

> GizmoSQL **Core** accepts external bearers only via the handshake convention
> above; a raw `Authorization: Bearer` header (the `use-token` toggle) is an
> Enterprise/JWKS capability. Use username `token` + JWT instead.

## Feature support

| Feature | Status |
|---|---|
| CSV uploads | ✅ (creates typed DuckDB table + model) |
| Metabase transforms (`:transforms/table`) | ✅ (CTAS) |
| Write (CTAS / DDL / DML) | ✅ |
| Actions (write-back buttons/forms) | ✅ (Enable-Actions toggle → [CRUD tutorial](../tutorials/crud-app-gizmosql.md)) |
| Read Iceberg / Delta / DuckLake / Parquet | ✅ (DuckDB extensions) |
| Multi-catalog | ✅ (3 test catalogs) |

## Gotchas

- Set a stable `SECRET_KEY` (the compose file does): it signs the session JWTs
  GizmoSQL issues after basic auth. Without a stable value, a container restart
  invalidates the bearer tokens Metabase's pooled connections retained
  (`retainAuth`) → auth errors until the pool cycles.
- `TLS_ENABLED=false` on the plain service; the TLS/mTLS variant lives in the
  TLS profile.
