# Backend reference: Spice.ai (OSS)

Spice is a DataFusion + Arrow Flight SQL engine that **federates** many sources
behind one endpoint and **accelerates** (caches/materializes) them. It is the
reference backend for [caching / acceleration](../tutorials/caching-acceleration.md),
[real-time analytics](../tutorials/realtime-analytics.md), and
[federation / semantic layer](../tutorials/federation-semantic-layer.md).

## Deploy

Spice is part of the base stack — two services:

- `spiced` (port **50051**) — API-key auth, configured by `spice/spicepod.yaml`.
- `spiced-anon` (port **50052**) — same image, no auth (anonymous test target).

Datasets are declared in the spicepod, not created ad-hoc. The bundled pod
exposes `yellow_taxis` plus schema-qualified `transport.trips` and
`finance.taxi_fares`.

## Connection

| Field | Value |
|---|---|
| Host | `spiced-container` |
| Port | `50051` |
| Username | *(leave blank)* |
| Password | `1234567890` (the API key from `spicepod.yaml` → `runtime.auth.api-key`) |
| Use a secure connection (TLS) | off |

For the anonymous service: host `spiced-anon`, port `50051` (host `50052`),
leave every credential field blank.

## Auth modes

| Mode | Supported | Notes |
|---|---|---|
| API key (blank user + password) | ✅ | Spice.ai convention; the key rides as the password |
| Anonymous | ✅ | via `spiced-anon` (no `runtime.auth`) |
| Username / password | ➖ | Spice OSS uses the API-key convention, not basic user/pass |

## Feature support

| Feature | Status |
|---|---|
| Read (federated datasets) | ✅ |
| Acceleration / caching | ✅ (in-memory, DuckDB, or SQLite `acceleration:`) |
| Iceberg catalog (read/write) | ✅ (`catalogs: - from: iceberg:…`) |
| Change-data-capture (CDC) | ✅ (via source connectors) |
| Uploads / Metabase transforms | ❌ (datasets are pod-declared; read-only from Metabase) |

## Gotchas

- **Prepared-statement params return 0 rows** on DataFusion — the driver inlines
  the catalog filter in `describe-database` to work around this. No action needed.
- Datasets/catalogs are defined in `spicepod.yaml`; to add data, edit the pod and
  restart `spiced`, don't `CREATE TABLE` from Metabase.
- `runtime.caching.sql_results` (renamed from `results_cache` in 2.x) controls
  Spice's own result cache — complementary to Metabase's query cache.
