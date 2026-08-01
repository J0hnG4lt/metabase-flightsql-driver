# Tutorials & use cases

The Arrow Flight SQL driver turns **any** Flight-SQL-speaking engine into a
Metabase data source. These tutorials are organized by **what you want to
build**, and for each one they name the backend that showcases it best.

Every tutorial is grounded in the compose profiles and connection settings in
this repo, so the steps are runnable, not hypothetical.

## Before you start

1. Deploy the stack (see the [main README](../../README.md#quick-start)):
   ```bash
   podman-compose up -d
   python scripts/metabase_setup.py     # admin + sample connections/dashboards
   ```
   Metabase → http://localhost:3000 (admin `admin@metabase.local` / `Metabase123!`).
2. **Metabase 63 runs on JDK 25.** The compose file already sets the required
   `JAVA_OPTS` (`--add-opens` + `--sun-misc-unsafe-memory-access=allow`); without
   them Arrow's allocator fails to initialize. See the
   [main README](../../README.md#java--jvm-requirements-important-for-metabase-63).
3. Optional backends live behind their own overlays (Dremio, Doris, TLS, OAuth,
   …) — each tutorial says which to bring up.

## Use-case × backend matrix

| Use case | Tutorial | Dremio | Spice.ai | GizmoSQL | InfluxDB 3 | quack | Doris/SR |
|---|---|:--:|:--:|:--:|:--:|:--:|:--:|
| Iceberg lakehouse (read/write) | ✅ [iceberg-lakehouse](iceberg-lakehouse.md) | ★ | ✓ | ✓ | | | |
| Transformations inside Metabase | ✅ [transformations-in-metabase](transformations-in-metabase.md) | ✓ | | ★ | | | ✓ |
| Caching / query acceleration | ✅ [caching-acceleration](caching-acceleration.md) | | ★ | | | | |
| Real-time / CDC / live OLAP | realtime-analytics *(planned)* | | ★ | | ✓ | | ✓ |
| CSV uploads → typed model | ✅ [csv-uploads](csv-uploads.md) | ✓ | | ★ | | | ✓ |
| Federation / semantic layer | ✅ [federation-semantic-layer](federation-semantic-layer.md) | ★ | ★ | | | | |
| Time-series analytics | timeseries *(planned)* | | ✓ | | ★ | | |
| Local / embedded (Parquet, no warehouse) | ✅ [local-embedded-analytics](local-embedded-analytics.md) | | | ★ | | ✓ | |
| Write-back / actions | writeback-actions *(planned)* | ✓ | | ✓ | | | ✓ |
| Every authentication mode | auth-cookbook *(planned)* | user/pass | API-key, anon | user/pass, JWT, mTLS, OAuth | bearer | tenant/pool | user/pass |

★ = the backend the tutorial is built around · ✓ = also works · ✅ = tutorial complete

> The tutorials marked ✅ are complete (with screenshots). The rest are being
> written — each is generated end-to-end from a live PoC via the
> [`tutorial-generator`](../../.claude/skills/tutorial-generator/SKILL.md) skill.

## Connection quick-reference

All backends share one connection form (**Databases → Add → Arrow Flight SQL**).
Host is the compose service name; the driver connects over the metanet1 network.

| Backend | Host | Port | Auth in the form | Reference |
|---|---|---|---|---|
| Dremio | `dremio` | 32010 | Username `dremio` / Password `dremio123` | [backends/dremio](../backends/dremio.md) |
| Spice.ai | `spiced-container` | 50051 | Username *blank*, Password = API key | [backends/spice](../backends/spice.md) |
| GizmoSQL | `gizmosql` | 31337 | Username `gizmosql` / Password `gizmosql_password` | [backends/gizmosql](../backends/gizmosql.md) |
| InfluxDB 3 | `influxdb3` | 8181 | Token toggle on, paste admin token | [backends/influxdb3](../backends/influxdb3.md) |
| quack-on-demand | `quack-on-demand` | 31338 | Username `admin` / Password `admin` | [backends/quack](../backends/quack.md) |
| Doris / StarRocks | `doris` / `starrocks` | 8070 / 9408 | Username `root`, empty password | [backends/doris-starrocks](../backends/doris-starrocks.md) |

## How every tutorial is structured

**Problem** → **When to use this** → **Backend setup** (compose + connection) →
**Steps in Metabase** → **Verify** → **Variations & gotchas**.

## Backend reference pages

Setup, all supported auth modes, feature support, and known gotchas per engine:
[Dremio](../backends/dremio.md) · [Spice.ai](../backends/spice.md) ·
[GizmoSQL](../backends/gizmosql.md) · [InfluxDB 3](../backends/influxdb3.md) ·
[quack-on-demand](../backends/quack.md) · [Doris/StarRocks](../backends/doris-starrocks.md)
