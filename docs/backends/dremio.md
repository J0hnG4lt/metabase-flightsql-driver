# Backend reference: Dremio (OSS)

Dremio is a native lakehouse query engine that speaks Arrow Flight SQL and
stores tables as **Apache Iceberg** by default. It is the reference backend for
the [Iceberg lakehouse](../tutorials/iceberg-lakehouse.md) and
[federation / semantic layer](../tutorials/federation-semantic-layer.md)
tutorials.

## Deploy

```bash
podman-compose -f docker-compose.yaml -f docker-compose.dremio.yaml up -d dremio
python scripts/setup_dremio.py     # admin bootstrap + NAS source + 3 Iceberg tables
```

`setup_dremio.py` creates an admin (`dremio` / `dremio123`), a writable NAS
source `wh` → a **named volume** (see gotchas), and three CTAS Iceberg tables:
`wh.sales.orders`, `wh.hr.employees`, `wh.analytics.events`.

Dremio wants ~4 GB RAM and ~1–2 min to first boot. Web UI/REST: http://localhost:9047.

## Connection

| Field | Value |
|---|---|
| Host | `dremio` |
| Port | `32010` |
| Username / Password | `dremio` / `dremio123` |
| Use a secure connection (TLS) | off |

## Auth modes

| Mode | Supported | Notes |
|---|---|---|
| Username / password | ✅ | the only working mode on Dremio **OSS** |
| Bearer / Personal Access Token | ⚠️ Enterprise only | the PAT REST endpoint 404s on OSS; the `use-token` branch is correct but untestable here |
| Anonymous | ❌ | Dremio requires credentials (rejected) |

## Feature support

| Feature | Status |
|---|---|
| Read Iceberg | ✅ (native) |
| Write Iceberg (CTAS / `INSERT`) | ✅ |
| Multi-schema sync | ✅ (`wh.sales`, `wh.hr`, `wh.analytics`) |
| Federation across sources | ✅ (NAS, S3, RDBMS, … as Dremio sources) |
| Semantic layer | ✅ (views + reflections) |
| CSV uploads / Metabase transforms | ✅ (writable) |

## Gotchas

- **Iceberg warehouse must be a named volume, not a host bind mount.** Dremio's
  Hadoop writer calls `setPermission()`, which fails on a Windows `drvfs` bind
  mount (`RawLocalFileSystem.setPermission`). The compose profile uses the
  `dremio-warehouse` named volume; works on every host.
- **Reserved word in sync.** `TABLES` is reserved in Dremio's parser, so the
  driver quotes it (`information_schema."tables"`); without that, sync finds
  zero tables even though queries work. Handled in the driver — no action needed.
- Dremio catalogs CTAS tables as catalog `DREMIO`, schema `wh.sales` (dotted).
- **Topology:** single-node Dremio is one JVM that serves Flight results
  *inline* (no advertised `127.0.0.1` endpoint), so it is reachable from a
  separate Metabase container on any host — unlike Doris/StarRocks.
