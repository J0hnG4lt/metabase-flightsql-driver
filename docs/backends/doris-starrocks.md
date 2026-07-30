# Backend reference: Apache Doris / StarRocks

Doris and StarRocks are **MySQL-dialect** real-time OLAP engines that expose
Arrow Flight SQL. They are the only MySQL-flavored, `SET ROLE`-capable, fully
writable backends in the matrix — the target for the write-heavy parts of
[real-time analytics](../tutorials/realtime-analytics.md) and
[write-back / actions](../tutorials/writeback-actions.md).

> **Read the topology limitation below before investing** — in the common
> single-container setup these backends are only reachable when Metabase is
> colocated with them.

## Deploy

```bash
# host prerequisites for the C++ backend (BE):
podman machine ssh "sudo sysctl -w vm.max_map_count=2000000 && sudo swapoff -a"

podman-compose -f docker-compose.yaml -f docker-compose.doris.yaml up -d doris
python scripts/setup_doris.py        # waits for a live BE, loads sample schemas
# (StarRocks: docker-compose.starrocks.yaml + scripts/setup_starrocks.py, port 9408)
```

Both seed three databases (which appear as Metabase schemas): `sales`, `hr`,
`analytics`.

## Connection

| Field | Value |
|---|---|
| Host | `doris` (or `starrocks`) |
| Port | `8070` (Doris) / `9408` (StarRocks) |
| Username | `root` |
| Password | *(empty)* |
| Additional options | `useServerPrepStmts=false` |

`useServerPrepStmts=false` is **required** — Arrow Flight SQL + Doris/StarRocks
do not support prepared-statement parameter passing.

## Auth modes

| Mode | Supported | Notes |
|---|---|---|
| Username + empty password | ✅ | `root` with no password; the driver sends `user=root&password=` |
| Username + password | ✅ | for non-root RBAC users |

## Feature support

| Feature | Status |
|---|---|
| MySQL SQL dialect | ✅ (the only MySQL-dialect backend) |
| Write (DDL / DML / CTAS) | ✅ |
| `SET ROLE` RBAC | ✅ |
| Multi-database (schemas) | ✅ |

## Gotchas

- **Topology limitation (important).** The all-in-one images advertise their
  Flight SQL data endpoint as `127.0.0.1`, which a **separate** Metabase
  container cannot reach — confirmed on both Linux and podman-on-Windows. They
  work only when Metabase shares the backend's network namespace (colocated) or
  in a multi-node deployment. The e2e suite treats them as opt-in/experimental.
- The Doris/StarRocks **BE won't start under podman-on-Windows** (the heartbeat
  service never reaches the FE). Use native Linux for these.
- Doris DDL needs a key + distribution clause, e.g.
  `... DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num"="1")`.
