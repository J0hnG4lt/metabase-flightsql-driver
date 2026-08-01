# Backend reference: kamu (Open Data Fabric)

[kamu](https://kamu.dev) is a verifiable, **bitemporal** data lakehouse and the
reference implementation of the [Open Data Fabric](https://opendatafabric.org)
(ODF) protocol. It speaks **Apache Arrow Flight SQL** directly (port **50050**)
via an embedded **Apache DataFusion** engine — so this driver reads it with zero
driver changes, the same way it reads Spice.

What makes kamu unlike every other backend in the matrix: **each dataset is an
append-only changelog, not a mutable table.** Every row carries ODF *system
columns* alongside your data, which is what makes time-travel and audit-trail
analytics possible from plain Metabase SQL. See the
[verifiable / time-travel BI tutorial](../tutorials/verifiable-timetravel-bi.md).

## Deploy

```bash
podman-compose -f docker-compose.yaml -f docker-compose.kamu.yaml up -d kamu
```

The container self-seeds a Snapshot dataset `account.balances` (two batches — the
second corrects one balance `50→75`, adds an account, and drops another, so the
changelog contains all four `op` codes) and then serves Flight SQL on `:50050`.
Seeding runs **before** the port binds, so an open port means the data is ready.
The workspace is ephemeral — it re-seeds on each start (fine for a demo backend).

## Connection

| Field | Value |
|---|---|
| Host | `kamu` |
| Port | `50050` |
| Username / Password | `anonymous` / `anonymous` |
| Use Encryption | **off** (plaintext gRPC) |
| Connect timeout (ms) | `30000` (advanced — kamu's cold-start handshake can exceed the 10s default) |

## Auth modes

| Mode | How | Notes |
|---|---|---|
| Anonymous / basic | Username `anonymous`, Password `anonymous` | The default for a local single-tenant kamu server |

> kamu **nodes** (the server/platform product) add access tokens, an OAuth2
> device flow, wallet/cryptographic auth, and ReBAC authorization for
> private/shared datasets. The local `kamu sql server` used here is single-tenant
> and accepts the anonymous basic handshake.

## The ODF system columns (why kamu is different)

Every dataset exposes these alongside your fields:

| Column | Meaning |
|---|---|
| `offset` | Monotonic row position in the changelog (0, 1, 2, …) |
| `op` | Change type: `0` append (+A), `1` retract (−R), `2` correct-from (−C), `3` correct-to (+C) |
| `system_time` | **Transaction time** — when the system observed/processed the record |
| `event_time` | **Valid time** — when the event actually happened |

- **Current state:** collapse the changelog with the `to_table()` UDTF —
  `SELECT * FROM to_table("account.balances")`. The argument is a table
  *identifier* (double-quote dotted names), not a string literal.
- **Time-travel (as of a transaction time):** the log is append-only, so
  reconstruct any past state by taking, per primary key, the latest row (by
  `offset`) with `system_time <= <cut>`, keeping value-setting ops `0`/`3`:
  ```sql
  SELECT account_id, balance FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY offset DESC) rn
    FROM "account.balances"
    WHERE system_time <= (SELECT MIN(system_time) FROM "account.balances")  -- "as originally booked"
  ) WHERE rn = 1 AND op IN (0, 3);
  ```

## Feature support

| Feature | Status |
|---|---|
| Read / query (DataFusion SQL) | ✅ |
| Bitemporal + changelog columns | ✅ (the headline) |
| `to_table()` current-state collapse | ✅ |
| Multi-dataset sync (as tables) | ✅ (datasets are flat tables named by their dotted dataset name, in schema `kamu`) |
| Write (CTAS / DDL / DML) | ❌ — data enters via kamu ingest/transforms, not SQL. So **CSV uploads, transforms, and Actions do not apply** |
| Verifiable queries (signed commitments) | ✅ via kamu's REST query API (out of band of Flight SQL) |

## Gotchas

- **Use a single, stable Metabase connection.** kamu's Flight SQL server is young
  and can wedge under rapid *connection churn* (many new connections created in a
  burst). One steady connection — normal BI usage — is fine; the e2e suite shares
  one connection for exactly this reason.
- **Read-only.** kamu is not a write target over SQL; don't enable Writable
  backend / Actions for it.
- **Dotted table names need double quotes:** `"account.balances"`.
- **Time-travel is column-based**, not `FOR SYSTEM_TIME AS OF` — filter
  `system_time` / `event_time` (see above). Prefer `system_time` for correct
  reconstruction of Snapshot changelogs (a Snapshot correction moves the old
  value's `event_time` onto the `correct-from` row).
- Ephemeral workspace: restarting the container re-seeds from scratch.
