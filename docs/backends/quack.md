# Backend reference: quack-on-demand (DuckLake)

quack-on-demand (starlake-ai) is a **multi-tenant** DuckLake serving layer that
speaks Arrow Flight SQL exclusively, over TLS. It is the reference backend for
the multi-tenant part of [local/embedded analytics](../tutorials/local-embedded-analytics.md).

## Deploy

Part of the base stack (`quack-on-demand`). `demo` mode self-provisions tenant
`acme` with admin/admin and a TPC-H DuckLake, on a self-signed cert.

```bash
python scripts/provision_quack_dashboard.py   # optional: seeds a sample dashboard
```

Ports: in-network `31338` (Flight SQL), host `31341`; admin REST on `20900`.

## Connection

| Field | Value |
|---|---|
| Host | `quack-on-demand` |
| Port | `31338` |
| Username / Password | `admin` / `admin` |
| Use a secure connection (TLS) | on |
| Skip certificate verification | on (self-signed demo cert) |
| Additional options | `tenant=acme` (and `pool=<name>` if used) |

## Auth modes

| Mode | Supported | Notes |
|---|---|---|
| Username / password + tenant | ✅ | `tenant=`/`pool=` ride Additional options as gRPC headers |
| TLS (self-signed) | ✅ | enable TLS + skip verification for the demo cert |

## Feature support

| Feature | Status |
|---|---|
| Read (DuckLake / TPC-H) | ✅ |
| Multi-tenant isolation | ✅ (per-tenant catalogs) |
| Write from Metabase | ➖ (serving layer; treat as read) |

## Gotchas

- It is **TLS-only** — you must enable the secure-connection toggle and, for the
  demo cert, skip certificate verification.
- `tenant=` is required to select the tenant; `pool=` picks a warehouse pool.
