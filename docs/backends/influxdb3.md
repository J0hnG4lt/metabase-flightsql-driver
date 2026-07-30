# Backend reference: InfluxDB 3 (Core)

InfluxDB 3 Core exposes Arrow Flight SQL on a single port shared with HTTP. It is
the reference backend for the [time-series](../tutorials/timeseries.md) tutorial
and the canonical **bearer-token** auth target.

## Deploy

Part of the base stack (`influxdb3`, port **8181**). Seed it:

```bash
python scripts/setup_influxdb3.py    # creates an admin token, writes it to .env
                                     # as INFLUXDB3_TOKEN, loads sample data into `demo`
```

## Connection

| Field | Value |
|---|---|
| Host | `influxdb3` |
| Port | `8181` |
| Token toggle | **on** |
| Token | the value of `INFLUXDB3_TOKEN` in `.env` |
| Additional options | `database=demo` |
| Use a secure connection (TLS) | off |

The `database=<db>` option is **required** — InfluxDB 3 selects the database from
a gRPC header, and the driver forwards unknown Additional-options as headers.

## Auth modes

| Mode | Supported | Notes |
|---|---|---|
| Bearer token | ✅ | the only mode; the token toggle sends `Authorization: Bearer …` |
| Username / password | ❌ | InfluxDB 3 is token-only |

## Feature support

| Feature | Status |
|---|---|
| Read (time-series over SQL) | ✅ |
| Write from Metabase | ❌ (writes use InfluxDB line protocol over HTTP) |
| Schemas | `iox` (your measurements as tables) + `system` |

## Gotchas

- Add a **schema-filters exclusion for `system`** so InfluxDB's internal tables
  stay out of sync.
- No catalog concept — leave the catalog field blank; scope with `database=` only.
- HTTP and gRPC/Flight share port 8181.
