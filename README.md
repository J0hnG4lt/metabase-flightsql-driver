# Metabase Arrow Flight SQL Driver

A Clojure library that enables Metabase to connect to databases using the Apache Arrow Flight SQL JDBC driver. This driver integrates Arrow Flight SQL into Metabase, delivering enhanced performance and advanced SQL querying capabilities.

My main goal was to allow Metabase to use [Spice.ai OSS](https://spiceai.org/docs) as a cache layer.

## Features

- JDBC-based integration: Leverages the Apache Arrow Flight SQL JDBC driver.
- Flexible configuration: Supports custom connection properties such as host, port, user, password, token, and encryption.
- Custom Schema Sync: Implements table and column description methods for seamless Metabase integration.
- Timestamp Conversion: Automatically converts TIMESTAMP columns to local date-time objects.
- Field Filters: Full support for Metabase field filters including table aliases for JOIN queries.
- Write-back: CSV uploads, Metabase transforms, and **Actions** (dashboard write-back buttons/forms — basic model create/update/delete + custom SQL) on full-DML backends, each behind an explicit connection toggle.

## Tutorials & use cases

Because the driver is a **universal Flight SQL bridge**, the most useful docs are
task-oriented: *here's a thing you can build, and the backend that makes it
shine.* Hands-on, screenshot-driven guides live in
**[docs/tutorials/](docs/tutorials/README.md)** — Iceberg lakehouses, caching,
real-time analytics, transformations inside Metabase, CSV uploads, federation,
time-series, and more.

**Eleven tutorials, each complete and screenshot-driven** — Iceberg lakehouse,
transformations, CSV uploads, caching/acceleration, local/embedded analytics,
federation/semantic layer, write-back, a [CRUD app with dashboard
buttons/forms](docs/tutorials/crud-app-gizmosql.md) (Metabase **Actions**), auth
cookbook, time-series, and real-time.
See the [use-case × backend matrix](docs/tutorials/README.md) to pick one.

Per-backend setup, auth modes, feature support, and gotchas:
**[docs/backends/](docs/backends/)** — Dremio · Spice.ai · GizmoSQL · InfluxDB 3 ·
quack-on-demand · Doris/StarRocks.

## Compatibility

**One jar per release — `arrow-flight-sql.metabase-driver.jar`** (see the
[release assets](https://github.com/J0hnG4lt/metabase-flightsql-driver/releases)).
Each release is built against the current Metabase line and bundles Arrow Flight
SQL JDBC 19.0.0. Rather than guess the supported range, CI proves it: the
`compat-smoke` job drops the jar into each Metabase version, boots it, and runs a
query — so the "verified on" column below is evidence, not a claim.

| Driver release | Built against | Verified on | Arrow JDBC | JDK note |
|---|---|---|---|---|
| unreleased (main) | Metabase v0.63.1 | 0.63.1 (required) · 0.62.5\* | 19.0.0 | 0.63 runs JDK 25 — see *Java / JVM requirements* |
| 0.1.0 | v0.62.5 + v0.63.1 | — | 19.0.0 | shipped two jars (`-mb62`/`-mb63`); **now consolidated to one** |
| 0.0.9 | v0.62.4 | — | 18.2.0 | |
| 0.0.5 – 0.0.8 | v0.55 – v0.62 | — | 18.2.0 | |

The driver targets **Metabase 0.63+**. \*0.62.5 is a best-effort/informational
smoke check (it EOLs 2026-09-01). On older Metabase, use an earlier driver
release or upgrade Metabase.

> **Why one jar (not per-version)?** The driver is a single source file; the old
> `-mb62`/`-mb63` split just AOT-compiled it against two Metabase checkouts. The
> JDK 21-vs-25 difference is a *runtime* flag concern (see below), not a
> packaging one. So — like the DuckDB and other community drivers — we ship one
> jar and **test** across versions instead of **releasing** across versions.

## Java / JVM requirements (important for Metabase 63+)

The official **Metabase v0.63 Docker image runs JDK 25** (v0.62 ran JDK 21). Two JDK-25 changes break the Arrow Flight SQL JDBC driver's memory allocator at first connection with:

```
Could not initialize class org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
```

1. **JEP 498**: `sun.misc.Unsafe` memory-access methods are disabled by default on JDK 24+ — Arrow's allocator depends on them.
2. The image's default flags no longer open `java.nio` internals to unnamed modules.

**Fix — pass these JVM options to Metabase** (the bundled `docker-compose.yaml` already does):

```yaml
environment:
  JAVA_OPTS: >-
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --sun-misc-unsafe-memory-access=allow
    --enable-native-access=ALL-UNNAMED
    -Dio.netty.tryReflectionSetAccessible=true
```

For bare-JVM installs, add the same flags to the `java ... -jar metabase.jar` command line. On Metabase ≤ 62 (JDK 21) only the two `--add-opens` are needed and the image already includes them; `--sun-misc-unsafe-memory-access` is unknown to JDK ≤ 22 and must be omitted there.

> Symptom guide: the driver *loads* and registers fine at startup — the failure appears only on the first real connection/health-check, and once the class-init fails the JVM caches the failure until restart.

## Upgrading from 0.0.x

Drop-in: replace the plugin jar (`arrow-flight-sql.metabase-driver.jar`) and restart Metabase. Existing connections keep working — the driver auto-detects legacy details (plain username/password or token) and backfills the new auth toggle on read.

After upgrading, open each Flight SQL connection in **Admin → Databases** and hit **Save** once: this persists the backfilled auth flag and re-validates the connection.

Behavior changes to be aware of:

- **New** connections default to *Use Encryption ON* (previously off). Existing connections keep their stored setting.
- Tokens entered through the admin UI now actually work (previously only API-created connections could authenticate with tokens).
- `convertTimezone()` no longer appears in the expression editor — it was advertised but never worked.
- FK metadata is no longer probed during sync (it always returned nothing anyway); Metabase-side semantic/FK settings are unaffected.
- Release assets are now a **single jar** — `arrow-flight-sql.metabase-driver.jar` (0.1.0 shipped separate `-mb62`/`-mb63` jars; these are consolidated). Update any download automation that pinned the per-line names.
- Built against Metabase v0.63.1 and CI-verified to load and query on it; bundles Arrow Flight SQL JDBC 19.0.0.

## Installation

### Prerequisites

- Podman (https://podman.io/) – for container management.
- Python 3 – for running the automated setup script.
- Leiningen (https://leiningen.org/) – optional, for local builds (the docker-compose handles this automatically).

### Quick Start

1. Clone the Repository

   ```bash
   git clone https://github.com/J0hnG4lt/metabase-flightsql-driver.git
   cd metabase-flightsql-driver
   ```

2. Start all services

   ```bash
   podman compose up -d
   ```

3. Wait for Metabase to be ready (takes ~1-2 minutes for JAR build + Metabase startup)

   ```bash
   # Check if Metabase is ready
   podman exec metabase curl -s http://localhost:3000/api/health
   ```

4. Run the automated setup script

   ```bash
   python scripts/metabase_setup.py
   ```

   This script will:
   - Perform initial Metabase setup (creates admin user)
   - Create an API key for automation
   - Configure database connections (GizmoSQL and Spice.ai)
   - Create a comprehensive test dashboard with 32 cards and 5 field filters

5. Open Metabase at http://localhost:3000
   - Email: `admin@metabase.local`
   - Password: `Metabase123!`

### Building the Driver Manually

If you prefer to build locally:

```bash
lein uberjar
```

The docker-compose file also contains a builder service, so don't worry if you have issues when installing lein and clojure.

## Docker Services

| Service | Port | Description |
|---------|------|-------------|
| metabase | 3000 | Metabase BI tool |
| postgres | 5432 | Metabase application database |
| spiced | 50051, 8090, 9090 | Spice.ai Flight SQL server (API-key auth) |
| spiced-anon | 50052 | Spice.ai without auth (anonymous-connection testing) |
| gizmosql | 31337 | GizmoSQL Flight SQL server (DuckDB-based) |
| influxdb3 | 8181 | InfluxDB 3 Core (bearer-token-only; seed via `scripts/setup_influxdb3.py`) |
| builder | - | Builds the driver JAR |

Optional overlay profiles (not in the default stack): **TLS/mTLS**, **OAuth2/Keycloak**, **Doris**, **StarRocks**, and **Dremio** (`docker-compose.dremio.yaml`, port `9047` UI / `32010` Flight SQL — the Apache Iceberg backend).

### Optional TLS/mTLS profile

```bash
./scripts/generate_tls_certs.sh    # local CA + server cert + mTLS client cert into ./tls/
podman-compose -f docker-compose.yaml -f docker-compose.tls.yaml up -d
```

Adds `gizmosql-tls` (31338, CA-signed TLS) and `gizmosql-mtls` (31339, requires client certificates), and mounts `./tls` into Metabase at `/opt/flightsql-tls` so the driver's *Server CA certificate*, *mTLS client certificate/key* fields can reference the files.

### Optional OAuth2 profile (Keycloak)

```bash
python scripts/generate_oauth_config.py    # deterministic RS256 signing key + Keycloak realm
podman-compose -f docker-compose.yaml -f docker-compose.oauth.yaml up -d keycloak gizmosql-oauth
```

Adds Keycloak (host port 8180, realm `flightsql` with client-credentials clients minting `role=admin` and `role=readonly` tokens) and `gizmosql-oauth` (host port 31340; verifies JWT signature/issuer/audience). Connect from Metabase with Username `token` and the OAuth access token as Password — the readonly-role token is SELECT-only.

> Note: GizmoSQL **Core** accepts external JWTs only via that handshake convention. The Arrow JDBC `oauth.*` client-credentials flow fetches and sends the token correctly, but Core's bearer-header path only accepts its own session tokens (external bearer headers are an Enterprise/JWKS capability — or use Dremio).

### Optional Apache Doris profile (MySQL-dialect backend)

Doris is the first **MySQL-dialect** Flight SQL backend in the test matrix (all
others are DuckDB/DataFusion-flavored) and the only one supporting `SET ROLE`
and full DDL/DML writes — the target for connection impersonation and the canary
for SQL-dialect divergence.

```bash
# host prerequisites for the Doris BE (a C++ process):
podman machine ssh "sudo sysctl -w vm.max_map_count=2000000 && sudo swapoff -a"

podman-compose -f docker-compose.yaml -f docker-compose.doris.yaml up -d doris
python scripts/setup_doris.py     # waits for a live BE, loads sample schemas
```

Connect from Metabase with **host `doris`, port `8070`, user `root`, empty
password**, and **Additional options `useServerPrepStmts=false`** (required —
Arrow Flight SQL + Doris don't support prepared-statement parameters). Doris
"databases" appear as Metabase schemas (`sales`, `hr`, `analytics`).

> **Known limitation:** the Doris BE does not become usable under
> podman-on-Windows (WSL2/Hyper-V) — the BE's heartbeat service never reaches the
> FE. The FE (Java) is fine; the backend simply never goes "alive". Run this
> profile on **native Linux** (or Linux CI). The pytest suite auto-skips Doris
> unless `scripts/setup_doris.py` confirmed it's usable (`DORIS_READY` gate).

### Optional Dremio profile (Apache Iceberg lakehouse)

Dremio is a native lakehouse engine that speaks Arrow Flight SQL and stores
tables as **Apache Iceberg** — so this profile is how the suite proves Metabase
can read (and write) Iceberg through the driver. Unlike Doris/StarRocks,
single-node Dremio is **one JVM that serves Flight results inline** (no
advertised `127.0.0.1` endpoint), so it works from a separate Metabase container
on any host — including podman-on-Windows.

```bash
podman-compose -f docker-compose.yaml -f docker-compose.dremio.yaml up -d dremio
python scripts/setup_dremio.py    # bootstraps admin, adds an Iceberg source, seeds tables + a PAT
```

Connect from Metabase with **host `dremio`, port `32010`, user `dremio`, password
`dremio123`** (or toggle the token option on and paste the **Personal Access
Token** from `.env` as `DREMIO_PAT`). The seeded Iceberg tables surface as schemas
`wh.sales` / `wh.hr` / `wh.analytics`. Dremio wants ~4 GB RAM and ~1–2 min to
boot; the pytest suite auto-skips it unless `scripts/setup_dremio.py` succeeded
(`DREMIO_READY` gate).

### CSV uploads (writable backends)

Uploads are double-gated: tick **"Writable backend (enable CSV uploads)"** in the connection's advanced options (only for servers that accept DDL/DML over Flight SQL — GizmoSQL/DuckDB, Doris, StarRocks), then pick the database under **Admin → Settings → Uploads** (schema e.g. `main`). Uploading a CSV creates a typed DuckDB table plus a Metabase model; appends via the table menu work too. Read-only backends (InfluxDB 3, Spice datasets, ROAPI) reject uploads with a clean error even if selected.

### Connecting to InfluxDB 3

Enable the token toggle, paste the admin token (in `.env` as `INFLUXDB3_TOKEN` after seeding), and set **Additional options** to `database=<your-db>` (forwarded as a gRPC header). Tip: add a schema-filters *exclusion* for `system` to keep InfluxDB's internal tables out of sync.

## Configuration

When setting up the connection in Metabase, the driver registers under the name `:arrow-flight-sql` with `:sql-jdbc` as its parent. The main configuration properties include:

- **Host**: (Default: localhost) – The server's hostname or IP address.
- **Port**: (Default: 443) – The port to use for the connection.
- **Authentication** – controlled by the *"Authenticate with a token instead of username/password"* toggle:
  - *Toggle off (default)*: Username + Password → Flight handshake basic auth (GizmoSQL, Dremio, Doris, StarRocks, Denodo…).
    - **Spice.ai API key**: leave Username empty, put the key in Password.
    - **GizmoSQL external JWT**: set Username to the literal `token`, JWT in Password.
  - *Toggle on*: Bearer token / PAT / API key → sent as `Authorization: Bearer …` (InfluxDB 3 tokens, Dremio PATs, pre-issued JWTs).
  - *Anonymous*: leave all credential fields blank (ROAPI, kamu, Ballista, Spice.ai without auth).
- **Catalog**: (Optional) - The name of the catalog to use.
- **Advanced**: server CA certificate, mTLS client certificate/key (PEM secrets), connect timeout, and free-form additional JDBC options (`threadPoolSize`, `retainAuth`, `oauth.*` for OAuth 2.0 client-credentials/token-exchange, or any custom parameter — unknown parameters are forwarded to the server as gRPC headers, e.g. `database=<db>` for InfluxDB 3).
- **Use Encryption**: (Default: true) – Enable or disable TLS. Switch off for local plaintext servers (the docker-compose demo does this explicitly).
- **Disable Certificate Verification**: (Default: false) – Only enable for servers with self-signed certificates.

## Project Structure

```
.
├── src/metabase/driver/       # Driver source code
│   └── arrow_flight_sql.clj
├── resources/                 # Plugin manifest
│   └── metabase-plugin.yaml
├── scripts/                   # Automation scripts
│   └── metabase_setup.py
├── gizmosql/                  # GizmoSQL configuration
│   └── init.sql               # Test data (sales, hr, analytics schemas)
├── spice/                     # Spice.ai configuration
│   └── spicepod.yaml
├── data/                      # Parquet files for Spice.ai
├── docker-compose.yaml        # Container orchestration
└── CLAUDE.md                  # Development guide
```

## End-to-End Testing

The project includes comprehensive end-to-end testing:

```bash
# Clean start (removes all data)
podman compose down -v
podman compose up -d

# Wait for Metabase, then run setup
python scripts/metabase_setup.py

# Run the pytest e2e suite (see tests/e2e/README.md for optional stacks)
python -m pytest tests/e2e -v
```

The suite covers connections/auth shapes, the full connector option matrix, every dashboard card across 11 visualization types, metadata refreshes, MBQL/segments/metrics/pivot, TLS/mTLS, anonymous auth, and InfluxDB 3 (modules auto-skip when their optional stack isn't running).

The setup script creates a test dashboard with:
- **32 cards** with various chart types (scalar, bar, pie, line, area, table, gauge, funnel, scatter, progress)
- **5 field filters** (Order Status, Country, Department, Marketing Channel, Date Range)
- **3 test schemas** in GizmoSQL (sales, hr, analytics)

### Claude Code Integration

If using Claude Code, run `/e2e-test` for guided end-to-end testing instructions.

## Features Tested

- SQL Native queries
- Graphical query editor
- Database syncs
- Field filters (single table and JOINs with table aliases)
- Dashboard rendering with multiple concurrent queries
- Connection pooling stability
- Date/time type handling
- Actions (write-back): basic model create/update/delete + custom SQL actions (`tests/e2e/test_actions.py`)

## Troubleshooting

### `Could not initialize class ...arrow.memory.RootAllocator`
Metabase is running on JDK 24+ (the v0.63 image ships JDK 25) without the required Arrow JVM flags — see **Java / JVM requirements** above.

### `podman compose` fails with `root@127.0.0.1: Permission denied`
On Windows with Docker Desktop installed, `podman compose` delegates to
`docker-compose.exe` (an external compose provider) which tries to reach a
Docker host over SSH and fails. Use the pip-installed `podman-compose`
instead: `pip install podman-compose`, then `podman-compose up -d`.

### Check Metabase logs
```bash
podman logs metabase 2>&1 | tail -100
```

### Check connection pool status
```bash
podman logs metabase 2>&1 | grep "connections:"
```

### Rebuild driver after code changes
```bash
podman compose down metabase builder
podman compose up -d builder
# Wait ~30 seconds for build
podman compose up -d metabase
```

## License

Copyright © 2025-2026 Georvic Tur

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](LICENSE).

## AI-Assisted Development

This project was built with help from ChatGPT and Claude Code, along with reference to the Metabase repository and several of its existing drivers. While I'm not a Clojure developer by background, these tools made development much more approachable.

## Contributing

Contributions are welcome! If you have suggestions or improvements, please open an issue or submit a pull request.

## Contact

For additional information or support, please open an issue in the repository or contact the maintainer at [georvic.tur@gmail.com].
