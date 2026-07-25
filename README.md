# Metabase Arrow Flight SQL Driver

A Clojure library that enables Metabase to connect to databases using the Apache Arrow Flight SQL JDBC driver. This driver integrates Arrow Flight SQL into Metabase, delivering enhanced performance and advanced SQL querying capabilities.

My main goal was to allow Metabase to use [Spice.ai OSS](https://spiceai.org/docs) as a cache layer.

## Features

- JDBC-based integration: Leverages the Apache Arrow Flight SQL JDBC driver.
- Flexible configuration: Supports custom connection properties such as host, port, user, password, token, and encryption.
- Custom Schema Sync: Implements table and column description methods for seamless Metabase integration.
- Timestamp Conversion: Automatically converts TIMESTAMP columns to local date-time objects.
- Field Filters: Full support for Metabase field filters including table aliases for JOIN queries.

## Compatibility

CI builds one jar per supported Metabase release line (see release assets):

| Driver release | Metabase | Arrow Flight SQL JDBC |
|---|---|---|
| unreleased (main) | v0.62.5 (`-mb62` jar), v0.63.1 (`-mb63` jar) | 19.0.0 |
| 0.0.9 | v0.62.4 | 18.2.0 |
| 0.0.5 – 0.0.8 | v0.55 – v0.62 | 18.2.0 |

## Upgrading from 0.0.x

Drop-in: replace the plugin jar (pick the `-mb62`/`-mb63` release asset matching your Metabase line) and restart Metabase. Existing connections keep working — the driver auto-detects legacy details (plain username/password or token) and backfills the new auth toggle on read.

After upgrading, open each Flight SQL connection in **Admin → Databases** and hit **Save** once: this persists the backfilled auth flag and re-validates the connection.

Behavior changes to be aware of:

- **New** connections default to *Use Encryption ON* (previously off). Existing connections keep their stored setting.
- Tokens entered through the admin UI now actually work (previously only API-created connections could authenticate with tokens).
- `convertTimezone()` no longer appears in the expression editor — it was advertised but never worked.
- FK metadata is no longer probed during sync (it always returned nothing anyway); Metabase-side semantic/FK settings are unaffected.
- Release assets are now named per Metabase line (`arrow-flight-sql.metabase-driver-mb62.jar`, `-mb63.jar`) — update any download automation.
- Built and tested against Metabase v0.62.5 and v0.63.1; bundles Arrow Flight SQL JDBC 19.0.0.

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

## Troubleshooting

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
