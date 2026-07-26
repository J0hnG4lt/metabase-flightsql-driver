# CLAUDE.md — Metabase Arrow Flight SQL Driver

Clojure Metabase driver (`:arrow-flight-sql`, parent `:sql-jdbc`) over the Apache Arrow Flight SQL JDBC driver. Demo backends: GizmoSQL (DuckDB) and Spice.ai OSS; the same driver reaches Dremio, InfluxDB 3, Doris, StarRocks, and any other Flight SQL server.

## Skills (read these before working in their area)

- **[driver-dev](.claude/skills/driver-dev/SKILL.md)** — editing `src/` or the plugin manifest: build paths, rebuild cycle, pooling/connection-options invariants, auth-mode mapping, known failure signatures.
- **[metabase-api](.claude/skills/metabase-api/SKILL.md)** — scripting/validating Metabase via REST or MCP: API keys, field filters with aliases, dashboard parameters.

## Commands

- `/e2e-test` — full clean-slate end-to-end test (compose up → setup script → assertions).
- `/rebuild-driver` — rebuild the driver jar and restart Metabase after a code change.
- `/upgrade-metabase <ref>` — bump the Metabase pin (compose + CI matrix) with the driver-changelog checklist.

## Layout

```
src/metabase/driver/arrow_flight_sql.clj   # the whole driver
resources/metabase-plugin.yaml             # manifest: auth-method dropdown, secrets, schema-filters
scripts/metabase_setup.py                  # e2e: admin, API key -> .env, connections, 32-card dashboard
docker-compose.yaml                        # metabase + postgres + spiced + gizmosql + builder
gizmosql/init.sql                          # 3 catalogs (memory/warehouse/staging), sales/hr/analytics
spice/spicepod.yaml                        # Spice v2 pod, api-key auth
.github/workflows/build.yaml               # lint + matrix build {v0.62.5, v0.63.1} + manual release
```

## Build truth

Two build paths produce different artifacts — see the driver-dev skill. Compose uses the `builder` (lein) jar and **compile errors only appear in `podman logs metabase` at plugin load**; CI/releases use `bin/build-driver.sh` inside a Metabase checkout.

## Everyday commands

```bash
podman compose up -d                      # start everything
python scripts/metabase_setup.py          # setup + test dashboard; writes .env
podman logs metabase 2>&1 | tail -100
podman logs metabase 2>&1 | grep -i "hash.*changed\|connections:"   # pool health
```

Metabase: http://localhost:3000 (admin@metabase.local / Metabase123!)

## MCP

`.mcp.json` runs a Metabase MCP server (`@imlewc/metabase-server`) against the local stack — export `METABASE_API_KEY` from `.env` first. The bundled official MCP server (`/api/metabase-mcp`, OAuth) suits interactive exploration only.

## Non-negotiables

1. `connection-details->spec` must be hash-stable (pool invalidation otherwise) — named fns only.
2. Secret-typed properties resolve via `driver-api/secret-value-as-string` — never `(:token details)` alone.
3. No `DatabaseMetaData` transaction/isolation probes (NPE on incomplete `GetSqlInfo` servers).
4. Before bumping Metabase: read `docs/developers-guide/driver-changelog.md` at the target ref; unknown feature keywords throw at load.
5. Verify changes with `/e2e-test` — compilation alone proves nothing on the compose path.
