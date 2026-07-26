---
name: driver-dev
description: Develop and debug the Metabase Arrow Flight SQL driver — edit/rebuild/redeploy cycle, connection pooling and connection-options gotchas, auth modes, secret-typed properties, feature flags, and known failure signatures. Use whenever modifying src/metabase/driver/arrow_flight_sql.clj or resources/metabase-plugin.yaml, or when diagnosing driver behavior in the compose stack.
---

# Flight SQL Driver Development

## Two build paths — know which artifact you are running

| Path | Used by | What it produces |
|---|---|---|
| `bin/build-driver.sh arrow-flight-sql` inside a Metabase checkout | **CI / releases** | AOT-compiled `arrow-flight-sql.metabase-driver.jar` |
| `lein uberjar` via the compose `builder` service | **local compose demo** | driver *source* + Arrow JDBC classes; Metabase compiles the namespace at plugin load |

Consequence: the compose path surfaces compile errors only in **Metabase's startup logs** (`podman logs metabase`), not at build time. After any driver edit, always check the log for `arrow-flight-sql` load errors before testing behavior.

## Rebuild/redeploy cycle (compose)

```bash
podman compose down metabase builder
podman compose up -d builder        # ~30s: rebuilds the jar
podman compose up -d metabase
podman logs metabase 2>&1 | grep -i "plugin\|arrow-flight-sql" | tail -20
```

Or run the `/rebuild-driver` command.

## Critical invariants (violating these caused real bugs)

1. **Stable connection spec hash.** `connection-details->spec` must return identical values for identical details — an anonymous fn in the map changes the hash every call and Metabase invalidates the pool constantly. Symptoms: `Hash of database X details changed; marking pool invalid`, `connections: 0/0`, random dashboard failures. That's why `:cast` is a named fn (`arrow-flight-sql-cast-fn`).
2. **Secret-typed properties never arrive as their own name.** A `type: secret` property `foo` arrives as `foo-value` (uploaded) or `foo-id` (persisted). Always resolve via `driver-api/secret-value-as-string` / `secret-value-as-file!` (see `secret-string` / `secret-file-path` helpers). Plain keys are only a REST-API-created fallback.
3. **No transaction-isolation probes.** `DatabaseMetaData.supportsTransactionIsolationLevel` NPEs on servers with incomplete `GetSqlInfo` (the reason `do-with-connection-with-options` is overridden). Never add code that touches transaction metadata.
4. **Feature keywords are validated.** Declaring a feature Metabase doesn't know **throws at load**. Check `metabase.driver/features` for the pinned version before adding flags; `describe-table-fks` no longer exists on 0.63+.
5. **Set connection options only at depth 0** (`sql-jdbc.execute/recursive-connection?` guard) and honor `(:write? options)` — forcing read-only would break future uploads/actions.

## Auth modes (manifest `use-token` toggle ↔ spec builder)

The manifest linter (`bin/build/src/build_drivers/lint_manifest_file.clj` in
the Metabase repo) only allows property types `string/text/textFile/boolean/
secret/info/schema-filters/section/password` — no `select`, no `integer` —
which is why auth is a boolean toggle (the Databricks `use-m2m` pattern), not
a dropdown. The runtime supports more types than the yaml linter accepts;
always validate manifest changes against the linter (CI build does).

| Configuration | JDBC params | Servers |
|---|---|---|
| toggle off, user+password | `user=…&password=…` | GizmoSQL, Dremio, Doris, StarRocks, Denodo |
| toggle off, blank user + password | `user=&password=…` | Spice.ai API key |
| toggle off, username literally `token` + JWT password | `user=token&password=<JWT>` | GizmoSQL external JWT |
| toggle on + token secret | `authorization=Bearer …` | InfluxDB 3, Dremio PAT, pre-issued JWTs |
| everything blank | (nothing) | ROAPI, kamu, Ballista, unauthenticated Spice |

Legacy details without `use-token` are inferred, and `normalize-db-details`
backfills the flag so the admin form opens in the right mode.

mTLS/CA material: secret pem-cert props materialized to files → `tlsRootCerts` / `clientCertificate` / `clientKey`. `additional-options` is the escape hatch for `oauth.*`, `retainAuth`, `threadPoolSize`, and custom gRPC headers (unknown params are forwarded as headers — e.g. `database=<db>` for InfluxDB 3).

## Known failure signatures

| Log/symptom | Cause | Fix |
|---|---|---|
| `Hash of database details changed` | unstable spec (see invariant 1) | stable named fns/values |
| NPE in `ArrowDatabaseMetadata.getSqlInfoAndCacheIfCacheIsEmpty` | server lacks SqlInfo keys | keep metadata probes out; GizmoSQL fixed server-side in #34 |
| `Channel shutdown` on close | benign gRPC close noise | fixed by Arrow JDBC ≥ 19.0.0 (GH-863) |
| Bearer auth fails after gizmosql restart | JWT signing key rotated | stable `SECRET_KEY` in compose |
| "Prepared statement not found" | concurrent queries on unstable pool | invariant 1; check pool health |

## Reference

When implementing new multimethods, crib from Metabase's in-tree drivers: `src/metabase/driver/postgres.clj` (full-featured), `modules/drivers/vertica` (small, modern), `metabase/driver/sql_jdbc/` (defaults you may be overriding). Driver changelog: `docs/developers-guide/driver-changelog.md` in the Metabase repo — read it before every Metabase version bump.
