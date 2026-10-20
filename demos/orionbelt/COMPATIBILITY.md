# Tested boundary and upgrade notes

This demo uses Metabase 0.64.2.9, the repository's Arrow JDBC 19.0.0 driver,
and the OrionBelt Flight/MCP 2.34.0 images pinned by digest in compose. It does
not modify the generic Metabase driver or vendor upstream source into the repo.

## Why the Flight entrypoint has an adapter

The initial run against the stock 2.34.0 Flight image exposed three issues.
[flight_compat.py](flight_compat.py) applies narrow adapters inside this isolated
container; [test_flight_compat.py](test_flight_compat.py) exercises actual Arrow
types and SQL expressions in the same image.

| Stock behavior observed | Adapter | Evidence |
|---|---|---|
| `SELECT 1` returns a UTF-8 string, but Metabase's standard JDBC check requires numeric 1 | Preserve numeric literal types; keep quoted `'1'` a string | Numeric and string probe regression |
| Quoted `information_schema` is misclassified; catalog SQL exposes Flight protocol names and does not apply expression projections correctly | Recognize quoted catalog names; evaluate single-table metadata SELECTs over model-derived Arrow catalog data in an in-memory DuckDB with external access disabled | Filtered, aliased `UPPER(data_type)` metadata regression and live Metabase schema synchronization |
| Compiler results group base measures before derived metrics, while advertised Flight columns follow requested order | Require unique/exact named columns, reorder to the advertised schema, cast and require matching types | A mixed `Revenue, Refunds, Order Count` regression and all parity scenarios |

The adapter does not change formulas, authentication checks in Metabase, query
planning, warehouse data or the upstream MCP server. For catalog queries it
accepts only one SELECT against `information_schema.tables` or `.columns`,
without joins, CTEs or subqueries; other shapes retain upstream handling.

Reviewed upstream implementation:
[OrionBelt source](https://github.com/ralforion/orionbelt-semantic-layer/tree/4f58ff7367dbee3fa646e40a92befb89d2e2623c)
(`server_catalog.py`, `server_execution.py`, `service/result_schema.py`) and
[MCP source](https://github.com/ralforion/orionbelt-semantic-layer-mcp/tree/f98d26207a4149903bb975cbf38fd5152c119a18).
The image digests, rather than a floating source branch, define the executable fixture.

## Empty-result metadata limitation

A direct Java JDBC probe against the same Flight endpoint reports two columns
for a nonempty `SELECT "Region", "Revenue" ...`, but four for its zero-row
equivalent. Metabase names the second set `Region_2`, `Revenue_2`. Both SQL and
Metabase MCP still return the correct empty row set. Sending a typed empty Arrow
batch did not resolve it, so no ineffective stream adapter is retained.

The verifier accepts only `expected_names + expected_names_with_2_suffix`, and
only when rows are exactly `[]`. It records the limitation and reports
`passed_with_known_limitations`. Other unexpected names, duplicate names,
truncated results and any nonempty result with extra columns fail. This is an
explicit exception for metadata, not a skipped empty-result test or a claim of
complete metadata parity. The dashboard's no-match experience is "no results".
The relevant client implementation is
[Arrow JDBC 19's Flight result set](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightJdbcFlightStreamResultSet.java).

## Modeling lessons caught by the oracle

1. **Independent facts need a shared leaf dimension.** With `Refund Events ->
   Orders`, requesting Net Revenue alone selected a refund-driven plan and gave
   385 instead of 675, because orders without refunds disappeared. Adding Units
   happened to select a different plan and return the correct 675. The current
   model uses three independent facts pointing to Order Context. The verifier
   tests both the complete metric set and every metric individually.
2. **COALESCE does not create missing groups.** The `covered_refunds` view supplies
   the order domain before grouping; the Refunds metric then converts NULL sums
   into zero. West appears even when Refunds is the only selected metric.
3. **Avoid object/metric label collisions.** The object is named `Refund Events`,
   while the business metric is `Refunds`. In this image, physical-name rewriting
   can replace a colliding quoted metric alias. Distinct names avoid that path.

These choices are visible in the source preparation and model. They are not
hidden client-specific SQL formulas. An oracle that merely queried the same
compiler would have missed the shared planning error.

## Upgrade procedure

1. Review the upstream fixes and licenses; change image versions and digests
   together in compose.
2. Run the three adapter regressions inside the candidate image. Remove an adapter
   only when its stock replacement passes the regression.
3. Run `python demos/orionbelt/run.py up --skip-build` to reload the model and
   restart pooled JDBC connections. A recreated container can have a new IP
   while Metabase's JVM still remembers its old address.
4. Require all 270 data-path comparisons, 45 dashboard-card checks, semantic
   explanation/lineage checks and repeat provisioning to pass. Review the
   compiled SQL, warnings, hashes and known limitations in the evidence file.
5. Recheck the actual UI and refresh the tutorial screenshots. Remove the empty
   metadata exception only after direct JDBC and Metabase empty results have
   the correct unique schema.

Tested locally with Podman on Windows/WSL2; CI runs the same fixture on Linux.
This does not establish compatibility for other database engines behind
OrionBelt, arbitrary SQL/MBQL, authentication modes, transforms or production load.
