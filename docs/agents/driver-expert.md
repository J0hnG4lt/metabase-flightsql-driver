# Flight SQL driver specialist

Read [driver development](driver-dev.md) before changing internals. The driver
and manifest share authentication, feature and pooling invariants. Keep each
change scoped; name the relevant multimethods and verify the behavior through
Metabase, not compilation alone.

Use Metabase's pinned in-tree PostgreSQL and Vertica implementations as references.
Inspect the exact version's driver changelog for compatibility work. Do not assume
DuckDB date functions or writable features work on every Flight SQL server.
Auth includes user/password, Spice's blank-user API key, bearer tokens,
GizmoSQL external JWTs and anonymous connections; PEM secrets support TLS/mTLS.
The manifest uses `use-token`, and write features are opt-in per connection.

For REST/MCP/CLI operations use [metabase-api](metabase-api.md). Record durable
findings in the shared workflow relevant to the behavior, rather than relying
on Claude-only project memory. State exactly which versions and backends passed.
