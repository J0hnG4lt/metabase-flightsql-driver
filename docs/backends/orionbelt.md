# OrionBelt semantic layer

The [complete demo](../../demos/orionbelt/README.md) runs OrionBelt 2.34.0 over
DuckDB with Metabase 0.64.2.9 and the upstream OrionBelt MCP server. Follow the
[screenshot tutorial](../tutorials/orionbelt-shared-semantics.md) for setup,
shared metric definitions, dashboard filters and Claude/Codex exercises.

```sh
python demos/orionbelt/run.py up
```

Metabase runs at http://127.0.0.1:3002. Its Arrow Flight SQL connection uses
host `orionbelt`, port `8815`, no encryption or authentication, and disables
uploads and Actions. All published ports bind to loopback. Host clients use
Flight port `18815`; MCP is `http://127.0.0.1:19000/mcp`.

| Capability | Tested scope |
|---|---|
| Schema discovery | The commerce model's fields plus semantic metadata tables |
| Native semantic SQL | Dimensions and named measures/metrics, grouped and filtered |
| Dashboards | Nine saved cards, text/date parameters, five filter scenarios |
| OrionBelt MCP | Discovery, description, metric explanation, lineage, structured query execution |
| Metabase MCP | Native `execute_sql` against the Flight connection |
| Transforms / uploads / Actions | Disabled; this demo's semantic connection is read-only |
| Arbitrary notebook queries, other warehouses, production auth or load | Not established by this demo |

**Compatibility adapters are required for the pinned Flight image.** They fix
numeric connection probes, SQL catalog projections and result-column ordering
inside the OrionBelt container; the generic driver is unchanged. Empty results
still expose duplicated JDBC column headers, recorded separately from verified
row parity. See [compatibility notes](../../demos/orionbelt/COMPATIBILITY.md)
before upgrading or generalizing the result.
