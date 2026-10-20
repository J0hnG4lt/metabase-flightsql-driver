# Metabase Arrow Flight SQL driver

Shared instructions for Claude Code and Codex. Edit this file; `CLAUDE.md` imports it.

## Architecture and invariants

The driver is `src/metabase/driver/arrow_flight_sql.clj`, registered as
`:arrow-flight-sql` with parent `:sql-jdbc`. The manifest is
`resources/metabase-plugin.yaml`. Authentication uses a `use-token` toggle;
uploads/transforms and Actions are separately enabled per connection.

- Keep connection specs hash-stable: use named functions, not new closures.
- Resolve secret-typed properties through `driver-api/secret-value-as-string`
  or `secret-value-as-file!`; plain REST values are a legacy fallback.
- Skip JDBC transaction/isolation metadata probes: incomplete Flight SQL
  `GetSqlInfo` responses can throw. Set connection options only at depth zero
  and honor `:write?`.
- Metabase 64 compiles MBQL 5 directly. Temporal overrides also accept the
  legacy clauses used by Metabase 63. Do not add the removed `:sql-mbql5` parent.
- Verify backend-specific SQL on the backends being claimed. GizmoSQL and
  Spice have different capabilities; passing GizmoSQL does not prove all servers.

## Workflows

Shared procedures are under `docs/agents/`. Load only the relevant one.
Codex discovers `.agents/skills/`; Claude discovers `.claude/skills/` and the
existing command wrappers. Run from this repository's root.

| Work | Shared procedure | Claude Code | Codex |
|---|---|---|---|
| Driver or manifest changes | [driver-dev](docs/agents/driver-dev.md) | `/driver-dev` | `$driver-dev` |
| Metabase CLI, MCP, REST | [metabase-api](docs/agents/metabase-api.md) | `/metabase-api` | `$metabase-api` |
| Tutorial + real screenshots | [tutorial-generator](docs/agents/tutorial-generator.md) | `/tutorial-generator` | `$tutorial-generator` |
| Rebuild and verify loading | [rebuild-driver](docs/agents/rebuild-driver.md) | `/rebuild-driver` | `$rebuild-driver` |
| End-to-end verification | [e2e-test](docs/agents/e2e-test.md) | `/e2e-test` | `$e2e-test` |
| Upgrade Metabase | [upgrade-metabase](docs/agents/upgrade-metabase.md) | `/upgrade-metabase <ref>` | `$upgrade-metabase <ref>` |

The optional `flightsql-driver-expert` specialist exists in both runtimes.
Persistent technical knowledge belongs in these shared files; runtime-local
agent memories and permission settings are not portable.

## Local environment

Use `podman` and pip-installed `podman-compose`. Local Docker and the
`podman compose` shim are not supported. Linux GitHub Actions deliberately uses
Docker. Keep container-CLI autodetection in existing scripts.

PowerShell is the primary Windows shell. Prefer the Python helpers below to
Bash loops. Put local artifacts in `.logs/`; never hardcode another checkout.
Do not reset volumes for routine testing. Start the existing Podman VM as
needed; ask before resizing it because that restarts the VM.

## Build and validation

```text
python scripts/dev.py up
python scripts/dev.py rebuild
python scripts/dev.py test
python scripts/check_agent_assets.py
python -m unittest discover -s tests/unit -v
```

The local Leiningen jar contains driver source; compilation happens when
Metabase loads it. Inspect the plugin-load log and execute queries after a
driver change. CI/releases AOT-build inside a pinned Metabase checkout, then
run the same artifact on the required 64 and 63 runtimes. Workflow-only changes
need asset/helper checks, not unrelated driver tests.

For version upgrades use the isolated stack in
[compatibility testing](docs/agents/compatibility.md); never upgrade a valuable
application database as a smoke test. The compose demo is at localhost:3000;
the isolated stack defaults to localhost:3001. Bootstrap credentials are demo-only.

## Agent configuration maintenance

`scripts/check_agent_assets.py --write` generates the small runtime adapters
from its workflow map and shared references. CI checks coverage, content,
local links, MCP configuration and version-pin agreement. Edit shared
procedures rather than maintaining two copies of domain knowledge.

For MCP/CLI login and tool discovery follow
[agent setup](docs/agents/setup.md). Credentials stay in ignored local files
or the runtime's credential store. Review what an API/CLI command changes
before running it; a request to inspect data does not authorize creating content.
