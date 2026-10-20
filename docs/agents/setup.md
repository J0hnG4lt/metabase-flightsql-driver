# Claude Code and Codex setup

Open this repository as the workspace, rather than starting in a sibling checkout.
Claude reads `CLAUDE.md` importing `AGENTS.md`; Codex reads `AGENTS.md` directly.
The six workflows have runtime-specific adapters around the same shared procedures.
Use Python 3.11+ for the helper/checker, Node.js 20.6+ for the official CLI,
and pip-installed podman-compose for local containers.

## Official Metabase MCP (preferred)

Both `.mcp.json` and `.codex/config.toml` point to
`http://127.0.0.1:3000/api/metabase-mcp`. In Metabase's Admin > AI > MCP, enable
the server and ensure AI features are enabled. An LLM provider is not required.
Set `MB_SITE_URL` to the exact external URL clients use; compose configures it.
Use the explicit IPv4 address for these local defaults: on some Windows hosts
Node resolves `localhost` only to `::1`, while Podman forwards the port on IPv4.

- Claude Code: start `claude`, open `/mcp`, select `metabase` and authenticate.
- Codex: trust/open this repository, check `codex mcp list`, then run
  `codex mcp login metabase`. Project TOML is loaded only for trusted projects.

The browser login grants access with your Metabase permissions. Connections made
before 64 need authentication again. Leave the detected OAuth client registration
settings in place; do not copy API keys into the OAuth configuration. Keep actual
credentials in the client's credential store. To use another host or port,
configure a local/user server entry rather than committing machine-specific URLs.

The compatibility test probes OAuth discovery and also exercises MCP with its
isolated demo API key. That automated transport check does not complete a human's
OAuth consent flow. MCP tools can query and create content; the old statement
that the official MCP is useful only for exploration is obsolete.

## Official Metabase CLI

This repo tests `@metabase/cli@0.5.1`. To install the `mb` command yourself:

```text
npm install -g @metabase/cli@0.5.1
mb auth login --url http://127.0.0.1:3000 --profile flightsql-local
mb auth status --profile flightsql-local
mb skills get core --profile flightsql-local
mb database list --profile flightsql-local --json
```

For the local test stack, the cross-platform wrapper runs the same pinned package
through npx, maps `.env`'s `METABASE_API_KEY` to `MB_API_KEY` and the selected URL
to `MB_URL`, and does not create a saved CLI login profile:

```text
python scripts/metabase_cli.py database list --json
python scripts/metabase_cli.py skills get core
python scripts/metabase_cli.py query --help --json
```

Set `METABASE_URL` and `METABASE_API_KEY` in the process environment to override
the demo defaults. For existing user profiles/OAuth use `mb` directly, so the
wrapper cannot accidentally substitute demo credentials. JSON payload files
under `.logs/` avoid shell quoting differences. Premium CLI commands still need
the corresponding plan; basic driver validation does not.

## Optional community MCP fallback

The previous `@imlewc/metabase-server` integration remains available through
`python scripts/community_mcp.py`. It reads the current checkout's ignored `.env`
or inherited `METABASE_API_KEY`, starts npx correctly on Windows and preserves
stdio for MCP. Codex has a disabled `metabase-community` entry; enable it locally
when needed. Claude can add it with:

```text
claude mcp add --transport stdio metabase-community -- python scripts/community_mcp.py
```

Run from the repo root. The official HTTP connection does not depend on this
package. Claude permission patterns are kept in `.claude/settings.json`; they
are not Codex permissions. Codex uses its normal sandbox/approval policy.

## Verify discovery

Run `python scripts/check_agent_assets.py`. In each fresh client, ask it to list
the active project instructions and workflows. Confirm the `flightsql-driver-expert`
role is discoverable if using specialists, and use the same procedure in either
runtime. Reopen/restart if a client cached the old asset catalog.

References: [Metabase MCP](https://www.metabase.com/docs/v0.64/ai/mcp),
[Metabase CLI](https://www.metabase.com/docs/v0.64/installation-and-operation/metabase-cli),
[Codex project guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md),
[Codex skills](https://learn.chatgpt.com/docs/build-skills),
[Codex MCP](https://learn.chatgpt.com/docs/extend/mcp?surface=cli).
