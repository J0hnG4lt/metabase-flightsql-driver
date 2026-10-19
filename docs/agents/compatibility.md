# Isolated Metabase compatibility testing

Use Python 3.11+, requests, Podman and pip-installed podman-compose. This stack
has no fixed container names and uses a separate H2 application-data volume.
Keep the same `-p` project argument throughout. Build before starting Metabase:

```text
podman-compose -p flightsql-compat -f docker-compose.compat.yaml run --rm builder
podman-compose -p flightsql-compat -f docker-compose.compat.yaml up -d metabase
python scripts/dev.py health --url http://localhost:3001
```

The following environment setting is PowerShell syntax (in Bash use
`export METABASE_URL=http://localhost:3001`):

```powershell
$env:METABASE_URL = 'http://localhost:3001'
python scripts/ci_bootstrap.py
python scripts/compat_smoke.py
```

Use an isolated checkout: bootstrap writes its API key to that checkout's ignored
`.env`. The smoke test creates and cleans up its own connection, verifies metadata,
native results and structured date filtering. Optional `scripts/agent_smoke.py`
checks the official MCP/CLI surfaces on 64.

Override `METABASE_IMAGE`, `METABASE_PORT` and `METABASE_URL` to test another
runtime on a separate project. **Use a new project/application-data volume for
each version**; downgrading a migrated Metabase database is not supported. A
passing local source jar is supplemented by CI's AOT jar tests.

```text
podman-compose -p flightsql-compat -f docker-compose.compat.yaml stop
```

Stopping preserves the isolated test volume. Delete it only when explicitly
requested, using the reset workflow. Do not resize the Podman VM without asking.
