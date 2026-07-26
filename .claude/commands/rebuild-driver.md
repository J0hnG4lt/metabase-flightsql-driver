---
description: Rebuild the driver jar and restart Metabase after a code change, then verify the plugin loaded.
allowed-tools: Bash, Read, Grep
---

# Rebuild Driver

Rebuild the driver after changes to `src/` or `resources/` and verify Metabase loads it.

## Steps

### 1. Rebuild the jar

```bash
podman compose down metabase builder
podman compose up -d builder
```

Wait for the build to finish (~30s):

```bash
for i in {1..12}; do
  podman ps --format '{{.Names}}' | grep -q '^builder' || break
  sleep 5
done
podman logs builder 2>&1 | tail -5
```

The builder exits when done; its last lines should show `Created /builder/target/metabase-flightsql-driver-0.1.0-SNAPSHOT-standalone.jar`.

### 2. Restart Metabase

```bash
podman compose up -d metabase
```

### 3. Verify plugin load (CRITICAL)

The compose path loads driver *source* at startup — compile errors surface here and only here:

```bash
for i in {1..40}; do
  if podman exec metabase curl -s -f "http://localhost:3000/api/health" >/dev/null 2>&1; then break; fi
  sleep 5
done
podman logs metabase 2>&1 | grep -iE "arrow-flight-sql|plugin" | tail -20
podman logs metabase 2>&1 | grep -iE "error|exception" | grep -i "arrow" | tail -10
```

**Success**: a "registered plugin" / "Loading plugin" line for arrow-flight-sql and no arrow-related exceptions.
**Failure**: any `clojure.lang.Compiler$CompilerException` mentioning `arrow_flight_sql.clj` — fix the reported line and rerun this command.

### 4. Quick smoke (optional but recommended)

If `.env` exists, run a query through an existing card:

```bash
API_KEY=$(grep METABASE_API_KEY .env | cut -d'=' -f2)
podman exec metabase curl -s -H "x-api-key: $API_KEY" \
  "http://localhost:3000/api/database" | python -c "import sys,json; d=json.load(sys.stdin); print([x['name'] for x in d.get('data',d)])"
```
