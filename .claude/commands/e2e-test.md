---
description: Run end-to-end tests for the Metabase Arrow Flight SQL driver. Starts all services from scratch, runs setup, and validates the dashboard works.
allowed-tools: Bash, Read, Write, Glob, Grep
---

# End-to-End Test for Metabase Arrow Flight SQL Driver

Run a complete end-to-end test of the driver by starting all services fresh and validating functionality.

## Pre-requisites

- Podman must be installed and running
- Python 3 must be available

## Test Steps

### 1. Clean up existing environment

```bash
cd $PROJECT_ROOT
podman compose down -v
rm -f .env
```

This stops all containers, removes volumes, and deletes any stale `.env` file for a fresh start.

**Important**: The `.env` file contains API keys from previous runs. If not deleted, the setup script will try to use the old (now invalid) key and fail with 401 errors.

### 2. Start all services

```bash
podman compose up -d
```

This starts:
- **builder**: Compiles the driver JAR using Leiningen
- **postgres**: Metabase application database
- **spiced**: Spice.ai Flight SQL server (port 50051)
- **gizmosql**: GizmoSQL Flight SQL server (port 31337)
- **metabase**: Metabase BI tool (port 3000)

### 3. Wait for Metabase to be ready

```bash
for i in {1..60}; do
  if podman exec metabase curl -s -f "http://localhost:3000/api/health" >/dev/null 2>&1; then
    echo "Metabase is ready!"
    break
  fi
  echo "Waiting... attempt $i"
  sleep 5
done
```

### 4. Run the setup script

```bash
python scripts/metabase_setup.py
```

This script:
- Performs initial Metabase setup (creates admin user)
- Creates API key and saves to `.env`
- Creates GizmoSQL and Spice database connections
- Syncs database schemas
- Creates a comprehensive test dashboard with 32 cards and 5 field filters

### 5. Validate the dashboard

Open in browser: http://localhost:3000/dashboard/2

Or test via API (read API key from .env and use it directly):
```bash
# Read the API key from .env file
API_KEY=$(grep METABASE_API_KEY .env | cut -d'=' -f2)

# Validate dashboard exists with expected cards and parameters
podman exec metabase curl -s -H "x-api-key: $API_KEY" \
  "http://localhost:3000/api/dashboard/2" | python -c "
import sys,json
d=json.load(sys.stdin)
print(f'Dashboard: {d.get(\"name\")}')
print(f'Cards: {len(d.get(\"dashcards\",[]))}')
print(f'Parameters: {len(d.get(\"parameters\",[]))}')
"
```

**Expected output:**
```
Dashboard: FlightSQL Driver Test Dashboard v2
Cards: 32
Parameters: 5
```

### 6. Test a filtered query

Do NOT hardcode a card ID — discover a card mapped to the `status` dashboard
parameter (card IDs depend on creation order). Note: Metabase ≥0.62 returns
cards in MBQL5 shape (`stages[0].template-tags`), not legacy
`native.template-tags`.

```bash
python -c "
import json, urllib.request
key = next(l.split('=',1)[1].strip() for l in open('.env') if l.startswith('METABASE_API_KEY='))
def req(method, p, body=None):
    r = urllib.request.Request('http://localhost:3000'+p, method=method,
        headers={'x-api-key': key, 'Content-Type': 'application/json'},
        data=json.dumps(body).encode() if body else None)
    return json.load(urllib.request.urlopen(r, timeout=120))
dash = req('GET', '/api/dashboard/2')
card_id = next(dc['card_id'] for dc in dash['dashcards']
               for pm in dc.get('parameter_mappings') or []
               if pm.get('parameter_id') == 'status')
res = req('POST', f'/api/card/{card_id}/query', {
    'parameters': [{'type':'string/=','value':['Delivered'],'target':['dimension',['template-tag','status']]}]})
print('Status:', res.get('status'), '| Rows:', len(res.get('data',{}).get('rows',[])))
"
```

**Expected output:** `Status: completed` with rows > 0 (ground truth:
`SELECT COUNT(*) FROM sales.orders WHERE status='Delivered'` returns 6; a
JOIN/aggregate card returns fewer rows than that).

### 7. Verify connection pool health

```bash
podman logs metabase 2>&1 | grep -i "hash.*changed" | tail -5
```

**Expected**: No output (no "Hash of database details changed" warnings). If you see these warnings, the connection pooling fix in the driver may have regressed.

## Expected Results

| Check | Expected |
|-------|----------|
| Dashboard cards | 32 |
| Dashboard parameters | 5 (Order Status, Country, Department, Marketing Channel, Date Range) |
| Filtered query status | `completed` |
| Filtered query rows | > 0 (card discovered via the status parameter mapping) |
| Connection pool warnings | None ("Hash of database details changed" should not appear) |
| GizmoSQL tables synced | 13 tables in memory catalog (analytics, hr, sales schemas) |
| Spice tables synced | 1 table |

## Troubleshooting

### Volume removal error on Windows
When running `podman compose down -v`, you may see an error like:
```
error during connect: Delete ".../volumes/...": EOF
```
This is a known issue on Windows with podman. The containers are still removed successfully - you can safely ignore this error.

### Check Metabase logs
```bash
podman logs metabase 2>&1 | tail -100
```

### Check for connection pool issues
```bash
podman logs metabase 2>&1 | grep -i "hash.*changed\|connections:"
```

### Check GizmoSQL logs
```bash
podman logs gizmosql 2>&1 | tail -50
```

### Restart Metabase after driver changes
```bash
podman compose down metabase builder
podman compose up -d builder
# Wait for JAR build (~30 seconds)
podman compose up -d metabase
```

## Credentials

- **Metabase URL**: http://localhost:3000
- **Admin Email**: admin@metabase.local
- **Admin Password**: Metabase123!
- **GizmoSQL**: gizmosql:31337 (user: gizmosql, pass: gizmosql_password)
- **Spice**: spiced-container:50051 (token: 1234567890)
