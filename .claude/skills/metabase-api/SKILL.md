---
name: metabase-api
description: Drive the local Metabase (localhost:3000) via REST API or MCP — API keys, database/card/dashboard endpoints, field filters with table aliases, dashboard parameter wiring. Use when scripting Metabase state, validating e2e results, or debugging cards/dashboards in the compose stack.
---

# Metabase API Cookbook (compose stack)

Auth: `python scripts/metabase_setup.py` creates the admin user and writes `METABASE_API_KEY` to `.env`. All calls: `-H "x-api-key: $KEY"`.

Prefer MCP when available: `.mcp.json` runs `@imlewc/metabase-server` against `METABASE_URL=http://localhost:3000` with that key (tools: list/execute cards, dashboards, SQL). Export `METABASE_API_KEY` from `.env` before starting Claude Code. The compose Metabase (≥ v0.60) also ships the official MCP server at `/api/metabase-mcp` (Admin > AI > MCP; OAuth browser flow — interactive use only).

## Endpoints

```bash
curl http://localhost:3000/api/health
curl -H "x-api-key: $KEY" http://localhost:3000/api/database
curl -H "x-api-key: $KEY" http://localhost:3000/api/database/{id}/metadata
curl -H "x-api-key: $KEY" http://localhost:3000/api/card/{id}
curl -H "x-api-key: $KEY" http://localhost:3000/api/dashboard/{id}
# run a card with parameters:
curl -X POST -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  http://localhost:3000/api/card/{id}/query \
  -d '{"parameters":[{"type":"string/=","value":["Delivered"],"target":["dimension",["template-tag","status"]]}]}'
```

Connection details note: when creating databases via `POST /api/database`, plain `token` works, but the UI stores secret-typed fields as `token-value` — test both shapes when touching auth code.

## Field filters

Single table: `SELECT * FROM orders [[WHERE {{status}}]]`

With JOINs the template tag **must carry the table alias**:

```json
{"status": {"id": "status", "name": "status", "display-name": "Order Status",
            "type": "dimension", "dimension": ["field", <field_id>, null],
            "widget-type": "category", "alias": "o.status"}}
```

```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id
WHERE 1=1 [[AND {{status}}]] [[AND {{country}}]]
```

## Dashboard parameters

```json
"parameters": [
  {"id": "status", "name": "Order Status", "slug": "status", "type": "string/=", "sectionId": "string"},
  {"id": "date_range", "name": "Date Range", "slug": "date_range", "type": "date/all-options", "sectionId": "date"}
]
```

Mapping onto a card's template tag:

```json
{"parameter_id": "status", "card_id": <card_id>,
 "target": ["dimension", ["template-tag", "status"]]}
```

Gotcha: cards render in question view but not on a dashboard → copy the card's `visualization_settings` onto the dashcard.
