# Metabase CLI, MCP and REST

Use the official Metabase CLI for scripted content operations and the built-in
MCP server for connected agent exploration. Read [setup](setup.md) first when
authentication or tools are missing. For the requested operation, discover
available commands/tools rather than assuming a fixed tool namespace.

The CLI ships version-matched guidance: `mb skills get core`, then
`mb skills get mbql`, `native-sql`, `dashboard` or another relevant topic.
Use `mb <command> --help --json` for schemas and `--json` for inspectable output.
`python scripts/metabase_cli.py` runs our pinned CLI with this checkout's demo
credentials; it accepts the same arguments as `mb`. For user-managed profiles,
use `mb` directly and pass the chosen `--profile` on each command.

For native MCP, initialize/discover tools and read the server's `learn` topic
before complex writes. MBQL queries go through `execute_query`; SQL through
`execute_sql`. Permission scopes and instance settings still apply. Inspecting
data does not authorize saving questions or scheduling deliveries.

The existing setup and E2E scripts deliberately exercise REST wire formats.
Keep them for regression tests, unsupported CLI operations and protocol debugging.
`scripts/metabase_setup.py` creates the demo admin and stores `METABASE_API_KEY`
in ignored `.env`; REST uses the `x-api-key` header. The CLI uses **MB_API_KEY**
and **MB_URL**. Never print keys or place them in command arguments. Bootstrap
and setup honor `METABASE_URL` (default `http://localhost:3000`).

When touching auth code test both legacy REST `token` and UI-created
`token-value`/`token-id` connection details. Use real metadata IDs; an existing
dashboard or question is not guaranteed to have the same ID after reset.

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
