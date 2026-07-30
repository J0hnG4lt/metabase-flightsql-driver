---
name: tutorial-generator
description: Generate a use-case tutorial for the Arrow Flight SQL driver end-to-end — deploy a backend PoC, provision demo content in Metabase, auto-capture real screenshots with Playwright, and write the tutorial markdown. Use when asked to "write a tutorial", "add a use-case doc", "generate docs with screenshots", or extend docs/tutorials/.
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, mcp__playwright__browser_navigate, mcp__playwright__browser_snapshot, mcp__playwright__browser_take_screenshot, mcp__playwright__browser_click, mcp__playwright__browser_type, mcp__playwright__browser_fill_form, mcp__playwright__browser_wait_for, mcp__playwright__browser_handle_dialog, mcp__playwright__browser_resize
---

# Tutorial generator

Produces a runnable, screenshot-illustrated tutorial under `docs/tutorials/`,
grounded in a live PoC. This is the process proven on
[`iceberg-lakehouse.md`](../../../docs/tutorials/iceberg-lakehouse.md) — copy its
shape.

Each tutorial pairs **one use case** with **the backend that showcases it** (see
the matrix in `docs/tutorials/README.md`). Everything is real: you deploy the
backend, drive Metabase, and screenshot what actually renders.

## Prerequisites

- `podman-compose` available (this repo uses **podman**, not docker — the setup
  scripts auto-detect via `CONTAINER_CLI`).
- The Playwright MCP tools available.
- Metabase admin login: `admin@metabase.local` / `Metabase123!`.

## Process (6 phases)

### Phase 1 — Choose use case + backend
Pick a row from the matrix (`docs/tutorials/README.md`). Confirm the backend has
a compose profile (`docker-compose.<backend>.yaml`) and a setup script
(`scripts/setup_<backend>.py`). If not, that PoC has to exist first — model it on
`docker-compose.dremio.yaml` + `scripts/setup_dremio.py`.

### Phase 2 — Deploy + seed + verify
```bash
podman-compose -f docker-compose.yaml -f docker-compose.<backend>.yaml up -d <backend>
python scripts/setup_<backend>.py
```
Verify the backend actually serves data before touching the UI (fresh backends
often need the setup re-run — e.g. Dremio's metadata store is ephemeral):
```bash
K=$(grep METABASE_API_KEY .env | cut -d= -f2-)
curl -sf -H "x-api-key: $K" -H 'Content-Type: application/json' -X POST \
  http://localhost:3000/api/dataset \
  -d '{"type":"native","database":<DBID>,"native":{"query":"SELECT 1"}}'
```

### Phase 3 — Provision demo content
Create a **persistent** Metabase connection (the e2e tests delete theirs) and any
demo dashboard, using the repo's own helpers so shapes are correct:
```python
import sys; sys.path.insert(0, "scripts")
from metabase_setup import MetabaseConfig, MetabaseClient
c = MetabaseClient(MetabaseConfig()); c.api_key = <API_KEY_FROM_ENV>
# c._request("POST","database",{...})           # create connection
# c.create_native_card(name, db_id, sql, display="bar",
#     visualization_settings={"graph.dimensions":["x"],"graph.metrics":["y"]})
# c.create_dashboard(...) ; c.add_cards_to_dashboard(dash_id, [...])
```
**Chart cards MUST set `graph.dimensions`/`graph.metrics`** or the card renders
"Which fields do you want to use for the X and Y axes?". Scalars and tables need
nothing. If a card won't render on the dashboard, drop it — a clean 3-card board
beats a broken 4th.

### Phase 4 — Capture screenshots (the hard part)

Drive Metabase with Playwright. Save every shot into `docs/tutorials/images/`.

1. **Set a clean viewport** once: `browser_resize(1440, 900)`.
2. **Log in:** `browser_navigate("http://localhost:3000")` → `browser_snapshot`
   → `browser_fill_form` the email/password → click **Sign in**.
3. **Navigate by direct URL, then `browser_wait_for(time: 2.5–4)`** (charts load
   async) **before** `browser_take_screenshot`. Useful URLs:
   | View | URL |
   |---|---|
   | Browse a DB's schemas/tables | `/browse/databases/<id>` |
   | One schema | `/browse/databases/<id>/schema/<schema>` |
   | Table data grid | `/table/<tableId>-<name>` |
   | Dashboard | `/dashboard/<id>` |
   | Connection health/sync | `/admin/databases/<id>` |
   | Connection form (host/port/auth) | `/admin/databases/<id>/edit` |
   | **Native SQL editor, pre-filled** | `/question#<base64>` (see below) |
4. **Pre-filled SQL editor** — base64 a query object into the hash, then click the
   run button (`Get Answer`, testid `run-button`):
   ```python
   import json, base64
   q={"dataset_query":{"database":<id>,"type":"native",
      "native":{"query":"SELECT ...\nFROM ..."}},"display":"table",
      "visualization_settings":{}}
   print("http://localhost:3000/question#"+base64.b64encode(json.dumps(q).encode()).decode())
   ```
5. **`browser_take_screenshot(filename="<slug>-NN-...png")` saves to the Playwright
   MCP's cwd, which is the OUTER repo root (`workspace/repos/georvic-claude-code/`),
   NOT this repo.** After each shot, copy it in:
   ```bash
   cp "C:/Users/DEEPGAMING/workspace/repos/georvic-claude-code/<slug>-NN-...png" \
      "C:/Users/DEEPGAMING/workspace/repos/metabase-flightsql-driver/docs/tutorials/images/"
   ```
   Clean the strays from the outer repo when done (`rm -f .../georvic-claude-code/<slug>-*.png`).
6. **Beforeunload dialog:** leaving an unsaved SQL question pops a "leave page?"
   dialog that blocks everything. Handle it with
   `browser_handle_dialog(accept: true)`, then re-navigate.
7. **Verify shots** by `Read`-ing 1–2 of the PNGs — confirm charts/data rendered
   (a slow backend can screenshot an empty chart; re-wait and re-shoot).

Aim for ~5–6 shots: connection form, browse tables, table data, SQL + results,
dashboard, connection health.

### Phase 5 — Write the tutorial
Create `docs/tutorials/<slug>.md` using the section shape from
`iceberg-lakehouse.md`: **Problem → When to use → What you'll build →
Prerequisites → numbered Steps (each with its screenshot) → How it works →
Variations & gotchas → Related**. Embed shots as `![alt](images/<slug>-NN.png)`.
Add the row to the matrix in `docs/tutorials/README.md` if missing.

### Phase 6 — Teardown (optional)
`podman-compose -f docker-compose.yaml -f docker-compose.<backend>.yaml down`
(keeps volumes). Mention leftover state to the user.

## Gotchas learned the hard way

- Playwright saves to the outer repo root — always copy shots into `images/`.
- `browser_navigate` while a question is unsaved → beforeunload dialog → handle it.
- Chart cards without `graph.dimensions`/`graph.metrics` show the axis prompt.
- Fresh backends may need `setup_<backend>.py` re-run (ephemeral metadata) — the
  Metabase connection persists but the backend forgets.
- Wait 2.5–4 s after navigation before screenshotting; charts render async.
- Use `scale: "css"` screenshots for consistent, reasonably sized PNGs.
