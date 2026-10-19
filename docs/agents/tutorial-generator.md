# Tutorial generator

Create a real, runnable tutorial under `docs/tutorials/`, with screenshots of
the actual Metabase UI. Use [the tutorial matrix](../tutorials/README.md) to pick
a use case and backend. Follow [the Iceberg example](../tutorials/iceberg-lakehouse.md)
for the document shape: problem, when to use, prerequisites, steps, explanation,
variations and related tutorials.

## Deploy and verify

Find the backend's compose overlay and setup script. Use pip's `podman-compose`
with the base file and the selected overlay, then run `scripts/setup_<backend>.py`.
Do not deploy every optional backend. If a recipe is missing, create and verify
that small backend example first. Check a real query before provisioning content;
some backends lose their internal metadata after restart and need reseeding.

Use [the CLI/API workflow](metabase-api.md) to create the requested demo content.
Keep tutorial connections persistent; E2E fixtures delete their own connections.
Charts need explicit dimensions and metrics in `visualization_settings`. If a
card renders alone but not on its dashboard, check the dashcard's visualization
settings and parameter mappings. Repair a broken chart before capturing it.

## Capture the actual UI

Use the browser capability available in the current runtime (browser automation,
Playwright or an installed browser connector). Follow that tool's documentation;
Claude-specific `mcp__playwright__...` names are not portable to Codex.

1. Use a consistent viewport around 1440 by 900. Sign in to the selected demo.
2. Discover table, connection, question and dashboard IDs from the API.
3. Visit the relevant views: `/admin/databases/<id>/edit`, `/browse/databases/<id>`,
   `/table/<id>`, `/question/<id>` and `/dashboard/<id>`.
4. Wait for the relevant rows/charts to finish loading, not a fixed sleep alone.
   Handle an unsaved-question navigation dialog when the browser reports one.
5. Save images under this checkout's `docs/tutorials/images/`. Resolve an absolute
   destination from the current Git root. If a tool returns an image elsewhere,
   inspect the reported path and copy that specific file; never assume an outer
   repository or delete files using a broad wildcard.
6. Visually inspect screenshots for rendered data, readable labels and absent
   error/loading states. Use CSS-scale screenshots if the tool supports them.

Capture the views that explain the tutorial (connection, metadata, query/results,
dashboard), usually five or six. Embed relative links as `images/<slug>-NN.png`.
Add the tutorial to the matrix. Stop only services started for this task when
cleanup is requested; preserve volumes unless deleting them was authorized.
