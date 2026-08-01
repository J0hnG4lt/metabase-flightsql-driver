# Tutorial: A CRUD app on your data (Actions)

**Backend:** GizmoSQL (DuckDB) · **Also works with:** Dremio, Doris/StarRocks ·
**Level:** intermediate · **Time:** ~15 min

## Problem

You want a small internal app — a to-do list, an approvals queue, a lookup-table
editor — where people **create, edit, and delete rows** from a Metabase
dashboard, no separate app framework. Metabase **Actions** do exactly this: buttons
and forms that write back to your database. Through this driver they work on
writable, full-DML Flight SQL backends.

## When to use this

- Simple internal CRUD over a table (todos, statuses, config/lookup tables).
- Data-entry or correction workflows next to your analytics.
- You want write-back **buttons/forms**, not just SQL.

> **Backend must support full DML.** GizmoSQL/DuckDB, [Dremio](../backends/dremio.md)
> (Iceberg), and [Doris/StarRocks](../backends/doris-starrocks.md) qualify.
> **Spice is INSERT-only**, so basic (edit/delete) actions won't work there — see
> [write-back / actions](writeback-actions.md).

## What you'll build

A **Todo app**: a dashboard showing a todos table with a **➕ New Todo** button
that opens a form and writes a new row back to GizmoSQL.

![The Todo app dashboard: a table plus a New Todo action button](images/crud-01-dashboard.png)

## Prerequisites

The base stack with a **GizmoSQL** connection ([backends/gizmosql](../backends/gizmosql.md)),
Metabase at http://localhost:3000.

---

## Step 1 — Enable Actions on the connection

Two switches (Actions write to your database, so they're deliberately opt-in):

1. **Connection:** Admin → Databases → gizmo → *Edit connection details* →
   *Advanced options* → turn on **Writable backend** and **Enable Actions
   (write-back buttons & forms)**.
2. **Database:** Admin → Databases → gizmo → *Turn on actions for this database*
   (the `database-enable-actions` setting).

## Step 2 — A table with an auto-increment PK

Actions target rows by **primary key**, and *create* forms expect the PK to be
auto-generated. In a native SQL cell:

```sql
CREATE SEQUENCE main.seq_todos START 1;
CREATE TABLE main.todos (
  id   INTEGER PRIMARY KEY DEFAULT nextval('main.seq_todos'),
  task VARCHAR,
  done BOOLEAN DEFAULT false
);
```

Sync the database, then in **Admin → Table Metadata → todos** set the `id`
field's type to **Entity Key**. (PK auto-detection varies across Flight SQL
backends, so this driver has you mark it — a one-time step per table.)

## Step 3 — Create a model + its actions

Turn the table into a **model** (New → Model → pick `main.todos`). A model that
wraps a single table automatically gets **basic actions** — *create*, *update*,
and *delete* — plus you can add **custom SQL actions** for bespoke writes.

## Step 4 — Put a button on a dashboard

Create a dashboard, add the todos question (a table), then **add an action
button**: *Edit dashboard → Add → Action*, pick the model's **Create** action,
and label it `➕ New Todo`. Save.

## Step 5 — Use it

Click **➕ New Todo**. Metabase opens a form built from the model's columns — note
the PK `id` is omitted (it auto-generates):

![The Create todo action form](images/crud-02-form.png)

Type a task and **Save**. The `INSERT` runs on GizmoSQL through the driver and the
table refreshes with the new row:

![The dashboard after the action inserted a new row](images/crud-03-result.png)

Wire up **Edit** and **Delete** the same way (the *update* / *delete* actions), or
let users click a row to edit it — full CRUD from the dashboard.

---

## How it works

```
button → form → Metabase Action ──(INSERT/UPDATE/DELETE via arrow-flight-sql)──▶ GizmoSQL
                  create/update/delete                 driver
```

The driver advertises the `:actions` / `:actions/custom` features (gated on the
Enable-Actions toggle) and reuses Metabase's `perform-action!*` machinery, with a
few Flight-SQL-specific pieces (autocommit "transactions", value casting, and an
inline row-lookup after insert since Flight SQL returns an update count, not
generated keys). Covered by [test_actions.py](../../tests/e2e/test_actions.py).

## Variations & gotchas

- **Full-DML backends only.** Spice is INSERT-only (no edit/delete). InfluxDB 3
  and read-only sources don't advertise actions at all.
- **Mark the PK** (Entity Key) — required for basic actions; auto-detection
  varies across backends so it's a manual step here.
- **Auto-increment the PK** (a sequence/identity) so *create* can omit it.
- **Custom SQL actions** handle writes the basic actions don't — any
  parameterized `INSERT/UPDATE/DELETE` with `{{template-tags}}`.
- **Permissions.** Creating/running actions is admin-gated; restrict write access
  with Metabase permissions + the backend's own RBAC.
- Basic Actions need a model that wraps a **single** table.

## Related

- [Write-back to your lakehouse](writeback-actions.md) — SQL-level writes (incl. Spice INSERT)
- [Transformations inside Metabase](transformations-in-metabase.md) · Backend: [GizmoSQL](../backends/gizmosql.md)
