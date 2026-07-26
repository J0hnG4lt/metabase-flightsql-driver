---
description: Bump the Metabase version pin (compose image + CI matrix) with the driver-changelog compatibility checklist.
allowed-tools: Bash, Read, Edit, Grep, WebFetch
argument-hint: <metabase-ref e.g. v0.63.2>
---

# Upgrade Metabase Pin to $1

## 1. Read the driver changelog FIRST

Fetch the driver changelog at the target ref and review every section between the current pin and `$1`:

```bash
gh api -H "Accept: application/vnd.github.raw" \
  "repos/metabase/metabase/contents/docs/developers-guide/driver-changelog.md?ref=$1" | head -200
```

Checklist per section:
- Removed multimethods we implement? (a `defmethod` on a removed defmulti fails the namespace load)
- Removed/renamed **feature keywords** we declare? (unknown keywords throw)
- Signature changes to: `describe-fields-sql`, `do-with-connection-with-options`, `connection-details->spec` conventions, secrets API?
- New required methods for features we claim (`:describe-fields`, etc.)?
- MBQL5 (`:sql-mbql5`) migration notes — our `sql.qp/->honeysql` overrides use the legacy clause shape.

## 2. Apply the bump

- `docker-compose.yaml`: `metabase/metabase:<current>` → closest release image for `$1`.
- `.github/workflows/build.yaml`: update/extend the `metabase-ref` matrix entry for that line.
- `README.md`: update the Compatibility table.

## 3. Verify

Run `/rebuild-driver`, then `/e2e-test`. Both must pass before committing. Commit as `chore: upgrade Metabase to $1`.
