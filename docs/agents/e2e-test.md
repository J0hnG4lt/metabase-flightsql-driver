# End-to-end verification

Use an existing local demo, or start it with `python scripts/dev.py up`.
Run `python scripts/dev.py test`: it waits for health, runs the existing setup
script and executes pytest. It does not delete containers, volumes or `.env`.
Setup creates demo connections/cards, and tests create and remove test content;
use this workflow only on a development instance.

After driver changes, [rebuild first](rebuild-driver.md). For a focused run:

```text
python scripts/dev.py test --pytest tests/e2e/test_core.py tests/e2e/test_features.py -v
```

Full coverage is documented in [the suite README](../../tests/e2e/README.md).
Report passed, failed and skipped tests separately; optional backends skip when
their profiles are absent. Check plugin-load errors and pool invalidation
warnings with `podman logs --tail 100 metabase`. Discover dashboard/card IDs
from the API; do not assume IDs from a previous run.

For Metabase version changes use [the isolated compatibility stack](compatibility.md).
Do not run a routine test by resetting the application database. When the user
explicitly requests deleting a test environment, the separate command is:

```text
python scripts/dev.py reset -p flightsql-compat -f docker-compose.compat.yaml --delete-data
```

This deletes that project's volumes. It preserves credential files; bootstrap
again after recreating the application database. The main demo has fixed
container names, so a different project name alone does not isolate that stack.
