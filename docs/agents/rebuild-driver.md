# Rebuild the driver

Run `python scripts/dev.py rebuild` from the repository root. This stops only
Metabase, runs the builder in the foreground, checks its exit code, restarts
Metabase, polls health with a timeout and prints recent startup logs. It keeps
application data and `.env`. Other required services must already be running.

The Leiningen jar contains source, so a successful build alone does not prove
the driver compiles. Check startup logs for `arrow-flight-sql` and exceptions;
then execute a query with [the CLI/API workflow](metabase-api.md) or
[run the E2E tests](e2e-test.md).

For the isolated stack, use `-f docker-compose.compat.yaml -p flightsql-compat
--url http://localhost:3001` with the helper. Use `scripts/compat_smoke.py`
instead of the full demo setup for that two-service stack.
