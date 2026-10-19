# Upgrade Metabase

Use the exact target ref given in the request. If only a release line is given,
resolve its newest stable numeric hotfix tag; GitHub's release list can lag tags.
Confirm the official image exists before changing pins.

Read `docs/developers-guide/driver-changelog.md` **at that tag** in
[Metabase's repository](https://github.com/metabase/metabase), plus the API
changelog for setup/test code affected by the upgrade. Check removed methods,
feature keywords, secret resolution, metadata and SQL clause shapes. Metabase
64 removed `:sql-mbql5` and switched `:sql` to MBQL 5; do not reparent this
driver to the removed transitional driver.

Update all four pins together: `MB_BUILD_REF` in `build.yaml` and
`release-please.yml`, the main compose image, and the isolated compose default.
Update the required CI runtime matrix and README compatibility table. The shared
driver harness follows `MB_BUILD_REF`; preserve required coverage for Metabase 63
while the release continues to claim it.

Run [compatibility validation](compatibility.md), including a real temporal MBQL
filter. Build the AOT release artifact in CI and run it on each required runtime.
Only label a version verified when its runtime checks pass; report backend and
artifact limitations. Run `python scripts/check_agent_assets.py` to catch pin drift.
