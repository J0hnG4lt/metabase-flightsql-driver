# Changelog

## [0.1.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.0.9...0.1.0) (2026-07-26)


### Features

* auth-method dropdown covering all Flight SQL auth modes + mTLS/TLS options ([228d5d1](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/228d5d1072bcc345e882aced9c0784094e40b046))
* automated releases via release-please + categorized notes + asset attachment ([3a602fe](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/3a602fed6b6839d0c781a792d8a03a59df0e3da6))
* Claude Code integration — MCP server, skills, agent, commands, curated settings ([20b4634](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/20b46344b6ab29cf3e1ccb4f59e10a06c094a5d2))
* CSV uploads for writable backends (per-connection gated) ([e734e04](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/e734e04abbb2b5879d06c47ce72587565c99fa4d))
* Metabase shared-harness test extensions + operations e2e (caching, x-rays, alerts) ([b54b348](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/b54b3488d4ae6752608d10cd6c71f57f00dade5b))
* OAuth2 test profile — Keycloak-minted JWTs with role enforcement ([5d2a489](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/5d2a489e7834cabf67ae9cb8849c6df59e321fc9))
* schema-filters support + portable sync queries ([d0d454c](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/d0d454cdd190dfc64c85156e1d33ccd9fbe06707))
* TLS/mTLS profile, InfluxDB 3 + anonymous backends, pytest e2e suite ([6579d2c](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/6579d2c091ae46b40eb8c46bd7e2682050f98b84))


### Bug Fixes

* align encryption defaults across manifest, code, and README ([1858e72](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/1858e72ee218b6a40dcff779bcd4ed72f8d60b89))
* correct feature flags and remove describe-table-fks (Metabase 0.63 compat) ([31babe2](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/31babe2cf4d25526fe78572d444ff17d1567d8ee))
* describe-fields must emit NULL, not empty string, for field-comment ([99ad480](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/99ad4808b9a21210d843287c4fa08ecd6611761c))
* drop custom can-connect? override (refs [#11](https://github.com/J0hnG4lt/metabase-flightsql-driver/issues/11)) ([b5efa01](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/b5efa01615cc4c261912bd8d710340b61ac6fa0e))
* harden connection-options override (recursive guard, :write?, network timeout) ([941cb2a](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/941cb2ab5c7de3c9ccf80e07fc500c7b836598d4))
* linter-compliant auth design (use-token toggle) + normalize-db-details backfill ([c028a78](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/c028a789f52a44f4f5f4baa049e3bc5913079b17))
* resolve secret-typed token via driver-api (UI-created tokens were ignored) ([ff9a186](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/ff9a186fc2a2b1280847b9e439699896ba47f4d5))
* self-register driver for the test harness; skip FK DDL (DuckDB limitation) ([184f21a](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/184f21a3a5491bdd496379a5bf353fe8e8eeecb3))
* **test-extensions:** add explicit ids to generated INSERTs ([5d9e177](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/5d9e17780fcd8714fdf121d3b6827b5788c2c97a))
* typed temporal literals in :absolute-datetime (LocalDate crashed) ([136d203](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/136d203a808f52b1ba441ecfc29bf90cc3177fc6))


### CI

* authenticate setup-clojure tool downloads (runner rate-limit flake) ([1209a83](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/1209a835058a356f4ca2b6b0c0da2c263b52f94b))
* fix duplicate github-token key in lint step ([2a14417](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/2a14417724a74d2d3c19851afb04b428e1676303))
* Metabase version matrix, robust driver registration, lint, deliberate releases ([7754a4d](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/7754a4da46cba44c9f744a7d27d157121bc133ff))
* pin Clojure CLI and clj-kondo versions (kill setup-clojure rate-limit flake) ([9441e33](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/9441e33a3345f1415f046706b46bbd59b39a7db5))
* promote driver-test-suite to a required check ([8248295](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/82482959b267e259d0689890d0182f994ba27f98))


### Documentation

* e2e verified on the modernized stack; fix brittle checks ([2aa3e54](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/2aa3e54bcaf67612f0915d840f7c1b8836456674))


### Maintenance

* add Apache-2.0 LICENSE file and fix contradictory licensing text ([1232a9e](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/1232a9eab7f04f906c31d0d8bbf0b494446a72df))
* bump Arrow Flight SQL JDBC driver 18.2.0 -&gt; 19.0.0 ([4659bef](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/4659bef8be6119d36776fdd35035fe7906a2b18d))
* ignore python bytecode and pytest cache ([ff70f1a](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/ff70f1a06396034116e7ef279746d5788ce1df3b))
* refresh demo stack pins (Metabase 0.62.5, Spice 2.1.1, GizmoSQL v1.35.1) ([ce59e0c](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/ce59e0ce2e3150153cbb8fb867491ff8fbbf92be))

## Changelog

Maintained automatically by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/) (`feat:` → minor, `fix:` → patch,
`feat!:`/`BREAKING CHANGE:` → major). Do not edit released sections by hand.

## Historical releases (pre-automation)

- **0.0.9** (2026-07-25) — build pinned to Metabase v0.62.4
- **0.0.8** (2025-12-24) — GizmoSQL microservice demo + e2e infrastructure; multi-catalog support
- **0.0.7** (2025-10-29) — catalog connection property (gizmodata contribution)
- **0.0.6** (2025-09-14) — manifest linter fixes (gizmodata contribution)
- **0.0.5** (2025-07-31) — username/password auth restored; information_schema table listing
- **0.0.1 – 0.0.4** (2025-04 – 2025-07) — initial driver: Spice.ai API keys, column type mapping, CI builds
