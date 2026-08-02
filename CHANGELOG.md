# Changelog

## [0.6.1](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.6.0...0.6.1) (2026-08-02)


### Maintenance

* **deps:** align Clojure to 1.12.3 (Metabase 0.63 runtime); Arrow JDBC stays 19.0.0 ([61ad412](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/61ad412640f2dc34cc78eab8cd8f0b245509e87b))
* **deps:** align Clojure to 1.12.3; Arrow Flight SQL JDBC stays at latest (19.0.0) ([63a93d3](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/63a93d3c646a748b6395dfc1539d6ecf043c4d3c))

## [0.6.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.5.0...0.6.0) (2026-08-01)


### Features

* kamu (Open Data Fabric) backend — verifiable, time-travel BI ([4d99628](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/4d99628860845699fa61b5a12fc3890170881b43))
* kamu (Open Data Fabric) backend + verifiable time-travel BI tutorial ([71e0c3d](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/71e0c3d1307c097099b85b92e303e92fe60b85cf))

## [0.5.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.4.2...0.5.0) (2026-08-01)


### Features

* Metabase Actions — full-CRUD dashboard buttons/forms + tutorial ([88f940e](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/88f940e5acd58ec9afc541f11c97f53101c09353))
* Metabase Actions (write-back) for full-DML Flight SQL backends ([1b174c5](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/1b174c527d5cf1556d9c32671757fe126041060c))


### Documentation

* CRUD-app tutorial (Actions) + wire Actions through the docs ([7aa9110](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/7aa911085e7efe780c5898574fa8d7c5df49a387))
* full CRUD in the Todo-app tutorial (edit + delete + row-click) ([a2ebff9](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/a2ebff96c3d14796acfc0c373d8deb7c220e7b9f))

## [0.4.2](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.4.1...0.4.2) (2026-08-01)


### Maintenance

* sync plugin manifest version to 0.4.1 ([5a757eb](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/5a757eb97b7108abde71db00b2418e807ed665e1))
* sync plugin manifest version to 0.4.1 (matches latest release) ([6ce2da5](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/6ce2da5509ddc61800622508e6249566d7f2dd94))

## [0.4.1](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.4.0...0.4.1) (2026-08-01)


### Bug Fixes

* **dremio:** make setup_dremio.py idempotent across restarts ([867fbab](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/867fbabc8982e0e92dadfd0bfb4e48742efdad4c))


### Documentation

* auth-cookbook tutorial (all backends, 2 form screenshots) ([85b1978](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/85b1978b49a450d88f8f287a7e542d3c362519ee))
* caching-acceleration tutorial (Spice, the driver's original purpose) ([0b07721](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/0b077216eb537045e332e39e6972395383bc936b))
* csv-uploads tutorial (GizmoSQL, with screenshots) ([e6d607c](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/e6d607c8cf0ada11b4efdf45080726db154ab9c4))
* federation-semantic-layer tutorial (Dremio/Spice, with screenshot) ([3ed31e0](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/3ed31e07aac8b282196c8b9b57d544728a8716fb))
* local-embedded-analytics tutorial (GizmoSQL/DuckDB, with screenshot) ([132636d](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/132636d55a18491b5b9493e23d72ffe5c1960920))
* realtime-analytics tutorial — all 10 tutorials complete ([4b9f89b](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/4b9f89b62e3586dd1a4476eede20e24b1cc00672))
* timeseries tutorial (InfluxDB 3, with screenshot) ([6bcb63e](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/6bcb63e2fa2f6dc30a245814f576d85e5746c7a8))
* transformations-in-metabase tutorial (GizmoSQL, with screenshots) ([004a889](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/004a88999ec45f928881f3601d974aad48ad26c2))
* tutorial-driven docs — Iceberg exemplar (with screenshots) + backend refs + generator skill ([b8e81d3](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/b8e81d371f841408c2cf5c01f251b504c4a4e1c1))
* tutorial-driven docs — Iceberg exemplar + backend refs + generator skill ([ff23648](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/ff236482dee22584a202ad93cc9291e0cb25e26e))
* writeback-actions tutorial (Dremio, with screenshots) ([cd40113](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/cd4011322ae90613a30018986900422296570893))

## [0.4.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.3.0...0.4.0) (2026-07-30)


### Features

* Dremio backend — Apache Iceberg over Flight SQL + sync-query dialect fix ([c4b85e1](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/c4b85e16b86f0fc513cd7e9429df9e019b4b93bf))

## [0.3.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.2.0...0.3.0) (2026-07-29)


### Features

* Apache Doris test backend (MySQL-dialect Flight SQL) — Linux/CI profile ([854b101](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/854b101f0ab9ad79f5f489ee9542cc78b0491cd9))


### Bug Fixes

* **auth:** send user= for username with empty password; add StarRocks backend ([8b62add](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/8b62adde0abaab634a3ded9658d2fe93357e909b))


### CI

* e2e workflow that deploys Metabase + tests each backend on Linux ([40599cd](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/40599cdfe8e97773b2c6698b08020a215d08a513))
* fix e2e runner plumbing (bash for cert script, docker as container CLI) ([4627b28](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/4627b2859eb33892fa22e036935038d755c52033))
* make Doris/StarRocks opt-in; resilient seeds; document topology limit ([6ddac22](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/6ddac225f0431adc2d5b80ec8eec8fc911b2fb3e))
* remove temporary branch push trigger from e2e workflow ([e30d560](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/e30d5608618da89e08238ea3378f571bee132bd2))
* temporary branch push trigger to validate e2e workflow (remove before merge) ([aeb578f](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/aeb578f2ba0c322eea9608f2d0bbb151942b7c0d))

## [0.2.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/compare/0.1.0...0.2.0) (2026-07-26)


### Features

* Metabase 63 + Data Studio transforms + quack-on-demand backend + catalog/schema test matrix ([90b6e6b](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/90b6e6ba7efbd527a444bcff08025bb60c410526))


### Bug Fixes

* attach release jars from release-please (0.1.0 shipped empty) ([dac7099](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/dac7099c9dbe7ffc75f19ec89623a1462ecf8f32))
* attach release jars from the release-please workflow ([78d32a3](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/78d32a31c4a299a101440e8a979998612f2de122))


### Maintenance

* add quack-on-demand dashboard provisioning script; ignore local artifacts ([80cb0c0](https://github.com/J0hnG4lt/metabase-flightsql-driver/commit/80cb0c0968b58ac0a5c2f6d68406afd69ac2d4d6))

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
