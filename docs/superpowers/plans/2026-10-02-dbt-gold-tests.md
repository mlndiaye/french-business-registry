# dbt Gold Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dbt tests and documentation on top of the gold Iceberg table built in
Plan 2 — data-quality tests that encode the SCD2 invariants (exactly one current
version per establishment, no duplicate versions, consistent `valid_to`/`is_current`)
so a regression in the merge logic gets caught, plus a browsable docs site.

**Architecture:** The gold table is built by our own PySpark `MERGE INTO` logic
(Plan 2), not by dbt — so dbt's role here is testing and documenting an *existing*
table, not building one. It is declared as a dbt **source** (not a model), with
generic column tests plus three custom singular tests encoding the SCD2 invariants.
dbt connects via `dbt-spark`'s **session** method (an embedded PySpark session
inside the dbt process itself) — there is no persistent Spark Thrift server to run.
Two targets: `dev` (a local, file-backed Iceberg catalog seeded with known
good/bad fixture rows, for fast deterministic test verification) and `prod` (the
real Garage-backed lakehouse built by Plans 1-3).

**Tech Stack:** `dbt-core` 1.12.x, `dbt-spark[session]` 1.11.x (verified to resolve
cleanly alongside the pinned `pyspark==3.5.3` before writing this plan).

This is Plan 4 of 5 for Step 1 (SIRENE lakehouse pipeline) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` for the full design
and Plan 2 (`2026-10-01-spark-bronze-silver-gold.md`) for the gold table's schema
(`ensure_gold_table`) this plan tests. Plan 5 (Postgres sync + FastAPI serving) is
out of scope here.

## Decisions made

- **dbt tests an existing table (a *source*), it does not build one.** The gold
  table's construction (the SCD2 `MERGE INTO`) is already Plan 2's PySpark code;
  duplicating that logic as a dbt model would create two systems writing the same
  table. dbt's job is purely data-quality assertions and documentation.
- **Connection method: `session`**, not `thrift`/`http`/`odbc`. Those require a
  running Spark Thrift server or Databricks endpoint; `session` embeds a PySpark
  session directly inside the `dbt` process — the same shape as every other
  PySpark entry point in this project (Plan 2/3), with no new long-running service.
- **No `dbt-utils` dependency.** The one thing it would buy here is a generic
  composite-uniqueness test; writing that as a plain singular SQL test is three
  lines and avoids a `packages.yml` + `dbt deps` step for a single test.
- **No dbt source freshness check.** Freshness tests compare a `loaded_at` column
  against "now," which assumes most rows get touched on every successful run. This
  table is the opposite: on a day where nothing changed, every row keeps its old
  `valid_from` by design (see Plan 2's SCD2 decisions) — a freshness check would
  flag a perfectly healthy run as stale. Freshness belongs at the bronze landing
  layer (where a true ingestion timestamp exists), not here.
- **The `dev` target points at a local, file-backed Iceberg warehouse
  (`.dbt_dev_warehouse/`, gitignored)**, seeded by a small script
  (`scripts/seed_dbt_dev_warehouse.py`) with the same two establishments used
  throughout Plans 1-3, plus an optional `--with-violations` flag that adds three
  rows, each violating exactly one of the three custom tests. This is this plan's
  equivalent of Plan 2's local-catalog pytest fixtures: fast, deterministic,
  no network — and, critically, a way to prove the tests actually fail on bad data
  (Task 7), not just pass on good data.
- **Sources use an explicit `database: lakehouse` key** in `sources.yml`, rather
  than relying on `dbt-spark`'s profile-level `catalog:` field (added primarily for
  Databricks Unity Catalog). `database` is a universal dbt concept every adapter
  resolves into the generated SQL's qualified table name
  (`lakehouse.gold.sirene_etablissements_historized`), which is lower-risk than
  depending on an adapter-specific, less-universally-documented option.
- **Two unverified assumptions, flagged for Task 1 to confirm quickly rather than
  guess blindly:** (1) that `dbt-spark`'s session connector correctly threads
  `server_side_parameters` into the embedded `SparkSession.builder.config(...)`
  calls (this is the documented pattern for local/dev use of `dbt-spark`, but this
  environment has not exercised it before); (2) that the resulting session
  correctly resolves 3-part `catalog.schema.table` identifiers from the
  `database`/`schema` source keys against our named `lakehouse` Iceberg catalog.
  Task 1's `dbt debug` + a trivial query are the cheap way to find out before
  building the rest of the plan on top of it.

## Architecture

```
dbt/
├── dbt_project.yml
├── profiles.yml          (targets: dev = local Iceberg catalog, prod = Garage)
└── models/
    └── sources.yml        -> declares lakehouse.gold.sirene_etablissements_historized
    └── ... (tests/ directory holds the 3 singular test .sql files)

scripts/seed_dbt_dev_warehouse.py   -> seeds .dbt_dev_warehouse/ for the dev target

dev target:  dbt test  -->  embedded PySpark session  -->  .dbt_dev_warehouse/ (local disk)
prod target: dbt test  -->  embedded PySpark session  -->  s3a://lakehouse/... (Garage)
```

---

### Task 1: dbt project scaffold + connectivity smoke check

**Files:**
- Modify: `pyproject.toml`
- Modify: `.gitignore`
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`

- [ ] **Step 1: Add `dbt-spark` to dependencies**

Edit `pyproject.toml`'s `dependencies` list:

```toml
dependencies = [
    "boto3>=1.34",
    "pyarrow>=16.0",
    "requests>=2.31",
    "pyspark==3.5.3",
    "dbt-spark[session]>=1.11,<1.12",
]
```

Run: `uv sync --all-groups`
Expected: resolves and installs `dbt-core`, `dbt-spark`, and their dependencies
(already verified to resolve cleanly alongside `pyspark==3.5.3` before writing this
plan). Exits 0.

- [ ] **Step 2: Create `dbt/dbt_project.yml`**

```yaml
name: french_business_registry
version: "1.0.0"
config-version: 2

profile: french_business_registry

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "logs"
```

- [ ] **Step 3: Create `dbt/profiles.yml`**

```yaml
french_business_registry:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      host: NA
      schema: default
      server_side_parameters:
        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog"
        "spark.sql.catalog.lakehouse.type": "hadoop"
        "spark.sql.catalog.lakehouse.warehouse": "{{ env_var('DBT_DEV_WAREHOUSE', '.dbt_dev_warehouse') }}"
    prod:
      type: spark
      method: session
      host: NA
      schema: default
      server_side_parameters:
        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog"
        "spark.sql.catalog.lakehouse.type": "hadoop"
        "spark.sql.catalog.lakehouse.warehouse": "s3a://{{ env_var('LAKEHOUSE_BUCKET') }}/warehouse"
        "spark.hadoop.fs.s3a.endpoint": "{{ env_var('S3_ENDPOINT_URL') }}"
        "spark.hadoop.fs.s3a.endpoint.region": "{{ env_var('S3_REGION') }}"
        "spark.hadoop.fs.s3a.access.key": "{{ env_var('S3_ACCESS_KEY') }}"
        "spark.hadoop.fs.s3a.secret.key": "{{ env_var('S3_SECRET_KEY') }}"
        "spark.hadoop.fs.s3a.path.style.access": "true"
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false"
```

The `dev` target needs no env vars set (its `env_var()` call has a default). The
`prod` target's `env_var()` calls have no defaults — they will raise a clear error
if `.env` hasn't been sourced, which is the desired behavior.

- [ ] **Step 4: Add dbt artifacts to `.gitignore`**

Append to `.gitignore`:

```
# dbt
.dbt_dev_warehouse/
dbt/target/
dbt/logs/
```

- [ ] **Step 5: Smoke-check the connection**

```bash
cd dbt && uv run dbt debug --profiles-dir . --target dev
```

Expected: `All checks passed!` (or equivalent "Connection test: OK connection ok").
This is the cheap check for the two assumptions flagged in "Decisions made" above —
if `dbt-spark`'s session method doesn't apply `server_side_parameters` the way
expected, or the Iceberg catalog config is rejected, it surfaces here, before any
tests are written on top of it. If it fails, stop and investigate rather than
building further tasks on an unconfirmed connection.

- [ ] **Step 6: Commit**

```bash
cd ..
git add pyproject.toml uv.lock .gitignore dbt/dbt_project.yml dbt/profiles.yml
git commit -m "feat: scaffold dbt project with session-mode Spark connection"
```

---

### Task 2: Seed the local dev warehouse

**Files:**
- Create: `scripts/seed_dbt_dev_warehouse.py`

- [ ] **Step 1: Create `scripts/seed_dbt_dev_warehouse.py`**

```python
"""Seed a local Iceberg warehouse with known SIRENE gold data, for fast dbt test
verification against the `dev` target (see dbt/profiles.yml)."""

from __future__ import annotations

import argparse
import datetime as dt

from pyspark.sql import SparkSession

from registry.transform.silver_to_gold import ensure_gold_table

DEV_WAREHOUSE_DIR = ".dbt_dev_warehouse"
GOLD_TABLE = "lakehouse.gold.sirene_etablissements_historized"

GOLD_COLUMNS = [
    "siren",
    "nic",
    "siret",
    "statut_diffusion",
    "date_creation",
    "etablissement_siege",
    "numero_voie",
    "type_voie",
    "libelle_voie",
    "code_postal",
    "libelle_commune",
    "activite_principale",
    "etat_administratif",
    "date_dernier_traitement",
    "valid_from",
    "valid_to",
    "is_current",
]

GOOD_ROWS = [
    (
        "552032534", "00019", "55203253400019", "O", dt.date(1966, 1, 1), True,
        "8", "RUE", "DE LA PAIX", "75002", "PARIS", "70.10Z", "A", dt.date(2023, 5, 12),
        dt.date(2026, 10, 1), None, True,
    ),
    (
        "732829320", "00014", "73282932000014", "O", dt.date(1994, 3, 15), False,
        "12", "AV", "DES CHAMPS ELYSEES", "75008", "PARIS", "46.19B", "A", dt.date(2022, 11, 3),
        dt.date(2026, 10, 1), None, True,
    ),
]

# Each row below is a deliberate violation of exactly one of the three custom
# singular dbt tests (Tasks 4-6), isolated so each test's failure can be verified
# independently in Task 7.
VIOLATION_ROWS = [
    # Second is_current=true row for an existing siret -> violates
    # assert_exactly_one_current_version_per_siret.
    (
        "552032534", "00019", "55203253400019", "O", dt.date(1966, 1, 1), True,
        "99", "RUE", "DE LA PAIX", "75002", "PARIS", "70.10Z", "A", dt.date(2026, 9, 30),
        dt.date(2026, 10, 2), None, True,
    ),
    # Duplicate (siret, valid_from) for an existing siret -> violates
    # assert_unique_siret_valid_from.
    (
        "732829320", "00014", "73282932000014", "O", dt.date(1994, 3, 15), False,
        "12", "AV", "DES CHAMPS ELYSEES", "75008", "PARIS", "46.19B", "A", dt.date(2022, 11, 3),
        dt.date(2026, 10, 1), dt.date(2026, 10, 2), False,
    ),
    # is_current=true but valid_to is also set -> violates
    # assert_valid_to_matches_is_current.
    (
        "999999999", "00001", "99999999900001", "O", dt.date(2020, 1, 1), True,
        "1", "RUE", "DU TEST", "75001", "PARIS", "62.01Z", "A", dt.date(2026, 10, 1),
        dt.date(2026, 10, 1), dt.date(2026, 10, 5), True,
    ),
]


def seed(with_violations: bool) -> None:
    spark = (
        SparkSession.builder.appName("dbt-dev-seed")
        .master("local[1]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", DEV_WAREHOUSE_DIR)
        .getOrCreate()
    )

    spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")
    ensure_gold_table(spark, GOLD_TABLE)

    rows = list(GOOD_ROWS) + (VIOLATION_ROWS if with_violations else [])
    spark.createDataFrame(rows, schema=GOLD_COLUMNS).writeTo(GOLD_TABLE).append()

    print(f"Seeded {len(rows)} rows into {GOLD_TABLE} (violations={with_violations})")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-violations", action="store_true")
    args = parser.parse_args()
    seed(args.with_violations)
```

- [ ] **Step 2: Run it and verify the data landed**

```bash
uv run python scripts/seed_dbt_dev_warehouse.py
uv run python -c "
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName('check')
    .master('local[1]')
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type', 'hadoop')
    .config('spark.sql.catalog.lakehouse.warehouse', '.dbt_dev_warehouse')
    .getOrCreate()
)
spark.table('lakehouse.gold.sirene_etablissements_historized').show(truncate=False)
spark.stop()
"
```

Expected: prints `Seeded 2 rows ... (violations=False)`, then a table with the 2
établissements, both `is_current=true`.

- [ ] **Step 3: Commit**

```bash
git add scripts/seed_dbt_dev_warehouse.py
git commit -m "feat: add dev warehouse seed script for dbt test verification"
```

---

### Task 3: Source definition with generic tests

**Files:**
- Create: `dbt/models/sources.yml`

- [ ] **Step 1: Create `dbt/models/sources.yml`**

```yaml
version: 2

sources:
  - name: lakehouse
    description: >
      Iceberg lakehouse tables produced by the PySpark bronze-to-gold pipeline
      (see docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md).
    database: lakehouse
    schema: gold
    tables:
      - name: sirene_etablissements_historized
        description: >
          SCD2-historized SIRENE establishment registry. Any change on any
          tracked column opens a new version; `is_current` marks the single
          currently-valid version per `siret`.
        columns:
          - name: siren
            description: 9-digit company identifier.
            tests:
              - not_null
          - name: siret
            description: 14-digit establishment identifier (siren + nic).
            tests:
              - not_null
          - name: valid_from
            description: Date from which this version became valid.
            tests:
              - not_null
          - name: is_current
            description: True for the single currently-valid version of each establishment.
            tests:
              - not_null
```

- [ ] **Step 2: Run the generic tests against the seeded good data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev
```

Expected: `Completed successfully` with 4 tests run (one `not_null` per listed
column), all passing.

- [ ] **Step 3: Commit**

```bash
cd ..
git add dbt/models/sources.yml
git commit -m "feat: declare the gold table as a dbt source with not_null tests"
```

---

### Task 4: Singular test — unique `(siret, valid_from)`

**Files:**
- Create: `dbt/tests/assert_unique_siret_valid_from.sql`

- [ ] **Step 1: Create the test**

```sql
select siret, valid_from, count(*) as version_count
from {{ source('lakehouse', 'sirene_etablissements_historized') }}
group by siret, valid_from
having count(*) > 1
```

- [ ] **Step 2: Run it against the (still clean) seeded data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev --select assert_unique_siret_valid_from
```

Expected: `PASS` (0 rows returned — the seeded good data has no duplicate
`(siret, valid_from)` pairs).

- [ ] **Step 3: Commit**

```bash
cd ..
git add dbt/tests/assert_unique_siret_valid_from.sql
git commit -m "test: add dbt test for unique (siret, valid_from) per gold row"
```

---

### Task 5: Singular test — exactly one current version per establishment

**Files:**
- Create: `dbt/tests/assert_exactly_one_current_version_per_siret.sql`

- [ ] **Step 1: Create the test**

```sql
select siret, count(*) as current_count
from {{ source('lakehouse', 'sirene_etablissements_historized') }}
where is_current = true
group by siret
having count(*) != 1
```

- [ ] **Step 2: Run it against the seeded data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev --select assert_exactly_one_current_version_per_siret
```

Expected: `PASS` (0 rows — every establishment in the good seed has exactly one
current version).

- [ ] **Step 3: Commit**

```bash
cd ..
git add dbt/tests/assert_exactly_one_current_version_per_siret.sql
git commit -m "test: add dbt test for exactly one current version per establishment"
```

---

### Task 6: Singular test — `valid_to` matches `is_current`

**Files:**
- Create: `dbt/tests/assert_valid_to_matches_is_current.sql`

- [ ] **Step 1: Create the test**

```sql
select siret, valid_from, valid_to, is_current
from {{ source('lakehouse', 'sirene_etablissements_historized') }}
where
    (is_current = true and valid_to is not null)
    or (is_current = false and valid_to is null)
```

- [ ] **Step 2: Run it against the seeded data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev --select assert_valid_to_matches_is_current
```

Expected: `PASS` (0 rows).

- [ ] **Step 3: Run the full test suite once more**

```bash
uv run dbt test --profiles-dir . --target dev
```

Expected: `Completed successfully`, 7 tests passing (4 generic `not_null` + 3
singular).

- [ ] **Step 4: Commit**

```bash
cd ..
git add dbt/tests/assert_valid_to_matches_is_current.sql
git commit -m "test: add dbt test for valid_to/is_current consistency"
```

---

### Task 7: Prove the tests catch bad data

**Files:** none (verification only — the seed script and test files already exist)

This is the equivalent of the "red" step the other plans got via genuine TDD: dbt
tests can't meaningfully fail-then-pass against synthetic good data the way a
`pytest` test can, so this task proves the three custom tests actually have teeth by
feeding them data each one is specifically designed to reject.

- [ ] **Step 1: Reseed the dev warehouse with the violation rows**

```bash
uv run python scripts/seed_dbt_dev_warehouse.py --with-violations
```

Expected: prints `Seeded 5 rows ... (violations=True)`.

- [ ] **Step 2: Run the full test suite and confirm exactly the 3 custom tests fail**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev
```

Expected: the 4 generic `not_null` tests still `PASS` (the violation rows have no
null values in those columns), and all 3 singular tests `FAIL`:
- `assert_unique_siret_valid_from` fails with 1 row (the duplicated
  `(73282932000014, 2026-10-01)` pair).
- `assert_exactly_one_current_version_per_siret` fails with 1 row
  (`55203253400019` now has 2 current versions).
- `assert_valid_to_matches_is_current` fails with 1 row (`99999999900001`, which
  has both `is_current=true` and a non-null `valid_to`).

If any of these 3 tests unexpectedly passes, its SQL has a bug — fix it before
continuing; a test that can't fail is worse than no test.

- [ ] **Step 3: Reseed clean data, leaving the repo in a good state**

```bash
cd ..
uv run python scripts/seed_dbt_dev_warehouse.py
cd dbt && uv run dbt test --profiles-dir . --target dev
```

Expected: back to `Completed successfully`, 7 passing, 0 failing.

---

### Task 8: Documentation site

**Files:** none (uses the descriptions already written into `sources.yml` in Task 3)

- [ ] **Step 1: Generate the docs**

```bash
cd dbt && uv run dbt docs generate --profiles-dir . --target dev
```

Expected: completes successfully, writes `dbt/target/catalog.json` and
`dbt/target/manifest.json`.

- [ ] **Step 2: Serve and spot-check it**

```bash
uv run dbt docs serve --profiles-dir . --target dev --port 8081
```

Expected: starts a local server; open `http://localhost:8081` in a browser and
confirm the `lakehouse.sirene_etablissements_historized` source page shows the
table description, the 4 documented columns, and all 7 tests listed. Stop the
server with Ctrl-C when done.

---

### Task 9: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite and lint**

```bash
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Expected: all 23 pytest tests still pass (this plan added no new pytest tests —
dbt has its own test runner, exercised in Tasks 3-7), and lint/format are clean.
`scripts/seed_dbt_dev_warehouse.py` is a plain Python file under the repo root, so
it is covered by the existing ruff config without any changes needed.

---

### Task 10: Manual verification against the real Garage lakehouse (prod target)

**Files:** none (manual verification only)

This exercises the `prod` target against the actual gold table built by Plans 1-3,
which should already contain data from the manual verification steps in those
plans.

- [ ] **Step 1: Ensure Garage is running and `.env` is sourced**

```bash
docker compose up -d garage
set -a && source .env && set +a
```

- [ ] **Step 2: Smoke-check the prod connection**

```bash
cd dbt && uv run dbt debug --profiles-dir . --target prod
```

Expected: `All checks passed!`.

- [ ] **Step 3: Run the tests against the real gold table**

```bash
uv run dbt test --profiles-dir . --target prod
```

Expected: `Completed successfully`, 7 tests passing — confirming the real gold
table built by Plans 1-3 actually satisfies the SCD2 invariants these tests
encode, not just the synthetic dev fixture.

- [ ] **Step 4: Generate and spot-check the docs against the real table**

```bash
uv run dbt docs generate --profiles-dir . --target prod
uv run dbt docs serve --profiles-dir . --target prod --port 8081
```

Expected: same as Task 8, but the catalog metadata (row counts, column stats if
shown) now reflects the real Garage-backed table.
