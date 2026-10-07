# BODACC Links dbt Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dbt tests on `gold.bodacc_sirene_links` encoding its SCD2 invariants (exactly one current version per announcement, no duplicate versions, consistent `valid_to`/`is_current`), the same way Step 1's Plan 4 did for `gold.sirene_etablissements_historized`.

**Architecture:** The dbt project, `session`-mode Spark connection, and `dev`/`prod` targets already exist (Step 1 Plan 4) — nothing to scaffold. This plan only adds a second table entry to the existing `lakehouse` source, three new singular tests keyed on `bodacc_announcement_id` instead of `siret`, and extends the existing dev-warehouse seed script to also seed `gold.bodacc_sirene_links` with good + deliberately-violating rows.

**Tech Stack:** Same as Plan 4 — `dbt-core`, `dbt-spark[session]`, no new dependencies.

This is the dbt-tests sub-project identified after the BODACC matching orchestration plan shipped. See `docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md`'s "Testing" section ("dbt tests on `gold.bodacc_sirene_links` mirroring Plan 4's SCD2 invariant tests") and `docs/superpowers/plans/2026-10-06-gold-links-evaluation.md`'s "Out of scope" note (which deferred this here).

## Decisions made

- **Same invariants, same reasoning, different key column.** `bodacc_announcement_id` plays the role `siret` played for the SIRENE table — the three singular tests are structurally identical to Plan 4's, just re-keyed. No new invariant is needed: this table's SCD2 merge (`gold_links.py`) was built with the exact same two-statement MERGE+INSERT pattern, so the same three things can go wrong.
- **`match_confidence` is not a `not_null` candidate.** It's `NULL` by design for every `unresolved` row (see `resolve_unresolved_matches`), so testing it `not_null` would fail against correct data. `siret_siege` and `siren_bodacc` are similarly nullable by design (fuzzy matches have no `siren_bodacc`; unresolved rows have neither) and are left untested for the same reason. Only `bodacc_announcement_id`, `match_method`, `valid_from`, and `is_current` are always populated — mirroring the 4-test count Plan 4 used for the SIRENE table.
- **One unified seed script, not two.** `scripts/seed_dbt_dev_warehouse.py` already exists and owns the "spin up a local Iceberg session and seed known fixture data" responsibility; extending it to also seed `gold.bodacc_sirene_links` under the same `--with-violations` flag avoids duplicating the SparkSession/Iceberg boilerplate in a second script.
- **Test filenames are prefixed `assert_bodacc_links_...`** to avoid colliding with Plan 4's existing `assert_unique_siret_valid_from.sql` etc. (dbt test names are global, derived from the file name).
- **No macro to generalize the two tables' nearly-identical singular tests.** Plan 4 already decided against a `dbt-utils` dependency for a similar reason (three lines of plain SQL is cheaper than a package); the same reasoning extends to not building a bespoke macro for two call sites — revisit only if a third historized table needs the same tests.

## Architecture

```
dbt/models/sources.yml   -> add a second table under the existing `lakehouse` source:
                             bodacc_sirene_links

dbt/tests/
  assert_bodacc_links_unique_announcement_valid_from.sql
  assert_bodacc_links_one_current_version_per_announcement.sql
  assert_bodacc_links_valid_to_matches_is_current.sql

scripts/seed_dbt_dev_warehouse.py  -> extended to also seed gold.bodacc_sirene_links
```

---

### Task 1: Declare `bodacc_sirene_links` as a dbt source table

**Files:**
- Modify: `dbt/models/sources.yml`

- [ ] **Step 1: Add the new table entry**

Add to `dbt/models/sources.yml`, as a second entry in the existing `tables:` list:

```yaml
      - name: bodacc_sirene_links
        description: >
          SCD2-historized BODACC/SIRENE entity-resolution results (see
          docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md and
          docs/superpowers/plans/2026-10-06-gold-links-evaluation.md).
          `match_confidence` is deliberately excluded from SCD2 change-detection,
          so a probabilistic match score drifting slightly between re-runs never
          opens a spurious new version on its own.
        columns:
          - name: bodacc_announcement_id
            description: Identifier of the BODACC legal announcement being linked.
            tests:
              - not_null
          - name: match_method
            description: "One of: exact_siren, splink_fuzzy, unresolved."
            tests:
              - not_null
          - name: valid_from
            description: Date from which this version became valid.
            tests:
              - not_null
          - name: is_current
            description: True for the single currently-valid version of each announcement.
            tests:
              - not_null
```

- [ ] **Step 2: Reseed the dev warehouse and run the generic tests**

(Task 2 extends the seed script to actually populate this table — until then, running tests against it will fail with a table-not-found error, which is expected and not a bug to chase here.)

- [ ] **Step 3: Commit**

```bash
git add dbt/models/sources.yml
git commit -m "feat: declare gold.bodacc_sirene_links as a dbt source"
```

---

### Task 2: Seed the dev warehouse with BODACC links fixture data

**Files:**
- Modify: `scripts/seed_dbt_dev_warehouse.py`

- [ ] **Step 1: Add the new table's constants**

Add to `scripts/seed_dbt_dev_warehouse.py`, after the existing `VIOLATION_ROWS` list:

```python
BODACC_LINKS_TABLE = "lakehouse.gold.bodacc_sirene_links"

BODACC_LINKS_SCHEMA = StructType(
    [
        StructField("bodacc_announcement_id", StringType()),
        StructField("siren_bodacc", StringType()),
        StructField("siret_siege", StringType()),
        StructField("match_method", StringType()),
        StructField("match_confidence", DoubleType()),
        StructField("valid_from", DateType()),
        StructField("valid_to", DateType()),
        StructField("is_current", BooleanType()),
    ]
)

BODACC_LINKS_GOOD_ROWS = [
    ("A1", "552032534", "55203253400019", "exact_siren", 1.0, dt.date(2026, 10, 6), None, True),
    ("A2", None, "73282932000014", "splink_fuzzy", 0.87, dt.date(2026, 10, 6), None, True),
    ("A3", None, None, "unresolved", None, dt.date(2026, 10, 6), None, True),
]

# Each row below is a deliberate violation of exactly one of the three custom
# singular dbt tests for gold.bodacc_sirene_links, isolated the same way as
# VIOLATION_ROWS above.
BODACC_LINKS_VIOLATION_ROWS = [
    # Second is_current=true row for an existing bodacc_announcement_id (different
    # valid_from, so this does NOT also trip the uniqueness test) -> violates
    # assert_bodacc_links_one_current_version_per_announcement.
    ("A1", "552032534", "55203253400019", "exact_siren", 1.0, dt.date(2026, 10, 7), None, True),
    # Duplicate (bodacc_announcement_id, valid_from) for an existing id, closed
    # (is_current=False, valid_to set — so it does NOT also trip the other two
    # tests) -> violates assert_bodacc_links_unique_announcement_valid_from.
    (
        "A2",
        None,
        "73282932000014",
        "splink_fuzzy",
        0.87,
        dt.date(2026, 10, 6),
        dt.date(2026, 10, 7),
        False,
    ),
    # is_current=true but valid_to is also set, on a brand-new id (so it does NOT
    # also trip the other two tests) -> violates
    # assert_bodacc_links_valid_to_matches_is_current.
    ("A4", None, "99999999900001", "splink_fuzzy", 0.6, dt.date(2026, 10, 6), dt.date(2026, 10, 10), True),
]
```

Add `DoubleType` to the existing `pyspark.sql.types` import line, and add
`from registry.matching.gold_links import ensure_gold_links_table` to the imports.

- [ ] **Step 2: Extend `seed()` to also write the new table**

Replace the body of `seed()`:

```python
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
    spark.createDataFrame(rows, schema=GOLD_SCHEMA).writeTo(GOLD_TABLE).append()
    print(f"Seeded {len(rows)} rows into {GOLD_TABLE} (violations={with_violations})")

    spark.sql(f"DROP TABLE IF EXISTS {BODACC_LINKS_TABLE}")
    ensure_gold_links_table(spark, BODACC_LINKS_TABLE)
    links_rows = list(BODACC_LINKS_GOOD_ROWS) + (
        BODACC_LINKS_VIOLATION_ROWS if with_violations else []
    )
    spark.createDataFrame(links_rows, schema=BODACC_LINKS_SCHEMA).writeTo(
        BODACC_LINKS_TABLE
    ).append()
    print(
        f"Seeded {len(links_rows)} rows into {BODACC_LINKS_TABLE} "
        f"(violations={with_violations})"
    )

    spark.stop()
```

- [ ] **Step 3: Run it and verify both tables landed**

```bash
uv run python scripts/seed_dbt_dev_warehouse.py
```

Expected: prints `Seeded 2 rows into lakehouse.gold.sirene_etablissements_historized (violations=False)` then `Seeded 3 rows into lakehouse.gold.bodacc_sirene_links (violations=False)`.

- [ ] **Step 4: Run the generic `not_null` tests from Task 1 against the real seeded data**

```bash
set -a && source .env && set +a
cd dbt && uv run dbt test --profiles-dir . --target dev --select source:lakehouse.bodacc_sirene_links
```

Expected: `Completed successfully`, 4 tests passing (one `not_null` per listed column).

- [ ] **Step 5: Commit**

```bash
cd ..
git add scripts/seed_dbt_dev_warehouse.py
git commit -m "feat: seed gold.bodacc_sirene_links fixture data for dbt tests"
```

---

### Task 3: Singular test — unique `(bodacc_announcement_id, valid_from)`

**Files:**
- Create: `dbt/tests/assert_bodacc_links_unique_announcement_valid_from.sql`

- [ ] **Step 1: Create the test**

```sql
select bodacc_announcement_id, valid_from, count(*) as version_count
from {{ source('lakehouse', 'bodacc_sirene_links') }}
group by bodacc_announcement_id, valid_from
having count(*) > 1
```

- [ ] **Step 2: Run it against the (still clean) seeded data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev --select assert_bodacc_links_unique_announcement_valid_from
```

Expected: `PASS` (0 rows).

- [ ] **Step 3: Commit**

```bash
cd ..
git add dbt/tests/assert_bodacc_links_unique_announcement_valid_from.sql
git commit -m "test: add dbt test for unique (bodacc_announcement_id, valid_from)"
```

---

### Task 4: Singular test — exactly one current version per announcement

**Files:**
- Create: `dbt/tests/assert_bodacc_links_one_current_version_per_announcement.sql`

- [ ] **Step 1: Create the test**

```sql
select bodacc_announcement_id, count(*) as current_count
from {{ source('lakehouse', 'bodacc_sirene_links') }}
where is_current = true
group by bodacc_announcement_id
having count(*) != 1
```

- [ ] **Step 2: Run it against the seeded data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev --select assert_bodacc_links_one_current_version_per_announcement
```

Expected: `PASS` (0 rows).

- [ ] **Step 3: Commit**

```bash
cd ..
git add dbt/tests/assert_bodacc_links_one_current_version_per_announcement.sql
git commit -m "test: add dbt test for exactly one current version per announcement"
```

---

### Task 5: Singular test — `valid_to` matches `is_current`

**Files:**
- Create: `dbt/tests/assert_bodacc_links_valid_to_matches_is_current.sql`

- [ ] **Step 1: Create the test**

```sql
select bodacc_announcement_id, valid_from, valid_to, is_current
from {{ source('lakehouse', 'bodacc_sirene_links') }}
where
    (is_current = true and valid_to is not null)
    or (is_current = false and valid_to is null)
```

- [ ] **Step 2: Run it against the seeded data**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev --select assert_bodacc_links_valid_to_matches_is_current
```

Expected: `PASS` (0 rows).

- [ ] **Step 3: Run the full test suite once more**

```bash
uv run dbt test --profiles-dir . --target dev
```

Expected: `Completed successfully`, 11 tests passing (Plan 4's existing 7 + this plan's 4 new `bodacc_sirene_links` tests: 4 generic `not_null` is already counted within... — to be precise: 4 generic `not_null` for `sirene_etablissements_historized` + 3 singular for it + 4 generic `not_null` for `bodacc_sirene_links` + 3 singular for it = 14 total).

- [ ] **Step 4: Commit**

```bash
cd ..
git add dbt/tests/assert_bodacc_links_valid_to_matches_is_current.sql
git commit -m "test: add dbt test for valid_to/is_current consistency on bodacc links"
```

---

### Task 6: Prove the tests catch bad data

**Files:** none (verification only)

- [ ] **Step 1: Reseed the dev warehouse with the violation rows**

```bash
uv run python scripts/seed_dbt_dev_warehouse.py --with-violations
```

Expected: prints `Seeded 5 rows into lakehouse.gold.sirene_etablissements_historized (violations=True)` then `Seeded 6 rows into lakehouse.gold.bodacc_sirene_links (violations=True)`.

- [ ] **Step 2: Run the full test suite and confirm exactly the singular tests fail**

```bash
cd dbt && uv run dbt test --profiles-dir . --target dev
```

**Actual result (corrected from this plan's original prediction):** the `--with-violations` flag is shared by both tables in the unified seed script (Task 2's decision), so this reseeds violations into *both* `sirene_etablissements_historized` and `bodacc_sirene_links` at once — not just the new table. All 8 generic `not_null` tests (4 per table) still `PASS`, and all 6 singular tests `FAIL` (Plan 4's 3 pre-existing ones on `sirene_etablissements_historized`, plus this plan's 3 new ones): `PASS=8 ERROR=6 TOTAL=14`. For the 3 new ones specifically:
- `assert_bodacc_links_unique_announcement_valid_from` fails with 1 row (the duplicated `(A2, 2026-10-06)` pair).
- `assert_bodacc_links_one_current_version_per_announcement` fails with 1 row (`A1` now has 2 current versions).
- `assert_bodacc_links_valid_to_matches_is_current` fails with 1 row (`A4`, `is_current=true` with a non-null `valid_to`).

If any of these 6 unexpectedly passes, its SQL has a bug — fix it before continuing.

- [ ] **Step 3: Reseed clean data, leaving the repo in a good state**

```bash
cd ..
uv run python scripts/seed_dbt_dev_warehouse.py
cd dbt && uv run dbt test --profiles-dir . --target dev
```

Expected: back to `Completed successfully`, 14 passing, 0 failing.

---

### Task 7: Regenerate the docs site

**Files:** none (uses the descriptions already written into `sources.yml` in Task 1)

- [ ] **Step 1: Generate and spot-check the docs**

```bash
cd dbt && uv run dbt docs generate --profiles-dir . --target dev
uv run dbt docs serve --profiles-dir . --target dev --port 8081
```

Expected: starts a local server; open `http://localhost:8081` and confirm the `lakehouse.bodacc_sirene_links` source page shows the table description, the 4 documented columns, and all 7 of its tests listed (4 generic + 3 singular). Stop the server with Ctrl-C when done.

---

### Task 8: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full pytest suite and lint**

```bash
cd ..
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Expected: all 75 pytest tests still pass (this plan adds no new pytest tests — dbt has its own test runner, exercised in Tasks 3-6), and lint/format are clean.

---

### Task 9: Manual verification against the real Garage lakehouse (prod target) — deferred

**Files:** none (deferred; same reasoning as every prior manual-verification task in this project)

This exercises the `prod` target against the real `gold.bodacc_sirene_links` table, which only gets populated once the BODACC matching orchestration DAG (previous plan) has actually run — still blocked on `SIRENE_API_KEY` and `SIRENE_CANDIDATES_BRONZE_KEY`.

- [ ] **Step 1:** Once the DAG has run at least once against real data, run:

```bash
docker compose up -d garage
set -a && source .env && set +a
cd dbt && uv run dbt test --profiles-dir . --target prod
```

Expected: `Completed successfully` — confirming the real gold table satisfies the SCD2 invariants these tests encode, not just the synthetic dev fixture.

---

## Self-review notes

- **Spec coverage:** the spec's "dbt tests on `gold.bodacc_sirene_links` mirroring Plan 4's SCD2 invariant tests" is fully covered — same three invariants, re-keyed on `bodacc_announcement_id`.
- **Placeholder scan:** none found.
- **Type consistency:** `BODACC_LINKS_SCHEMA`'s column order and types match `ensure_gold_links_table`'s DDL exactly (`gold_links.py`). Test file names match the `--select` flags used in Tasks 3-6 exactly.
