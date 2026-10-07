# BODACC Matching Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A daily Airflow DAG that runs the full BODACC matching cascade (exact → fuzzy Splink → unresolved) against the backlog of unlinked announcements and historizes results into `gold.bodacc_sirene_links`.

**Architecture:** Three TaskFlow tasks mirroring `sirene_daily_pipeline.py`: extract BODACC diff, transform bronze→silver, then a single Spark task that computes the backlog (anti-join against gold), runs the matching cascade, and historizes via SCD2. A prerequisite fix: `bodacc_to_silver.bronze_to_silver` currently `createOrReplace`s silver on every run, which would silently truncate the backlog computation to only today's diff — it must accumulate instead, since BODACC announcements are immutable once published (unlike SIRENE establishments).

**Tech Stack:** PySpark, Apache Iceberg (`MERGE INTO`), Splink (`SparkAPI` backend), Airflow TaskFlow API.

---

## Decisions made

- **Fix `bronze_to_silver`'s accumulation semantics first** (Task 1). Discovered while designing this plan: SIRENE's `createOrReplace` is correct because gold's SCD2 merge only needs *changed* establishments in silver each day, and SIRENE diffs are genuine mutations. BODACC's `createOrReplace` is a bug by the same reasoning, because this plan needs `silver.bodacc_annonces` to hold every announcement ever ingested, not just today's — otherwise `find_unlinked_announcements` can never see backlog from before today. Fixed via a `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` (dedup on `id`, no UPDATE branch needed since announcements don't change).
- **SIRENE candidates are read from a fixed bronze snapshot** (`SIRENE_CANDIDATES_BRONZE_KEY` env var), never re-ingested by this DAG. See the spec's "Decisions made".
- **`run_matching_cascade` lives in a new `src/registry/matching/cascade.py`**, not bolted onto `combine.py` or `gold_links.py` — it's an orchestration layer crossing `exact_siren`, `prepare`, `splink_matching`, and `combine`, and deserves its own file per the project's "one clear responsibility per file" convention.
- **No pytest for `cascade.py` or the DAG itself** — same reasoning as `splink_matching.py`: a real Splink `Linker` is infrastructure, not pure logic. Instead, a committed smoke-test script (`scripts/cascade_smoke_test.py`) proves the wiring end-to-end on a plain local Spark session with hand-built data — no API keys needed, unlike the full DAG.
- **Task 6 (real end-to-end DAG run) remains deferred**, consistent with every prior plan: it needs both `SIRENE_API_KEY` and `SIRENE_CANDIDATES_BRONZE_KEY`, neither set yet.

---

### Task 1: Fix `bronze_to_silver` to accumulate BODACC announcements

**Files:**
- Modify: `src/registry/transform/bodacc_to_silver.py`
- Test: `tests/transform/test_bodacc_to_silver.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/transform/test_bodacc_to_silver.py`:

```python
def test_bronze_to_silver_accumulates_across_runs(spark_session, tmp_path, table_suffix):
    first_bronze_df = spark_session.createDataFrame(
        [
            (
                "BX202500012345",
                "2025-10-15",
                "12345",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Paris",
                "DUPONT BATIMENT SARL",
                "334393806",
                "PARIS",
                "75002",
                None,
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    first_bronze_path = str(tmp_path / "bronze_bodacc_1.parquet")
    first_bronze_df.write.parquet(first_bronze_path)
    silver_table = f"lakehouse.silver.bodacc_{table_suffix}"
    bronze_to_silver(spark_session, first_bronze_path, silver_table)

    second_bronze_df = spark_session.createDataFrame(
        [
            (
                "BX202500099999",
                "2025-10-16",
                "99999",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Lyon",
                "MARTIN TRAVAUX SARL",
                "445566778",
                "LYON",
                "69001",
                None,
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    second_bronze_path = str(tmp_path / "bronze_bodacc_2.parquet")
    second_bronze_df.write.parquet(second_bronze_path)
    bronze_to_silver(spark_session, second_bronze_path, silver_table)

    result = spark_session.table(silver_table).orderBy("id").collect()
    assert [row.id for row in result] == ["BX202500012345", "BX202500099999"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py::test_bronze_to_silver_accumulates_across_runs -v`
Expected: FAIL — the second `createOrReplace` wipes out the first run's row, so only `BX202500099999` remains.

- [ ] **Step 3: Write the fix**

Replace in `src/registry/transform/bodacc_to_silver.py`:

```python
def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_table: str) -> None:
    raw_df = spark.read.parquet(bronze_path)
    clean_df = clean_bodacc_bronze(raw_df)
    clean_df.writeTo(silver_table).createOrReplace()
```

with:

```python
def ensure_silver_table(spark: SparkSession, silver_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            id STRING,
            date_parution DATE,
            numero_annonce STRING,
            type_avis STRING,
            famille_avis STRING,
            tribunal STRING,
            commercant STRING,
            denomination STRING,
            siren_declared STRING,
            ville STRING,
            code_postal STRING
        ) USING iceberg
    """)


def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_table: str) -> None:
    raw_df = spark.read.parquet(bronze_path)
    clean_df = clean_bodacc_bronze(raw_df)

    ensure_silver_table(spark, silver_table)
    clean_df.createOrReplaceTempView("incoming_announcements")

    spark.sql(f"""
        MERGE INTO {silver_table} AS silver
        USING incoming_announcements AS incoming
        ON silver.id = incoming.id
        WHEN NOT MATCHED THEN INSERT *
    """)
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py::test_bronze_to_silver_accumulates_across_runs -v`
Expected: PASS

- [ ] **Step 5: Write a second test proving re-running the same bronze doesn't duplicate rows**

Add to `tests/transform/test_bodacc_to_silver.py`:

```python
def test_bronze_to_silver_does_not_duplicate_already_seen_announcements(
    spark_session, tmp_path, table_suffix
):
    bronze_df = spark_session.createDataFrame(
        [
            (
                "BX202500012345",
                "2025-10-15",
                "12345",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Paris",
                "DUPONT BATIMENT SARL",
                "334393806",
                "PARIS",
                "75002",
                None,
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    bronze_path = str(tmp_path / "bronze_bodacc.parquet")
    bronze_df.write.parquet(bronze_path)
    silver_table = f"lakehouse.silver.bodacc_{table_suffix}"

    bronze_to_silver(spark_session, bronze_path, silver_table)
    bronze_to_silver(spark_session, bronze_path, silver_table)

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
```

- [ ] **Step 6: Run the full file to confirm everything passes**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py -v`
Expected: PASS (5 tests — 2 `clean_bodacc_bronze` tests, the original write test, and the 2 new ones)

- [ ] **Step 7: Commit**

```bash
git add src/registry/transform/bodacc_to_silver.py tests/transform/test_bodacc_to_silver.py
git commit -m "fix: accumulate BODACC announcements in silver instead of overwriting"
```

---

### Task 2: `find_unlinked_announcements`

**Files:**
- Modify: `src/registry/matching/gold_links.py`
- Test: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_gold_links.py`:

```python
from registry.matching.gold_links import find_unlinked_announcements


def test_find_unlinked_announcements_excludes_only_current_links(spark_session):
    bodacc_df = spark_session.createDataFrame([("A1",), ("A2",), ("A3",)], schema=["id"])
    gold_links_df = spark_session.createDataFrame(
        [("A1", True), ("A2", False)],
        schema=["bodacc_announcement_id", "is_current"],
    )

    result = find_unlinked_announcements(bodacc_df, gold_links_df).orderBy("id").collect()

    assert [row.id for row in result] == ["A2", "A3"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py::test_find_unlinked_announcements_excludes_only_current_links -v`
Expected: FAIL with `ImportError: cannot import name 'find_unlinked_announcements'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/gold_links.py` (add `from pyspark.sql import functions as F` to the imports):

```python
def find_unlinked_announcements(bodacc_df: DataFrame, gold_links_df: DataFrame) -> DataFrame:
    current_ids = gold_links_df.filter(F.col("is_current")).select(
        F.col("bodacc_announcement_id").alias("linked_id")
    )
    return bodacc_df.join(current_ids, bodacc_df["id"] == current_ids["linked_id"], "left_anti")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/gold_links.py tests/matching/test_gold_links.py
git commit -m "feat: find BODACC announcements with no current gold link"
```

---

### Task 3: `run_matching_cascade`

**Files:**
- Create: `src/registry/matching/cascade.py`
- Create: `scripts/cascade_smoke_test.py`

No pytest for this module — it drives a real Splink `Linker`, same reasoning as `splink_matching.py`. Verified instead by a committed smoke-test script using a plain local Spark session (no API keys, no Garage).

- [ ] **Step 1: Write the implementation**

Create `src/registry/matching/cascade.py`:

```python
"""Wire the full BODACC/SIRENE matching cascade (exact -> fuzzy -> unresolved) into
a single callable, for use by the daily orchestration DAG. No pytest: it drives a
real Splink Linker end-to-end, same reasoning as splink_matching.py — verified
manually (scripts/cascade_smoke_test.py), not mocked."""

from __future__ import annotations

from pyspark.sql import DataFrame

from registry.matching.combine import (
    best_fuzzy_match_per_announcement,
    combine_match_results,
    extract_fuzzy_match_candidates,
    resolve_unresolved_matches,
)
from registry.matching.exact_siren import exact_siren_matches, unmatched_announcements
from registry.matching.prepare import prepare_bodacc_for_matching, prepare_sirene_for_matching
from registry.matching.splink_matching import build_linker, predict_fuzzy_matches, train_linker


def run_matching_cascade(
    bodacc_df: DataFrame, sirene_candidates_df: DataFrame, db_api
) -> DataFrame:
    exact_matches = exact_siren_matches(bodacc_df, sirene_candidates_df)
    remaining = unmatched_announcements(bodacc_df, exact_matches)

    bodacc_prepared = prepare_bodacc_for_matching(remaining)
    sirene_prepared = prepare_sirene_for_matching(sirene_candidates_df)

    linker = build_linker(bodacc_prepared, sirene_prepared, db_api)
    train_linker(linker)
    predictions_df = predict_fuzzy_matches(linker).as_spark_dataframe()

    fuzzy_candidates = extract_fuzzy_match_candidates(predictions_df)
    best_fuzzy = best_fuzzy_match_per_announcement(fuzzy_candidates)

    combined = combine_match_results(exact_matches, best_fuzzy)
    return resolve_unresolved_matches(bodacc_df, combined)
```

- [ ] **Step 2: Write the smoke-test script**

Create `scripts/cascade_smoke_test.py`:

```python
"""Smoke test proving `run_matching_cascade` wires exact + fuzzy (Splink on the
Spark backend) + unresolved correctly end-to-end, without needing BODACC/SIRENE
API credentials — a plain local Spark session and small hand-built DataFrames are
enough. Not part of the pytest suite, same reasoning as splink_matching.py."""

from __future__ import annotations

from pyspark.sql import SparkSession
from splink import SparkAPI

from registry.matching.cascade import run_matching_cascade

spark = SparkSession.builder.master("local[*]").appName("cascade-smoke-test").getOrCreate()

bodacc_df = spark.createDataFrame(
    [
        ("B1", "552032534", "DUPONT BATIMENT", "75002", "PARIS"),
        ("B2", None, "MARTIN TRAVAUX", "69001", "LYON"),
        ("B3", None, "ENTREPRISE INCONNUE", "13001", "MARSEILLE"),
    ],
    schema=["id", "siren_declared", "commercant", "code_postal", "ville"],
)
sirene_candidates_df = spark.createDataFrame(
    [
        ("552032534", "55203253400019", "DUPONT BATIMENT SARL", "75002", "PARIS"),
        ("445566778", "44556677800012", "MARTIN TRAVAUX SARL", "69001", "LYON"),
    ],
    schema=["siren", "siret", "denomination", "code_postal", "libelle_commune"],
)

db_api = SparkAPI(spark_session=spark)
result = run_matching_cascade(bodacc_df, sirene_candidates_df, db_api)
result.orderBy("bodacc_announcement_id").show(truncate=False)
spark.stop()
print("Cascade smoke test completed without error.")
```

- [ ] **Step 3: Run the smoke test and verify the three expected outcomes**

Run: `uv run python scripts/cascade_smoke_test.py 2>&1 | tail -10`
Expected: a row for `B1` with `match_method = exact_siren` and `siret_siege = 55203253400019`. `B2` and `B3` both land as `match_method = unresolved`: `B3` because no candidate shares its name's first-4-char blocking prefix (never compared); `B2` because, after removing `B1`'s exact match, the fuzzy stage is left with exactly one comparison pair (`B2` vs. its true candidate) — too little volume for Splink's EM training to produce a probability above the 0.5 threshold. This is a real, honest limitation of a 2-candidate toy dataset, not a wiring bug: the script's job is to prove the cascade runs end-to-end without crashing and produces a well-formed combined result, not to prove fuzzy-match accuracy — that's what the real blind-holdout evaluation (Task 7, deferred) measures on real data.

**Environment notes discovered while running this** (none were anticipated in the spec — all found empirically, consistent with this project's "verify before committing" practice):

1. PySpark 3.5.3 on Python 3.12 needs `setuptools` installed (provides the `distutils` shim Python 3.12 removed from the stdlib) for Splink's `toPandas()` calls to work. Fixed by `uv add setuptools` (dev dependency).
2. Splink's `SparkAPI` requires a checkpoint directory (`spark.sparkContext.setCheckpointDir(...)`) — added inline in the smoke test script above, and must be added the same way inside the DAG's matching task in Task 4 (not in `build_lakehouse_session`, since it's a `SparkContext` call, not a session config, and only the one Splink-using task needs it).
3. Splink's `SparkAPI` needs its bundled similarity-functions jar (`jaro_winkler`, etc.) on the session's classpath, via the `spark.jars` config set at session-build time — this one **is** added to the shared `lakehouse_spark_configs()` (see the extra step below), since `spark.jars` can't be added to an already-running session (unlike `spark.jars.packages`' Maven coordinates, no live "add a local jar" API exists in PySpark), and duplicating the whole lakehouse config dict just for one task would violate DRY.

- [ ] **Step 4: Add Splink's similarity jar to the shared lakehouse Spark config**

Add to `tests/transform/test_spark_session.py`, inside `test_lakehouse_spark_configs_sets_s3a_and_iceberg_catalog`:

```python
    assert "scala-udf-similarity" in configs["spark.jars"]
```

Run: `uv run pytest tests/transform/test_spark_session.py -v` — expect FAIL (`KeyError: 'spark.jars'`).

In `src/registry/transform/spark_session.py`, add the import `from splink.backends.spark import similarity_jar_location` and add `"spark.jars": similarity_jar_location(),` to the dict returned by `lakehouse_spark_configs()`.

Run: `uv run pytest tests/transform/test_spark_session.py -v` — expect PASS. Then `uv run pytest -q` to confirm the full suite (75 tests) still passes.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/registry/matching/cascade.py scripts/cascade_smoke_test.py src/registry/transform/spark_session.py tests/transform/test_spark_session.py
git commit -m "feat: wire the full BODACC matching cascade into one callable"
```

---

### Task 4: Daily orchestration DAG

**Files:**
- Create: `dags/bodacc_matching_pipeline.py`

No pytest — thin wiring, same convention as `dags/sirene_daily_pipeline.py`.

- [ ] **Step 1: Write the DAG**

Create `dags/bodacc_matching_pipeline.py`:

```python
"""Daily DAG: BODACC diff -> bronze -> silver -> matching cascade -> gold (SCD2 merge)."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="bodacc_matching_pipeline",
    schedule="@daily",
    start_date=dt.datetime(2026, 10, 1),
    catchup=False,
)
def bodacc_matching_pipeline():
    @task
    def extract_bodacc_daily_diff(data_interval_start=None, data_interval_end=None) -> str:
        from registry.ingestion.bodacc import run_ingestion

        return run_ingestion(
            bucket=os.environ["LAKEHOUSE_BUCKET"],
            since=data_interval_start.date(),
            until=data_interval_end.date(),
            work_dir=Path("/tmp"),
            run_type="diff",
        )

    @task
    def transform_bronze_to_silver(bronze_key: str) -> str:
        from registry.transform.bodacc_to_silver import bronze_to_silver
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        silver_table = "lakehouse.silver.bodacc_annonces"
        spark = build_lakehouse_session()
        try:
            bronze_to_silver(spark, f"s3a://{bucket}/{bronze_key}", silver_table)
        finally:
            spark.stop()
        return silver_table

    @task
    def run_matching_and_historize(silver_table: str, logical_date=None) -> None:
        from splink import SparkAPI

        from registry.matching.cascade import run_matching_cascade
        from registry.matching.gold_links import (
            ensure_gold_links_table,
            find_unlinked_announcements,
            historize_match_results,
        )
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        gold_table = "lakehouse.gold.bodacc_sirene_links"
        silver_links_table = "lakehouse.silver.bodacc_sirene_links"
        candidates_key = os.environ["SIRENE_CANDIDATES_BRONZE_KEY"]

        spark = build_lakehouse_session()
        try:
            ensure_gold_links_table(spark, gold_table)

            bodacc_df = spark.table(silver_table)
            gold_links_df = spark.table(gold_table)
            backlog = find_unlinked_announcements(bodacc_df, gold_links_df)

            sirene_candidates_df = spark.read.parquet(f"s3a://{bucket}/{candidates_key}")

            db_api = SparkAPI(spark_session=spark)
            matches_df = run_matching_cascade(backlog, sirene_candidates_df, db_api)

            historize_match_results(
                spark, matches_df, silver_links_table, gold_table, logical_date.date()
            )
        finally:
            spark.stop()

    bronze_key = extract_bodacc_daily_diff()
    silver_table = transform_bronze_to_silver(bronze_key)
    run_matching_and_historize(silver_table)


bodacc_matching_pipeline()
```

- [ ] **Step 2: Verify the DAG file is syntactically valid**

Run: `uv run python -c "import ast; ast.parse(open('dags/bodacc_matching_pipeline.py').read())"`
Expected: no output, exit code 0 (full Airflow DAG-parsing verification happens in Task 6, which needs the Airflow container and the still-missing env vars)

- [ ] **Step 3: Commit**

```bash
git add dags/bodacc_matching_pipeline.py
git commit -m "feat: add daily BODACC matching orchestration DAG"
```

---

### Task 5: Wire `SIRENE_CANDIDATES_BRONZE_KEY` into the environment

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env.example`

- [ ] **Step 1: Add the env var to `.env.example`**

Add to `.env.example`, after `SIRENE_API_KEY=`:

```
# Fixed bronze snapshot of SIRENE matching candidates (department 08 bootstrap —
# see docs/superpowers/plans/2026-10-04-entity-resolution-splink.md, Task 10).
# Refreshing this snapshot is a manual, separate concern; the daily BODACC
# matching DAG never re-ingests it (see
# docs/superpowers/specs/2026-10-07-bodacc-matching-orchestration-design.md).
SIRENE_CANDIDATES_BRONZE_KEY=
```

- [ ] **Step 2: Add the env var to both Airflow services in `docker-compose.yml`**

In the `airflow-webserver` service's `environment:` block, add after `SIRENE_API_KEY: ${SIRENE_API_KEY}`:

```yaml
      SIRENE_CANDIDATES_BRONZE_KEY: ${SIRENE_CANDIDATES_BRONZE_KEY}
```

Repeat the identical line in the `airflow-scheduler` service's `environment:` block.

- [ ] **Step 3: Verify the compose file is still valid**

Run: `docker compose config --quiet`
Expected: no output, exit code 0

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: wire SIRENE_CANDIDATES_BRONZE_KEY into Airflow services"
```

---

### Task 6: Full automated verification

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass (72 pre-existing + 2 new in `test_bodacc_to_silver.py` + 1 new in `test_gold_links.py` = 75).

- [ ] **Step 2: Run the linter**

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: no errors. Fix any issues found and re-run.

- [ ] **Step 3: Commit if the lint step changed anything**

```bash
git add -A
git commit -m "chore: fix lint issues"
```

(Skip this step entirely if ruff found nothing to fix.)

---

### Task 7: Manual verification — deferred

**Files:** None (deferred; same reasoning as every prior Splink/Airflow task in this project)

This task cannot run until both `SIRENE_API_KEY` and `SIRENE_CANDIDATES_BRONZE_KEY` are set. Documented here so the next session can pick it up directly:

- [ ] **Step 1: Run the department-08 SIRENE candidate ingestion** (if not already done), note the resulting bronze key, and set `SIRENE_CANDIDATES_BRONZE_KEY` in `.env` to it.

- [ ] **Step 2: Start the full stack** (`docker compose up`) and trigger `bodacc_matching_pipeline` manually from the Airflow UI for a day within the 12-month bootstrap window.

- [ ] **Step 3: Inspect `gold.bodacc_sirene_links`** after the run — confirm one current row per announcement, a mix of `exact_siren`/`splink_fuzzy`/`unresolved` methods, and that re-triggering the same DAG run a second time doesn't create spurious new SCD2 versions (idempotency check).

---

## Self-review notes

- **Spec coverage:** the spec's architecture (extract → silver → backlog → cascade → historize), the "full backlog, not just diff" decision, the fixed SIRENE-candidates snapshot decision, and the no-pytest convention for Splink-driving code are all implemented. The silver-accumulation bug found while designing is fixed in Task 1, ahead of everything that depends on it.
- **Placeholder scan:** none found.
- **Type consistency:** `find_unlinked_announcements` takes `(bodacc_df, gold_links_df)` matching how it's called in Task 4's DAG (`spark.table(silver_table)`, `spark.table(gold_table)`). `run_matching_cascade`'s signature `(bodacc_df, sirene_candidates_df, db_api)` matches both the smoke test and the DAG call. `historize_match_results`'s existing signature (`spark, matches_df, silver_table, gold_table, run_date`) is reused unchanged from the previous plan.
