# Gold Historization + Evaluation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Historize BODACC↔SIRENE match results into an SCD2 `gold.bodacc_sirene_links` table, and implement the blind-holdout evaluation that measures the fuzzy matcher's real precision/recall.

**Architecture:** Mirror Step 1 Plan 2's `silver_to_gold.py` pattern exactly: a new `gold_links.py` writes the combined match results to a silver table, then applies a two-statement `MERGE INTO` (close current version) + `INSERT` (open new version) SCD2 merge into gold, keyed on `bodacc_announcement_id`. A new `evaluation.py` implements the spec's blind-holdout methodology by reusing the existing exact-match results as trusted ground truth — no separate masking step is needed, since the fuzzy pipeline (`prepare_bodacc_for_matching`) never reads the declared SIREN in the first place.

**Tech Stack:** PySpark (DataFrame/SQL API), Apache Iceberg (`MERGE INTO`), pytest with the existing `spark_session`/`table_suffix` fixtures.

---

## Decisions made

- **`match_confidence` is excluded from SCD2 change-detection** (`TRACKED_COLUMNS = ["siret_siege", "match_method"]`). Splink's `estimate_u_using_random_sampling` defaults to `seed=None` (confirmed by inspecting the installed `LinkerTraining.estimate_u_using_random_sampling` signature), meaning run-to-run floating-point drift in `match_probability` is possible in principle. A determinism check against the toy fixture in `scripts/splink_smoke_test.py` (run twice) produced identical `match_probability` both times, but that check is inconclusive — the toy dataset has only 4 possible record pairs, too few for random sampling to actually vary. Rather than rely on an inconclusive toy-scale test, this plan treats confidence as point-in-time metadata, not part of a match's identity, and keeps it out of change-detection regardless. Task 8 additionally pins a fixed `seed` so training itself is reproducible — a cheap, independent safeguard, not a substitute for excluding confidence from `TRACKED_COLUMNS`.
- **Every BODACC announcement gets a gold row, including unresolved ones.** The spec's "Data quality" section requires one current row per `bodacc_announcement_id`, and its cascade allows an explicit `unresolved` outcome (`match_method = unresolved`, `siret_siege = NULL`) for announcements neither stage resolved. The current `combine.py` (from the entity-resolution plan) only unions exact and fuzzy results, silently dropping announcements with no match at all. Task 1 closes this gap with a `resolve_unresolved_matches` function, before gold historization even exists — otherwise gold would never reflect "we don't know" as a queryable state.
- **The evaluation set reuses `exact_siren_matches` output directly as ground truth**, per the adaptation already made in the entity-resolution plan: Step 1's gold SIRENE table only holds 2 fixture rows, so matching (and therefore evaluation) runs against freshly-fetched, department-scoped SIRENE candidates rather than Step 1's gold table. The spec's "hide the declared SIREN" step requires no extra code: `prepare_bodacc_for_matching` already never looks at `siren_declared`.
- **dbt tests for `gold.bodacc_sirene_links` are out of scope for this plan.** The spec asks for them, but mirroring Step 1's Plan 2 (transformations) / Plan 4 (dbt tests) split, they're deferred to a follow-up plan once the table shape is proven correct by this plan's own pytest coverage.
- **Task 10 (real end-to-end run against department-08 data) remains deferred**, consistent with every prior plan in this project: `SIRENE_API_KEY` is still empty in `.env`.

## Out of scope

- dbt tests on `gold.bodacc_sirene_links` (deferred to a follow-up plan).
- Airflow orchestration of the matching + historization cascade (not yet designed; today's plans only cover ingestion and Step 1's SIRENE pipeline).
- Postgres sync / FastAPI endpoints for the links table (same reasoning — not yet designed for this table).

---

### Task 1: Add `unresolved` rows for announcements neither stage matched

**Files:**
- Modify: `src/registry/matching/combine.py`
- Test: `tests/matching/test_combine.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_combine.py`:

```python
def test_resolve_unresolved_matches_adds_rows_for_unmatched_announcements(spark_session):
    bodacc_df = spark_session.createDataFrame([("A1",), ("A2",), ("A3",)], schema=["id"])
    combined_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    result = (
        resolve_unresolved_matches(bodacc_df, combined_df)
        .orderBy("bodacc_announcement_id")
        .collect()
    )

    assert [row.bodacc_announcement_id for row in result] == ["A1", "A2", "A3"]
    assert result[0].match_method == "exact_siren"
    assert result[1].match_method == "unresolved"
    assert result[1].siret_siege is None
    assert result[1].match_confidence is None
    assert result[2].match_method == "unresolved"
```

Add `resolve_unresolved_matches` to the import line at the top of the file:

```python
from registry.matching.combine import (
    best_fuzzy_match_per_announcement,
    combine_match_results,
    extract_fuzzy_match_candidates,
    resolve_unresolved_matches,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_combine.py::test_resolve_unresolved_matches_adds_rows_for_unmatched_announcements -v`
Expected: FAIL with `ImportError: cannot import name 'resolve_unresolved_matches'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/combine.py`:

```python
def resolve_unresolved_matches(bodacc_df: DataFrame, combined_matches_df: DataFrame) -> DataFrame:
    unresolved_ids = bodacc_df.select(F.col("id").alias("bodacc_announcement_id")).join(
        combined_matches_df.select("bodacc_announcement_id"),
        on="bodacc_announcement_id",
        how="left_anti",
    )
    unresolved_df = unresolved_ids.select(
        F.col("bodacc_announcement_id"),
        F.lit(None).cast("string").alias("siren_bodacc"),
        F.lit(None).cast("string").alias("siret_siege"),
        F.lit("unresolved").alias("match_method"),
        F.lit(None).cast("double").alias("match_confidence"),
    )
    return combined_matches_df.unionByName(unresolved_df)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_combine.py -v`
Expected: PASS (all tests in the file, 4 total)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/combine.py tests/matching/test_combine.py
git commit -m "feat: add unresolved match rows for announcements neither stage matched"
```

---

### Task 2: `write_matches_to_silver`

**Files:**
- Create: `src/registry/matching/gold_links.py`
- Test: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Create `tests/matching/test_gold_links.py`:

```python
from registry.matching.gold_links import write_matches_to_silver

MATCH_SCHEMA = [
    "bodacc_announcement_id",
    "siren_bodacc",
    "siret_siege",
    "match_method",
    "match_confidence",
]


def test_write_matches_to_silver_creates_table(spark_session, table_suffix):
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    write_matches_to_silver(matches_df, silver_table)

    rows = spark_session.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0].siret_siege == "55203253400019"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.gold_links'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/matching/gold_links.py`:

```python
"""SCD2 historization of BODACC/SIRENE match results into `gold.bodacc_sirene_links`,
following the exact same two-statement MERGE+INSERT pattern as Step 1's
`silver_to_gold.py`."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession


def write_matches_to_silver(matches_df: DataFrame, silver_table: str) -> None:
    matches_df.writeTo(silver_table).createOrReplace()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/gold_links.py tests/matching/test_gold_links.py
git commit -m "feat: write combined match results to a silver table"
```

---

### Task 3: `ensure_gold_links_table`

**Files:**
- Modify: `src/registry/matching/gold_links.py`
- Test: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_gold_links.py`:

```python
from registry.matching.gold_links import ensure_gold_links_table, write_matches_to_silver


def test_ensure_gold_links_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"

    ensure_gold_links_table(spark_session, gold_table)
    ensure_gold_links_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "bodacc_announcement_id" in columns
    assert "match_confidence" in columns
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py::test_ensure_gold_links_table_is_idempotent -v`
Expected: FAIL with `ImportError: cannot import name 'ensure_gold_links_table'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/gold_links.py`:

```python
def ensure_gold_links_table(spark: SparkSession, gold_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            bodacc_announcement_id STRING,
            siren_bodacc STRING,
            siret_siege STRING,
            match_method STRING,
            match_confidence DOUBLE,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        ) USING iceberg
    """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/gold_links.py tests/matching/test_gold_links.py
git commit -m "feat: create gold.bodacc_sirene_links table DDL"
```

---

### Task 4: `apply_links_scd2_merge` — initial insert

**Files:**
- Modify: `src/registry/matching/gold_links.py`
- Test: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_gold_links.py`:

```python
import datetime as dt

from registry.matching.gold_links import (
    apply_links_scd2_merge,
    ensure_gold_links_table,
    write_matches_to_silver,
)


def test_apply_links_scd2_merge_inserts_new_links(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    ensure_gold_links_table(spark_session, gold_table)

    matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )
    write_matches_to_silver(matches_df, silver_table)

    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 6))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].bodacc_announcement_id == "A1"
    assert rows[0].siret_siege == "55203253400019"
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 6)
    assert rows[0].valid_to is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py::test_apply_links_scd2_merge_inserts_new_links -v`
Expected: FAIL with `ImportError: cannot import name 'apply_links_scd2_merge'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/gold_links.py`:

```python
TRACKED_COLUMNS = ["siret_siege", "match_method"]


def apply_links_scd2_merge(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    spark.table(silver_table).createOrReplaceTempView("incoming_links")

    comparison = " AND ".join(f"incoming_links.{c} <=> gold.{c}" for c in TRACKED_COLUMNS)

    changed_links = spark.sql(f"""
        SELECT incoming_links.bodacc_announcement_id
        FROM incoming_links
        JOIN {gold_table} AS gold
          ON incoming_links.bodacc_announcement_id = gold.bodacc_announcement_id
         AND gold.is_current = true
        WHERE NOT ({comparison})
    """)
    changed_links.createOrReplaceTempView("changed_links")

    spark.sql(f"""
        MERGE INTO {gold_table} AS gold
        USING changed_links AS c
        ON gold.bodacc_announcement_id = c.bodacc_announcement_id AND gold.is_current = true
        WHEN MATCHED THEN UPDATE SET
            gold.valid_to = DATE('{run_date.isoformat()}'),
            gold.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT
            incoming_links.bodacc_announcement_id,
            incoming_links.siren_bodacc,
            incoming_links.siret_siege,
            incoming_links.match_method,
            incoming_links.match_confidence,
            DATE('{run_date.isoformat()}') AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            true AS is_current
        FROM incoming_links
        LEFT JOIN {gold_table} AS gold
          ON incoming_links.bodacc_announcement_id = gold.bodacc_announcement_id
         AND gold.is_current = true
        WHERE gold.bodacc_announcement_id IS NULL
    """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/gold_links.py tests/matching/test_gold_links.py
git commit -m "feat: insert new links via SCD2 merge"
```

---

### Task 5: `apply_links_scd2_merge` — changed match is versioned

**Files:**
- Modify: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_gold_links.py`:

```python
def test_apply_links_scd2_merge_versions_changed_match(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    ensure_gold_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", None, "11111111100019", "splink_fuzzy", 0.75)], schema=MATCH_SCHEMA
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 6))

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 7))

    rows = spark_session.table(gold_table).orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].match_method == "splink_fuzzy"
    assert rows[0].is_current is False
    assert rows[0].valid_to == dt.date(2026, 10, 7)
    assert rows[1].match_method == "exact_siren"
    assert rows[1].siret_siege == "55203253400019"
    assert rows[1].is_current is True
    assert rows[1].valid_to is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py::test_apply_links_scd2_merge_versions_changed_match -v`
Expected: FAIL (table already has a row from the prior merge call's final state persisting across two merges — if it fails, inspect; expected failure mode here is actually a PASS already once Task 4's code runs, since the logic is generic. Run it anyway to confirm the behavior is actually exercised.)

- [ ] **Step 3: Confirm no implementation change is needed**

`apply_links_scd2_merge` from Task 4 already handles this case generically — this step exists to prove it with a dedicated test, not to write new code.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/matching/test_gold_links.py
git commit -m "test: cover SCD2 versioning when a match changes"
```

---

### Task 6: `apply_links_scd2_merge` — confidence drift alone does not version

**Files:**
- Modify: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_gold_links.py`:

```python
def test_apply_links_scd2_merge_ignores_confidence_only_drift(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    ensure_gold_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", None, "11111111100019", "splink_fuzzy", 0.650001)], schema=MATCH_SCHEMA
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 6))

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", None, "11111111100019", "splink_fuzzy", 0.650002)], schema=MATCH_SCHEMA
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 7))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 6)
    assert rows[0].match_confidence == 0.650001
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py::test_apply_links_scd2_merge_ignores_confidence_only_drift -v`
Expected: PASS immediately — `TRACKED_COLUMNS` already excludes `match_confidence`. This step proves the exclusion is load-bearing (the test would fail if someone added `match_confidence` back to `TRACKED_COLUMNS`), not that it's currently broken.

- [ ] **Step 3: Run the full file to confirm nothing regressed**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS (5 tests)

- [ ] **Step 4: Commit**

```bash
git add tests/matching/test_gold_links.py
git commit -m "test: confirm confidence drift alone never triggers a new SCD2 version"
```

---

### Task 7: `historize_match_results` orchestration

**Files:**
- Modify: `src/registry/matching/gold_links.py`
- Test: `tests/matching/test_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_gold_links.py`:

```python
from registry.matching.gold_links import historize_match_results


def test_historize_match_results_creates_table_and_merges(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    historize_match_results(
        spark_session, matches_df, silver_table, gold_table, dt.date(2026, 10, 6)
    )

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_gold_links.py::test_historize_match_results_creates_table_and_merges -v`
Expected: FAIL with `ImportError: cannot import name 'historize_match_results'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/gold_links.py`:

```python
def historize_match_results(
    spark: SparkSession,
    matches_df: DataFrame,
    silver_table: str,
    gold_table: str,
    run_date: dt.date,
) -> None:
    write_matches_to_silver(matches_df, silver_table)
    ensure_gold_links_table(spark, gold_table)
    apply_links_scd2_merge(spark, silver_table, gold_table, run_date)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_gold_links.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/gold_links.py tests/matching/test_gold_links.py
git commit -m "feat: orchestrate silver write + gold SCD2 merge for match results"
```

---

### Task 8: Pin a fixed seed for Splink's random sampling

**Files:**
- Modify: `src/registry/matching/splink_matching.py`

No pytest for this module, consistent with the entity-resolution plan's decision (same reasoning as the Airflow DAG: thin wiring around a third-party library, verified manually rather than mocked).

- [ ] **Step 1: Edit `train_linker`**

In `src/registry/matching/splink_matching.py`, change:

```python
def train_linker(linker) -> None:
    linker.training.estimate_probability_two_random_records_match([BLOCKING_RULE], recall=0.7)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.training.estimate_parameters_using_expectation_maximisation(BLOCKING_RULE)
```

to:

```python
def train_linker(linker) -> None:
    linker.training.estimate_probability_two_random_records_match([BLOCKING_RULE], recall=0.7)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=42)
    linker.training.estimate_parameters_using_expectation_maximisation(BLOCKING_RULE)
```

- [ ] **Step 2: Verify manually**

Run: `for i in 1 2; do uv run python scripts/splink_smoke_test.py 2>/dev/null | grep "^0"; done`
Expected: identical `match_probability` on both runs (this was already true with the default `seed=None` on this tiny toy dataset; pinning the seed makes that guarantee explicit rather than incidental — it no longer depends on the toy dataset being too small to sample from).

- [ ] **Step 3: Commit**

```bash
git add src/registry/matching/splink_matching.py
git commit -m "fix: pin a fixed seed for deterministic Splink u-probability training"
```

---

### Task 9: `build_evaluation_set`

**Files:**
- Create: `src/registry/matching/evaluation.py`
- Test: `tests/matching/test_evaluation.py`

- [ ] **Step 1: Write the failing test**

Create `tests/matching/test_evaluation.py`:

```python
from registry.matching.evaluation import build_evaluation_set

MATCH_SCHEMA = [
    "bodacc_announcement_id",
    "siren_bodacc",
    "siret_siege",
    "match_method",
    "match_confidence",
]


def test_build_evaluation_set_keeps_only_exact_matched_announcements(spark_session):
    bodacc_df = spark_session.createDataFrame(
        [("A1", "ACME SARL"), ("A2", "OTHER SARL")], schema=["id", "commercant"]
    )
    exact_matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    result = build_evaluation_set(bodacc_df, exact_matches_df).collect()

    assert len(result) == 1
    assert result[0].id == "A1"
    assert result[0].true_siret_siege == "55203253400019"
    assert result[0].commercant == "ACME SARL"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_evaluation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.evaluation'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/matching/evaluation.py`:

```python
"""Blind-holdout evaluation of the Splink fuzzy stage: reuse exact-SIREN matches as
trusted ground truth, since `prepare_bodacc_for_matching` never reads the declared
SIREN in the first place (see docs/superpowers/specs/
2026-10-03-bodacc-entity-resolution-design.md, 'Evaluation methodology')."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_evaluation_set(bodacc_df: DataFrame, exact_matches_df: DataFrame) -> DataFrame:
    ground_truth = exact_matches_df.select(
        F.col("bodacc_announcement_id").alias("id"),
        F.col("siret_siege").alias("true_siret_siege"),
    )
    return bodacc_df.join(ground_truth, on="id", how="inner")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_evaluation.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/evaluation.py tests/matching/test_evaluation.py
git commit -m "feat: build blind-holdout evaluation set from exact-SIREN matches"
```

---

### Task 10: `compute_precision_recall`

**Files:**
- Modify: `src/registry/matching/evaluation.py`
- Test: `tests/matching/test_evaluation.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/matching/test_evaluation.py`:

```python
from pyspark.sql.types import StringType, StructField, StructType

from registry.matching.evaluation import compute_precision_recall

FUZZY_PREDICTED_SCHEMA = StructType(
    [
        StructField("bodacc_announcement_id", StringType()),
        StructField("siret_siege", StringType()),
    ]
)


def test_compute_precision_recall_counts_correct_and_incorrect_matches(spark_session):
    evaluation_df = spark_session.createDataFrame(
        [("A1", "SIRET1"), ("A2", "SIRET2"), ("A3", "SIRET3")],
        schema=["id", "true_siret_siege"],
    )
    fuzzy_predicted_df = spark_session.createDataFrame(
        [("A1", "SIRET1"), ("A2", "WRONG_SIRET")],
        schema=["bodacc_announcement_id", "siret_siege"],
    )

    result = compute_precision_recall(evaluation_df, fuzzy_predicted_df)

    assert result["total"] == 3
    assert result["predicted"] == 2
    assert result["correct"] == 1
    assert result["precision"] == 0.5
    assert result["recall"] == 1 / 3


def test_compute_precision_recall_handles_no_predictions(spark_session):
    evaluation_df = spark_session.createDataFrame(
        [("A1", "SIRET1")], schema=["id", "true_siret_siege"]
    )
    fuzzy_predicted_df = spark_session.createDataFrame([], schema=FUZZY_PREDICTED_SCHEMA)

    result = compute_precision_recall(evaluation_df, fuzzy_predicted_df)

    assert result["total"] == 1
    assert result["predicted"] == 0
    assert result["correct"] == 0
    assert result["precision"] == 0.0
    assert result["recall"] == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_evaluation.py -v`
Expected: FAIL with `ImportError: cannot import name 'compute_precision_recall'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/evaluation.py`:

```python
def compute_precision_recall(evaluation_df: DataFrame, fuzzy_predicted_df: DataFrame) -> dict:
    total = evaluation_df.count()
    joined = evaluation_df.select(
        F.col("id").alias("bodacc_announcement_id"), "true_siret_siege"
    ).join(fuzzy_predicted_df, on="bodacc_announcement_id", how="left")

    predicted = joined.filter(F.col("siret_siege").isNotNull()).count()
    correct = joined.filter(F.col("siret_siege") == F.col("true_siret_siege")).count()

    precision = correct / predicted if predicted else 0.0
    recall = correct / total if total else 0.0

    return {
        "total": total,
        "predicted": predicted,
        "correct": correct,
        "precision": precision,
        "recall": recall,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_evaluation.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/evaluation.py tests/matching/test_evaluation.py
git commit -m "feat: compute precision and recall for the blind-holdout evaluation"
```

---

### Task 11: Full automated verification

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass. Actual result: 72 passed (62 pre-existing + 1 new in `test_combine.py` + 6 new in `test_gold_links.py` + 3 new in `test_evaluation.py`).

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

### Task 12: Manual verification — deferred

**Files:** None (deferred; same reasoning as the entity-resolution plan's Task 10)

This task cannot run until `SIRENE_API_KEY` is set in `.env`. Documented here so the next session can pick it up directly:

- [ ] **Step 1: Run the SIRENE candidate ingestion for department 08** (if not already run from the entity-resolution plan)

```bash
uv run python -c "
from registry.matching.sirene_candidates import run_candidate_ingestion
run_candidate_ingestion(bucket='lakehouse', api_key='<SIRENE_API_KEY>', department='08', work_dir='/tmp/sirene_candidates')
"
```

- [ ] **Step 2: Run the full matching cascade and historize into gold**

Run exact matching, fuzzy matching (via `build_linker`/`train_linker`/`predict_fuzzy_matches` against a `SparkAPI`), `combine_match_results`, `resolve_unresolved_matches`, then `historize_match_results` into a real `lakehouse.gold.bodacc_sirene_links` table.

- [ ] **Step 3: Run the blind-holdout evaluation on the same real data**

Call `build_evaluation_set` on the real `exact_siren_matches` output, re-run the fuzzy stage on that subset only, then `compute_precision_recall`. Record the real precision/recall numbers (even if disappointing) in the project's README, per the "measure everything, report honestly" standard.

---

## Self-review notes

- **Spec coverage:** gold historization (Tasks 2–7), evaluation methodology (Tasks 9–10), unresolved-match handling from "Data quality / error handling" (Task 1). dbt tests from "Testing" are explicitly deferred (see "Out of scope"). Splink determinism concern raised during design is resolved with a documented, honest "Decisions made" entry plus a defensive seed pin (Task 8), not an overclaimed empirical proof.
- **Placeholder scan:** none found — every step has runnable code or an exact command.
- **Type consistency:** `MATCH_SCHEMA` (5 columns: `bodacc_announcement_id`, `siren_bodacc`, `siret_siege`, `match_method`, `match_confidence`) is used identically across `test_combine.py`, `test_gold_links.py`, and `test_evaluation.py`, matching `exact_siren.py`'s and `combine.py`'s real output shape. `TRACKED_COLUMNS` in `gold_links.py` matches the two columns actually compared in every SCD2 test.
