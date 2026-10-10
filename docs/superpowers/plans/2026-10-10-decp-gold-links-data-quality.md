# DECP Gold Historization + Data Quality Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Historize resolved/validated DECP markets into `gold.decp_marches_links` (SCD2), and implement the data-quality coverage report that replaces BODACC's blind-holdout evaluation (there's nothing to hold out here — see the spec).

**Architecture:** Mirrors `gold_links.py`'s two-statement MERGE+INSERT SCD2 pattern exactly, keyed on `uid` instead of `bodacc_announcement_id`. The data-quality report is two small aggregation functions, not a holdout experiment — DECP has no probabilistic prediction to score (Plan 2 already established there's no fuzzy stage at all).

**Tech Stack:** PySpark (DataFrame/SQL API, `MERGE INTO`) — no new dependencies.

This is Plan 3 of Step 3 (DECP public procurement linkage). See
`docs/superpowers/specs/2026-10-08-decp-procurement-design.md` for the full
design and Plan 2 (`2026-10-09-decp-entity-resolution.md`) for
`resolve_decp_titulaires`/`validate_against_sirene`, whose output this
historizes. Orchestration, dbt tests, and serving are each their own
plan, deferred here — mirroring Step 2's split.

## Decisions made

- **`TRACKED_COLUMNS = ["siret_titulaire", "match_method", "montant"]`** — a
  deliberate difference from BODACC's links table, which only tracked the match
  outcome. DECP markets are *not* immutable the way BODACC announcements are:
  the source's own `modification_id`/`donneesActuelles` fields exist precisely
  because contract amounts and terms get revised after award. A contract's
  amount changing after the fact is itself a meaningful due-diligence fact
  ("this company's contract grew/shrank after signature"), not noise — so
  `montant` is tracked deliberately, not omitted for simplicity.
- **`siret_validated_in_sirene` is excluded from `TRACKED_COLUMNS`**, same
  reasoning as BODACC's `match_confidence` exclusion: it can change for reasons
  that have nothing to do with this market's own identity — specifically, once
  Step 1's real national SIRENE bootstrap is eventually run for real (still not
  done in this environment — see Plan 2's "Decisions made"), every currently
  `false` validation flag in this environment would flip to `true` on the next
  run, for every row, with nothing about the actual resolution having changed.
  Tracking it would flood gold with spurious versions on that one event.
- **The data-quality report needs the original silver DECP data, not just the
  validated output**, to report *why* rows are unresolved (identifier type,
  null-name breakdown) — `resolve_decp_titulaires`'s output (Plan 2) doesn't
  carry `titulaire_type_identifiant`/`titulaire_nom` forward, by design (it's a
  resolution function, not a diagnostic one). Rather than retrofitting that
  function's shape, `compute_unresolved_composition` takes both the validated
  output and the silver DataFrame and joins them on `uid` for just the
  unresolved subset — keeping Plan 2's already-shipped code untouched.
- **No deferred manual verification.** Plan 2 already produced real
  resolved/validated department-08 data in this environment. Task 10 historizes
  it for real and prints the real data-quality report.

## Architecture

```
resolve_decp_titulaires + validate_against_sirene output (Plan 2)
        │
        ▼  write_matches_to_silver
silver.decp_marches_links
        │
        ▼  apply_decp_links_scd2_merge (keyed on uid)
gold.decp_marches_links: uid, siret_titulaire, match_method,
  siret_validated_in_sirene, acheteur_id, acheteur_nom, montant, objet,
  code_cpv, valid_from, valid_to, is_current
        │
        ▼  compute_data_quality_report / compute_unresolved_composition
data-quality coverage report (resolved share, validated share, unresolved
  composition) — printed/reported, not asserted as a pytest pass/fail
```

---

### Task 1: `write_matches_to_silver`

**Files:**
- Create: `src/registry/matching/decp_gold_links.py`
- Test: `tests/matching/test_decp_gold_links.py`

- [ ] **Step 1: Write the failing test**

Create `tests/matching/test_decp_gold_links.py`:

```python
from registry.matching.decp_gold_links import write_matches_to_silver

MATCH_SCHEMA = [
    "uid",
    "siret_titulaire",
    "match_method",
    "siret_validated_in_sirene",
    "acheteur_id",
    "acheteur_nom",
    "montant",
    "objet",
    "code_cpv",
]


def test_write_matches_to_silver_creates_table(spark_session, table_suffix):
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "98236972000015",
                "source_siret",
                False,
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                510400.0,
                "LOT No1",
                "15300000",
            )
        ],
        schema=MATCH_SCHEMA,
    )

    write_matches_to_silver(matches_df, silver_table)

    rows = spark_session.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0].siret_titulaire == "98236972000015"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.decp_gold_links'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/matching/decp_gold_links.py`:

```python
"""SCD2 historization of resolved/validated DECP markets into
`gold.decp_marches_links`, following the exact same two-statement MERGE+INSERT
pattern as `gold_links.py` (BODACC) and `silver_to_gold.py` (SIRENE)."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession


def write_matches_to_silver(matches_df: DataFrame, silver_table: str) -> None:
    matches_df.writeTo(silver_table).createOrReplace()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_gold_links.py tests/matching/test_decp_gold_links.py
git commit -m "feat: write resolved DECP markets to a silver table"
```

---

### Task 2: `ensure_gold_decp_links_table`

**Files:**
- Modify: `src/registry/matching/decp_gold_links.py`
- Modify: `tests/matching/test_decp_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_decp_gold_links.py`:

```python
from registry.matching.decp_gold_links import ensure_gold_decp_links_table, write_matches_to_silver


def test_ensure_gold_decp_links_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"

    ensure_gold_decp_links_table(spark_session, gold_table)
    ensure_gold_decp_links_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "uid" in columns
    assert "siret_validated_in_sirene" in columns
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_decp_gold_links.py::test_ensure_gold_decp_links_table_is_idempotent -v`
Expected: FAIL with `ImportError: cannot import name 'ensure_gold_decp_links_table'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/decp_gold_links.py`:

```python
def ensure_gold_decp_links_table(spark: SparkSession, gold_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            uid STRING,
            siret_titulaire STRING,
            match_method STRING,
            siret_validated_in_sirene BOOLEAN,
            acheteur_id STRING,
            acheteur_nom STRING,
            montant DOUBLE,
            objet STRING,
            code_cpv STRING,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        ) USING iceberg
    """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_gold_links.py tests/matching/test_decp_gold_links.py
git commit -m "feat: create gold.decp_marches_links table DDL"
```

---

### Task 3: `apply_decp_links_scd2_merge` — initial insert

**Files:**
- Modify: `src/registry/matching/decp_gold_links.py`
- Modify: `tests/matching/test_decp_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_decp_gold_links.py`:

```python
import datetime as dt

from registry.matching.decp_gold_links import (
    apply_decp_links_scd2_merge,
    ensure_gold_decp_links_table,
    write_matches_to_silver,
)


def test_apply_decp_links_scd2_merge_inserts_new_links(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    ensure_gold_decp_links_table(spark_session, gold_table)

    matches_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "98236972000015",
                "source_siret",
                False,
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                510400.0,
                "LOT No1",
                "15300000",
            )
        ],
        schema=MATCH_SCHEMA,
    )
    write_matches_to_silver(matches_df, silver_table)

    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 10))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].uid == "M1"
    assert rows[0].siret_titulaire == "98236972000015"
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 10)
    assert rows[0].valid_to is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_decp_gold_links.py::test_apply_decp_links_scd2_merge_inserts_new_links -v`
Expected: FAIL with `ImportError: cannot import name 'apply_decp_links_scd2_merge'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/decp_gold_links.py`:

```python
TRACKED_COLUMNS = ["siret_titulaire", "match_method", "montant"]


def apply_decp_links_scd2_merge(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    spark.table(silver_table).createOrReplaceTempView("incoming_links")

    comparison = " AND ".join(f"incoming_links.{c} <=> gold.{c}" for c in TRACKED_COLUMNS)

    changed_links = spark.sql(f"""
        SELECT incoming_links.uid
        FROM incoming_links
        JOIN {gold_table} AS gold
          ON incoming_links.uid = gold.uid
         AND gold.is_current = true
        WHERE NOT ({comparison})
    """)
    changed_links.createOrReplaceTempView("changed_links")

    spark.sql(f"""
        MERGE INTO {gold_table} AS gold
        USING changed_links AS c
        ON gold.uid = c.uid AND gold.is_current = true
        WHEN MATCHED THEN UPDATE SET
            gold.valid_to = DATE('{run_date.isoformat()}'),
            gold.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT
            incoming_links.uid,
            incoming_links.siret_titulaire,
            incoming_links.match_method,
            incoming_links.siret_validated_in_sirene,
            incoming_links.acheteur_id,
            incoming_links.acheteur_nom,
            incoming_links.montant,
            incoming_links.objet,
            incoming_links.code_cpv,
            DATE('{run_date.isoformat()}') AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            true AS is_current
        FROM incoming_links
        LEFT JOIN {gold_table} AS gold
          ON incoming_links.uid = gold.uid
         AND gold.is_current = true
        WHERE gold.uid IS NULL
    """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_gold_links.py tests/matching/test_decp_gold_links.py
git commit -m "feat: insert new DECP links via SCD2 merge"
```

---

### Task 4: `apply_decp_links_scd2_merge` — changed amount is versioned

**Files:**
- Modify: `tests/matching/test_decp_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_decp_gold_links.py`:

```python
def test_apply_decp_links_scd2_merge_versions_changed_amount(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    ensure_gold_decp_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    False,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    510400.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 10))

    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    False,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    600000.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 11))

    rows = spark_session.table(gold_table).orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].montant == 510400.0
    assert rows[0].is_current is False
    assert rows[0].valid_to == dt.date(2026, 10, 11)
    assert rows[1].montant == 600000.0
    assert rows[1].is_current is True
    assert rows[1].valid_to is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_decp_gold_links.py::test_apply_decp_links_scd2_merge_versions_changed_amount -v`
Expected: PASS immediately — `apply_decp_links_scd2_merge` from Task 3 already handles this generically. This step exists to prove it with a dedicated test, same convention as every prior plan's equivalent "changed value is versioned" task.

- [ ] **Step 3: Run the full file to confirm nothing regressed**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: PASS (4 tests)

- [ ] **Step 4: Commit**

```bash
git add tests/matching/test_decp_gold_links.py
git commit -m "test: cover SCD2 versioning when a market's amount changes"
```

---

### Task 5: `apply_decp_links_scd2_merge` — validation-flag drift alone does not version

**Files:**
- Modify: `tests/matching/test_decp_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_decp_gold_links.py`:

```python
def test_apply_decp_links_scd2_merge_ignores_validation_flag_only_drift(
    spark_session, table_suffix
):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    ensure_gold_decp_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    False,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    510400.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 10))

    # Same market, same siret/method/amount — only siret_validated_in_sirene flips
    # (e.g. a later real SIRENE bootstrap now finds this SIRET). Must NOT version.
    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    True,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    510400.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 11))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 10)
    assert rows[0].siret_validated_in_sirene is False
```

- [ ] **Step 2: Run the full file to confirm it passes (proving the exclusion is load-bearing)**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: PASS (5 tests) — if `siret_validated_in_sirene` were ever added to
`TRACKED_COLUMNS`, this test would start failing, which is the point of it.

- [ ] **Step 3: Commit**

```bash
git add tests/matching/test_decp_gold_links.py
git commit -m "test: confirm validation-flag drift alone never triggers a new SCD2 version"
```

---

### Task 6: `historize_decp_links` orchestration

**Files:**
- Modify: `src/registry/matching/decp_gold_links.py`
- Modify: `tests/matching/test_decp_gold_links.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_decp_gold_links.py`:

```python
from registry.matching.decp_gold_links import historize_decp_links


def test_historize_decp_links_creates_table_and_merges(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "98236972000015",
                "source_siret",
                False,
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                510400.0,
                "LOT No1",
                "15300000",
            )
        ],
        schema=MATCH_SCHEMA,
    )

    historize_decp_links(spark_session, matches_df, silver_table, gold_table, dt.date(2026, 10, 10))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_decp_gold_links.py::test_historize_decp_links_creates_table_and_merges -v`
Expected: FAIL with `ImportError: cannot import name 'historize_decp_links'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/decp_gold_links.py`:

```python
def historize_decp_links(
    spark: SparkSession,
    matches_df: DataFrame,
    silver_table: str,
    gold_table: str,
    run_date: dt.date,
) -> None:
    write_matches_to_silver(matches_df, silver_table)
    ensure_gold_decp_links_table(spark, gold_table)
    apply_decp_links_scd2_merge(spark, silver_table, gold_table, run_date)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_decp_gold_links.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_gold_links.py tests/matching/test_decp_gold_links.py
git commit -m "feat: orchestrate silver write + gold SCD2 merge for DECP links"
```

---

### Task 7: `compute_data_quality_report`

**Files:**
- Create: `src/registry/matching/decp_data_quality.py`
- Test: `tests/matching/test_decp_data_quality.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/matching/test_decp_data_quality.py`:

```python
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from registry.matching.decp_data_quality import compute_data_quality_report

VALIDATED_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("siret_titulaire", StringType()),
        StructField("match_method", StringType()),
        StructField("siret_validated_in_sirene", BooleanType()),
        StructField("montant", DoubleType()),
    ]
)


def test_compute_data_quality_report_counts_resolved_and_validated(spark_session):
    validated_df = spark_session.createDataFrame(
        [
            ("M1", "98236972000015", "source_siret", True, 510400.0),
            ("M2", "11111111100011", "source_siret", False, 2000.0),
            ("M3", None, "unresolved", None, 3000.0),
        ],
        schema=VALIDATED_SCHEMA_TYPED,
    )

    report = compute_data_quality_report(validated_df)

    assert report["total"] == 3
    assert report["resolved"] == 2
    assert report["unresolved"] == 1
    assert report["resolved_share"] == 2 / 3
    assert report["validated_in_sirene"] == 1
    assert report["validated_share"] == 1 / 2


def test_compute_data_quality_report_handles_empty_input(spark_session):
    validated_df = spark_session.createDataFrame([], schema=VALIDATED_SCHEMA_TYPED)

    report = compute_data_quality_report(validated_df)

    assert report["total"] == 0
    assert report["resolved"] == 0
    assert report["resolved_share"] == 0.0
    assert report["validated_share"] == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_decp_data_quality.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.decp_data_quality'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/matching/decp_data_quality.py`:

```python
"""Data-quality coverage report for DECP entity resolution — the equivalent of
BODACC's blind-holdout evaluation, adapted to the fact that there's no
probabilistic prediction to score here (see
docs/superpowers/specs/2026-10-08-decp-procurement-design.md)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_data_quality_report(validated_df: DataFrame) -> dict:
    total = validated_df.count()
    resolved = validated_df.filter(F.col("match_method") == "source_siret").count()
    unresolved = total - resolved
    validated_in_sirene = validated_df.filter(
        F.col("siret_validated_in_sirene") == True  # noqa: E712
    ).count()

    return {
        "total": total,
        "resolved": resolved,
        "unresolved": unresolved,
        "resolved_share": resolved / total if total else 0.0,
        "validated_in_sirene": validated_in_sirene,
        "validated_share": validated_in_sirene / resolved if resolved else 0.0,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_decp_data_quality.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_data_quality.py tests/matching/test_decp_data_quality.py
git commit -m "feat: compute the DECP data-quality coverage report"
```

---

### Task 8: `compute_unresolved_composition`

**Files:**
- Modify: `src/registry/matching/decp_data_quality.py`
- Modify: `tests/matching/test_decp_data_quality.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/matching/test_decp_data_quality.py`:

```python
from registry.matching.decp_data_quality import compute_unresolved_composition

DECP_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("titulaire_type_identifiant", StringType()),
        StructField("titulaire_nom", StringType()),
    ]
)


def test_compute_unresolved_composition_breaks_down_by_identifiant_type(spark_session):
    validated_df = spark_session.createDataFrame(
        [
            ("M1", "98236972000015", "source_siret", True, 510400.0),
            ("M2", None, "unresolved", None, 2000.0),
            ("M3", None, "unresolved", None, 3000.0),
        ],
        schema=VALIDATED_SCHEMA_TYPED,
    )
    decp_df = spark_session.createDataFrame(
        [
            ("M1", "SIRET", "PRIMEURS CHAMPARDENNAIS"),
            ("M2", "TVA", None),
            ("M3", None, None),
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    result = (
        compute_unresolved_composition(validated_df, decp_df)
        .orderBy("titulaire_type_identifiant")
        .collect()
    )

    assert len(result) == 2
    assert result[0].titulaire_type_identifiant is None
    assert result[0]["count"] == 1
    assert result[0].null_nom_count == 1
    assert result[1].titulaire_type_identifiant == "TVA"
    assert result[1]["count"] == 1
    assert result[1].null_nom_count == 1
```

**Note (post-implementation):** use `result[0]["count"]`, not `result[0].count`
— PySpark's `Row` inherits a built-in `count()` method (for counting value
occurrences in the row's tuple), which shadows attribute access for a column
literally named `count`. Dict-style indexing avoids the collision.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/matching/test_decp_data_quality.py::test_compute_unresolved_composition_breaks_down_by_identifiant_type -v`
Expected: FAIL with `ImportError: cannot import name 'compute_unresolved_composition'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/decp_data_quality.py`:

```python
def compute_unresolved_composition(validated_df: DataFrame, decp_df: DataFrame) -> DataFrame:
    unresolved_ids = validated_df.filter(F.col("match_method") == "unresolved").select("uid")
    return (
        decp_df.join(unresolved_ids, on="uid", how="inner")
        .groupBy("titulaire_type_identifiant")
        .agg(
            F.count("*").alias("count"),
            F.sum(F.when(F.col("titulaire_nom").isNull(), 1).otherwise(0)).alias(
                "null_nom_count"
            ),
        )
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_decp_data_quality.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_data_quality.py tests/matching/test_decp_data_quality.py
git commit -m "feat: break down unresolved DECP markets by identifier type"
```

---

### Task 9: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass. Actual result: 103 passed (94 pre-existing + 6 in
`test_decp_gold_links.py` + 3 in `test_decp_data_quality.py`).

- [ ] **Step 2: Run the linter**

Run: `uv run ruff check . && uv run ruff format --check src/ tests/ scripts/`
Expected: no errors. Fix any issues found and re-run.

- [ ] **Step 3: Commit if the lint step changed anything**

```bash
git add -A
git commit -m "chore: fix lint issues"
```

(Skip this step entirely if ruff found nothing to fix.)

---

### Task 10: Manual verification — real historization and real data-quality report

**Files:** none (manual verification only)

No deferral — Plan 2 already produced real resolved/validated department-08
data in this environment.

- [ ] **Step 1: Ensure Garage is running**

```bash
docker compose up -d garage
set -a && source .env && set +a
```

- [ ] **Step 2: Historize the real resolved/validated data**

```bash
uv run python -c "
import datetime as dt
from registry.matching.decp_resolution import resolve_decp_titulaires, validate_against_sirene
from registry.matching.decp_gold_links import historize_decp_links
from registry.transform.spark_session import build_lakehouse_session

spark = build_lakehouse_session()
decp_df = spark.table('lakehouse.silver.decp_marches')
resolved_df = resolve_decp_titulaires(decp_df)
sirene_gold_df = spark.table('lakehouse.gold.sirene_etablissements_historized')
validated_df = validate_against_sirene(resolved_df, sirene_gold_df)

historize_decp_links(
    spark,
    validated_df,
    'lakehouse.silver.decp_marches_links',
    'lakehouse.gold.decp_marches_links',
    dt.date.today(),
)
print('gold row count:', spark.table('lakehouse.gold.decp_marches_links').count())
spark.stop()
"
```

Expected: no errors; gold row count matching Plan 2's real resolution output
(~1,293 rows, same ballpark as the ingestion/resolution plans' real runs).

- [ ] **Step 3: Compute and print the real data-quality report**

```bash
uv run python -c "
from registry.matching.decp_data_quality import (
    compute_data_quality_report,
    compute_unresolved_composition,
)
from registry.transform.spark_session import build_lakehouse_session

spark = build_lakehouse_session()
validated_df = spark.table('lakehouse.gold.decp_marches_links').filter('is_current = true')
decp_df = spark.table('lakehouse.silver.decp_marches')

report = compute_data_quality_report(validated_df)
print(report)
compute_unresolved_composition(validated_df, decp_df).show()
spark.stop()
"
```

Expected: a real report — based on Plan 2's real run, roughly 1,286 resolved /
7 unresolved, `validated_share` near 0 (Step 1's real SIRENE bootstrap still
hasn't run in this environment — see Plan 2's "Decisions made"), and an
unresolved breakdown showing `TVA` and `NULL` identifier types, each with
`null_nom_count` equal to their own row count (confirmed in Plan 2: none of
these 7 rows have a titulaire name at all). Report the real numbers exactly as
produced, including the near-zero validation rate — that is the honest,
explainable state of this environment, not a flaw in this plan.

**Real bug found and fixed while running this step:** the first run of this
report showed a `SIRET`-typed row inside the *unresolved* breakdown — which
should be impossible, since `SIRET`-typed rows always resolve. Investigation
found a real department-08 market (`uid =
21080372200417202626_302_45110000`) awarded jointly to two titulaires (a
"groupement"): one resolved via a real SIRET (`GABELLA S.A.`), one not (a
`TVA`-typed, unnamed co-awardee). `compute_unresolved_composition` joined on
`uid` alone, so the resolved sibling leaked into the unresolved bucket just for
sharing a market id. This is exactly the scenario the spec's "Data quality /
error handling" section flagged as unconfirmed ("this needs confirming against
real data in the matching plan, not assumed from the one sample inspected
here") — now confirmed real. Fixed by excluding resolved `(uid, titulaire_id)`
*pairs* rather than including rows whose `uid` merely appears in the unresolved
set; a regression test (`test_compute_unresolved_composition_excludes_resolved_sibling_on_shared_uid`)
reproduces the exact real case. After the fix, the real unresolved breakdown
was `NULL: 3, TVA: 4` — no `SIRET` row, and the counts sum to 7 as expected.

---

## Self-review notes

- **Spec coverage:** the spec's gold historization and "data quality
  measurement" sections (items 1-4) are both implemented. Item 3 specifically
  asked for the *actual composition* of unresolved rows, not just a count —
  `compute_unresolved_composition` delivers exactly that.
- **Placeholder scan:** none found.
- **Type consistency:** `MATCH_SCHEMA` (9 columns) matches
  `validate_against_sirene`'s real output shape from Plan 2 exactly.
  `TRACKED_COLUMNS` matches the three columns actually compared in every SCD2
  test. `compute_unresolved_composition`'s `decp_df` parameter expects the same
  `titulaire_type_identifiant`/`titulaire_nom` columns `clean_decp_bronze`
  (Plan 2) produces.
