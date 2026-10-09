# DECP Entity Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clean the ingested DECP bronze data into silver, then resolve each market's titulaire to a SIRET — trusting the source's own typed identifier when present, validating it against the national SIRENE registry, and marking everything else `unresolved`. No fuzzy matching stage (see the spec for why).

**Architecture:** `decp_to_silver.py` mirrors `bodacc_to_silver.py`'s cleaning shape but uses `createOrReplace` (not the accumulate-via-MERGE pattern BODACC needed — see "Decisions made" for why that's correct here, not a regression). `decp_resolution.py` is two small, pure DataFrame functions: a case-based branch (no join needed to resolve the identity itself) and a validation join against Step 1's national SIRENE gold table.

**Tech Stack:** PySpark (DataFrame/SQL API) — no Splink, no new dependencies. Same `spark_session`/`table_suffix` test fixtures as every other transform/matching module.

This is Plan 2 of Step 3 (DECP public procurement linkage). See
`docs/superpowers/specs/2026-10-08-decp-procurement-design.md` for the full
design and `docs/superpowers/plans/2026-10-09-decp-ingestion.md` (Plan 1) for the
bronze data this cleans. Gold historization and the data-quality measurement
report are the next plan — deferred here, mirroring how Step 2 split entity
resolution (Plan 2) from gold historization + evaluation (Plan 3).

## Decisions made

- **`decp_to_silver.bronze_to_silver` uses `createOrReplace`, deliberately —
  this is not the bug Plan 1 of Step 2 found and fixed for BODACC.** That fix
  was needed because BODACC's bronze is a true incremental diff (each run only
  has *new* announcements), so silver had to accumulate across runs to retain
  history. DECP's ingestion (Plan 1 of this step) re-downloads and re-filters
  the *entire* current national file to the full scope on every run — each
  run's bronze output already *is* the complete current scoped universe, so
  overwriting silver with it is correct, not lossy.
- **Resolution is a case check, not a join.** Unlike `exact_siren_matches`
  (which has to look up an unknown SIRET from a declared SIREN), DECP's
  `titulaire_id` already *is* the SIRET when `titulaire_typeIdentifiant = SIRET`
  — there's nothing to look up. `resolve_decp_titulaires` is a pure `CASE WHEN`
  over columns already in hand; no candidates table is joined at this stage.
- **SIRENE validation is a separate function from resolution, and only ever
  produces `True`/`False`/`NULL` — never changes `match_method`.** Whether a
  SIRET exists in `gold.sirene_etablissements_historized` is a data-quality
  signal about the *upstream source*, not a statement about how confident this
  project is in the identity (which came from the source's own typed field,
  full stop). `siret_validated_in_sirene` is `NULL` for `unresolved` rows (no
  SIRET to check — not "checked and failed") and `True`/`False` for
  `source_siret` rows.
- **`titulaire_typeIdentifiant` is normalized to uppercase during silver
  cleaning**, not inside the resolution function — normalization is a cleaning
  concern (handled once), and `resolve_decp_titulaires` can then do a plain
  equality check without repeating it.
- **This plan's functions live in `registry.matching`**, not
  `registry.transform`, alongside `exact_siren.py` — both are plain
  deterministic-join/case-check entity-resolution logic against SIRENE, with no
  ML involved, mirroring that module's existing precedent rather than treating
  "uses Splink" as the dividing line for what counts as "matching."
- **No deferred manual verification this time either.** Plan 1's real bronze
  data is already sitting in Garage from its own (non-deferred) manual
  verification. Task 4 here runs the real cleaning + resolution against it.
  SIRENE validation will show few or no hits in this environment specifically
  because Step 1's real national SIRENE bootstrap has never actually been run
  (a separate, already-known, already-deferred gap from Step 1) — not because
  anything in this plan doesn't work. That's reported honestly, not hidden.

## Architecture

```
bronze/decp/ingestion_date=.../marches.parquet  (Plan 1's output, already real)
        │
        ▼  decp_to_silver.bronze_to_silver (createOrReplace)
silver.decp_marches (Iceberg: uid, acheteur_id, acheteur_nom, titulaire_id,
                      titulaire_type_identifiant, titulaire_nom, objet, montant,
                      code_cpv, date_notification)
        │
        ▼  resolve_decp_titulaires (case check, no join)
resolved: uid, siret_titulaire, match_method, acheteur_id, acheteur_nom,
          montant, objet, code_cpv, date_notification
        │
        ▼  validate_against_sirene (left join vs. gold.sirene_etablissements_historized,
           is_current=true)
validated: + siret_validated_in_sirene (True / False / NULL)
```

---

### Task 1: `clean_decp_bronze`

**Files:**
- Create: `src/registry/transform/decp_to_silver.py`
- Test: `tests/transform/test_decp_to_silver.py`

- [ ] **Step 1: Write the failing test**

Create `tests/transform/test_decp_to_silver.py`:

```python
import datetime as dt

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from registry.transform.decp_to_silver import bronze_to_silver, clean_decp_bronze

BRONZE_SCHEMA = StructType(
    [
        StructField("uid", StringType()),
        StructField("acheteur_id", StringType()),
        StructField("acheteur_nom", StringType()),
        StructField("titulaire_id", StringType()),
        StructField("titulaire_typeIdentifiant", StringType()),
        StructField("titulaire_nom", StringType()),
        StructField("objet", StringType()),
        StructField("montant", DoubleType()),
        StructField("codeCPV", StringType()),
        StructField("dateNotification", DateType()),
        StructField("acheteur_departement_code", StringType()),
        StructField("datePublicationDonnees", DateType()),
        StructField("donneesActuelles", BooleanType()),
    ]
)


def test_clean_decp_bronze_types_and_renames_columns(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1 : FRUITS ET LEGUMES",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_decp_bronze(raw_df).collect()[0]

    assert row.uid == "M1"
    assert row.titulaire_id == "98236972000015"
    assert row.titulaire_type_identifiant == "SIRET"
    assert row.montant == 510400.0
    assert row.code_cpv == "15300000"
    assert row.date_notification == dt.date(2026, 5, 1)


def test_clean_decp_bronze_drops_rows_with_missing_uid(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            ),
            (
                "",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                None,
                None,
                None,
                "LOT No2",
                1000.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            ),
        ],
        schema=BRONZE_SCHEMA,
    )

    result = clean_decp_bronze(raw_df).collect()

    assert [row.uid for row in result] == ["M1"]


def test_clean_decp_bronze_normalizes_identifiant_case(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "Siret",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_decp_bronze(raw_df).collect()[0]

    assert row.titulaire_type_identifiant == "SIRET"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/transform/test_decp_to_silver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.transform.decp_to_silver'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/transform/decp_to_silver.py`:

```python
"""Bronze-to-silver cleaning and typing of DECP public procurement markets."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def clean_decp_bronze(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("uid").isNotNull()) & (F.col("uid") != ""))
        .select(
            F.col("uid").alias("uid"),
            F.col("acheteur_id").alias("acheteur_id"),
            F.col("acheteur_nom").alias("acheteur_nom"),
            F.col("titulaire_id").alias("titulaire_id"),
            F.upper(F.col("titulaire_typeIdentifiant")).alias("titulaire_type_identifiant"),
            F.col("titulaire_nom").alias("titulaire_nom"),
            F.col("objet").alias("objet"),
            F.col("montant").alias("montant"),
            F.col("codeCPV").alias("code_cpv"),
            F.col("dateNotification").alias("date_notification"),
        )
    )


def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_table: str) -> None:
    raw_df = spark.read.parquet(bronze_path)
    clean_df = clean_decp_bronze(raw_df)
    clean_df.writeTo(silver_table).createOrReplace()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_decp_to_silver.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/decp_to_silver.py tests/transform/test_decp_to_silver.py
git commit -m "feat: clean and type DECP bronze data into silver"
```

---

### Task 2: `bronze_to_silver` writes an Iceberg table

**Files:**
- Modify: `tests/transform/test_decp_to_silver.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/transform/test_decp_to_silver.py`:

```python
def test_bronze_to_silver_writes_cleaned_iceberg_table(spark_session, tmp_path, table_suffix):
    bronze_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    bronze_path = str(tmp_path / "bronze_decp.parquet")
    bronze_df.write.parquet(bronze_path)
    silver_table = f"lakehouse.silver.decp_{table_suffix}"

    bronze_to_silver(spark_session, bronze_path, silver_table)

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
    assert result[0].uid == "M1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_decp_to_silver.py::test_bronze_to_silver_writes_cleaned_iceberg_table -v`
Expected: FAIL with `ImportError: cannot import name 'bronze_to_silver'`

- [ ] **Step 3: Confirm no implementation change is needed**

`bronze_to_silver` from Task 1 already handles this — this step proves it with a
dedicated test, same convention as Step 1/Step 2's equivalent tests.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_decp_to_silver.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/transform/test_decp_to_silver.py
git commit -m "test: cover bronze_to_silver writing an Iceberg table for DECP"
```

---

### Task 3: `resolve_decp_titulaires`

**Files:**
- Create: `src/registry/matching/decp_resolution.py`
- Test: `tests/matching/test_decp_resolution.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/matching/test_decp_resolution.py`:

```python
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from registry.matching.decp_resolution import resolve_decp_titulaires

DECP_SCHEMA = [
    "uid",
    "acheteur_id",
    "acheteur_nom",
    "titulaire_id",
    "titulaire_type_identifiant",
    "titulaire_nom",
    "objet",
    "montant",
    "code_cpv",
]
DECP_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("acheteur_id", StringType()),
        StructField("acheteur_nom", StringType()),
        StructField("titulaire_id", StringType()),
        StructField("titulaire_type_identifiant", StringType()),
        StructField("titulaire_nom", StringType()),
        StructField("objet", StringType()),
        StructField("montant", DoubleType()),
        StructField("code_cpv", StringType()),
    ]
)


def test_resolve_decp_titulaires_trusts_source_siret(spark_session):
    df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
            )
        ],
        schema=DECP_SCHEMA,
    )

    row = resolve_decp_titulaires(df).collect()[0]

    assert row.siret_titulaire == "98236972000015"
    assert row.match_method == "source_siret"


def test_resolve_decp_titulaires_marks_non_siret_as_unresolved(spark_session):
    df = spark_session.createDataFrame(
        [
            (
                "M2",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                "DE119375450",
                "TVA",
                None,
                "LOT No2",
                2000.0,
                "15300000",
            )
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    row = resolve_decp_titulaires(df).collect()[0]

    assert row.siret_titulaire is None
    assert row.match_method == "unresolved"


def test_resolve_decp_titulaires_marks_missing_identifiant_as_unresolved(spark_session):
    df = spark_session.createDataFrame(
        [
            (
                "M3",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                None,
                None,
                None,
                "LOT No3",
                3000.0,
                "15300000",
            )
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    row = resolve_decp_titulaires(df).collect()[0]

    assert row.siret_titulaire is None
    assert row.match_method == "unresolved"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_decp_resolution.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.decp_resolution'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/matching/decp_resolution.py`:

```python
"""Two-branch resolution of DECP titulaires: trust the source's typed SIRET
directly when present, otherwise unresolved. No fuzzy matching stage — see
docs/superpowers/specs/2026-10-08-decp-procurement-design.md for why."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolve_decp_titulaires(decp_df: DataFrame) -> DataFrame:
    is_siret = F.col("titulaire_type_identifiant") == "SIRET"
    return decp_df.select(
        F.col("uid"),
        F.when(is_siret, F.col("titulaire_id"))
        .otherwise(F.lit(None).cast("string"))
        .alias("siret_titulaire"),
        F.when(is_siret, F.lit("source_siret"))
        .otherwise(F.lit("unresolved"))
        .alias("match_method"),
        F.col("acheteur_id"),
        F.col("acheteur_nom"),
        F.col("montant"),
        F.col("objet"),
        F.col("code_cpv"),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_decp_resolution.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_resolution.py tests/matching/test_decp_resolution.py
git commit -m "feat: resolve DECP titulaires via source-typed SIRET, no fuzzy stage"
```

---

### Task 4: `validate_against_sirene`

**Files:**
- Modify: `src/registry/matching/decp_resolution.py`
- Modify: `tests/matching/test_decp_resolution.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/matching/test_decp_resolution.py`:

```python
from registry.matching.decp_resolution import validate_against_sirene

RESOLVED_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("siret_titulaire", StringType()),
        StructField("match_method", StringType()),
    ]
)
SIRENE_GOLD_SCHEMA = ["siret", "is_current"]


def test_validate_against_sirene_marks_known_siret_true(spark_session):
    resolved_df = spark_session.createDataFrame(
        [("M1", "98236972000015", "source_siret")], schema=RESOLVED_SCHEMA_TYPED
    )
    sirene_gold_df = spark_session.createDataFrame(
        [("98236972000015", True)], schema=SIRENE_GOLD_SCHEMA
    )

    row = validate_against_sirene(resolved_df, sirene_gold_df).collect()[0]

    assert row.siret_validated_in_sirene is True


def test_validate_against_sirene_marks_unknown_siret_false(spark_session):
    resolved_df = spark_session.createDataFrame(
        [("M1", "98236972000015", "source_siret")], schema=RESOLVED_SCHEMA_TYPED
    )
    sirene_gold_df = spark_session.createDataFrame(
        [("11111111100011", True)], schema=SIRENE_GOLD_SCHEMA
    )

    row = validate_against_sirene(resolved_df, sirene_gold_df).collect()[0]

    assert row.siret_validated_in_sirene is False


def test_validate_against_sirene_leaves_unresolved_rows_null(spark_session):
    resolved_df = spark_session.createDataFrame(
        [("M2", None, "unresolved")], schema=RESOLVED_SCHEMA_TYPED
    )
    sirene_gold_df = spark_session.createDataFrame(
        [("98236972000015", True)], schema=SIRENE_GOLD_SCHEMA
    )

    row = validate_against_sirene(resolved_df, sirene_gold_df).collect()[0]

    assert row.siret_validated_in_sirene is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_decp_resolution.py -v`
Expected: FAIL with `ImportError: cannot import name 'validate_against_sirene'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/matching/decp_resolution.py`:

```python
def validate_against_sirene(resolved_df: DataFrame, sirene_gold_df: DataFrame) -> DataFrame:
    known_sirets = sirene_gold_df.filter(F.col("is_current")).select(
        F.col("siret").alias("known_siret")
    )
    joined = resolved_df.join(
        known_sirets,
        resolved_df["siret_titulaire"] == known_sirets["known_siret"],
        "left",
    )
    return joined.withColumn(
        "siret_validated_in_sirene",
        F.when(F.col("siret_titulaire").isNotNull(), F.col("known_siret").isNotNull()),
    ).drop("known_siret")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_decp_resolution.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/decp_resolution.py tests/matching/test_decp_resolution.py
git commit -m "feat: validate resolved DECP SIRETs against the SIRENE gold table"
```

---

### Task 5: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass (84 pre-existing + 4 in `test_decp_to_silver.py` + 6 in
`test_decp_resolution.py` = 94).

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

### Task 6: Manual verification — real bronze data, real (if sparse) SIRENE gold

**Files:** none (manual verification only)

No deferral — Plan 1 already landed real department-08 bronze data in Garage.

- [ ] **Step 1: Ensure Garage is running**

```bash
docker compose up -d garage
set -a && source .env && set +a
```

- [ ] **Step 2: Clean the real bronze data into silver**

```bash
uv run python -c "
import os
from registry.transform.decp_to_silver import bronze_to_silver
from registry.transform.spark_session import build_lakehouse_session

bucket = os.environ['LAKEHOUSE_BUCKET']
spark = build_lakehouse_session()
bronze_to_silver(spark, f's3a://{bucket}/bronze/decp/ingestion_date=2026-10-09/marches.parquet', 'lakehouse.silver.decp_marches')
spark.table('lakehouse.silver.decp_marches').show(5)
print('row count:', spark.table('lakehouse.silver.decp_marches').count())
spark.stop()
"
```

Expected: no errors, a row count in the same ballpark as Plan 1's ~1,293 rows
(use whichever `ingestion_date` Plan 1's own manual run actually produced).

- [ ] **Step 3: Resolve and validate against whatever SIRENE gold currently holds**

```bash
uv run python -c "
from registry.matching.decp_resolution import resolve_decp_titulaires, validate_against_sirene
from registry.transform.spark_session import build_lakehouse_session

spark = build_lakehouse_session()
decp_df = spark.table('lakehouse.silver.decp_marches')
resolved_df = resolve_decp_titulaires(decp_df)

sirene_gold_df = spark.table('lakehouse.gold.sirene_etablissements_historized')
validated_df = validate_against_sirene(resolved_df, sirene_gold_df)

validated_df.groupBy('match_method', 'siret_validated_in_sirene').count().show()
spark.stop()
"
```

Expected: a breakdown showing mostly `source_siret` with `siret_validated_in_sirene = false`
(since Step 1's real national SIRENE bootstrap was never actually run in this
environment — see "Decisions made") and a small number of `unresolved` rows with
`siret_validated_in_sirene = null`. Report the real counts honestly, including
the near-zero validation rate — that's the expected, explainable state of this
environment, not a bug in this plan's code.

---

## Self-review notes

- **Spec coverage:** the spec's "Data flow" steps 3-5 (clean to silver, resolve
  via the two-branch check, validate against SIRENE gold) are fully implemented.
  Gold historization (step 6) and the data-quality report are explicitly the
  next plan.
- **Placeholder scan:** none found.
- **Type consistency:** `clean_decp_bronze`'s output column names
  (`titulaire_type_identifiant`, `code_cpv`, `date_notification`) match exactly
  what `resolve_decp_titulaires` reads. `resolve_decp_titulaires`'s output
  columns (`uid`, `siret_titulaire`, `match_method`) match exactly what
  `validate_against_sirene` consumes and extends.
