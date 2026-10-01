# Spark Bronze-to-Gold Transformations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the PySpark/Iceberg transformations that clean the raw SIRENE
bronze data into a typed silver table, then historize it into a gold table using a
full-row SCD2 `MERGE INTO`, so that any change on any tracked field produces a new
versioned record instead of overwriting history.

**Architecture:** Three small, independently testable modules under
`src/registry/transform/`: `bronze_to_silver.py` (cleaning/typing), `silver_to_gold.py`
(the gold table DDL and the SCD2 merge logic), and `spark_session.py` (the production
Spark session factory wired to Garage via the S3A filesystem). Automated tests run
against a local, file-backed Iceberg Hadoop catalog (fast, no network) using a
session-scoped PySpark fixture; a final manual task validates the real path against
the Garage instance from Plan 1, reusing the bronze data it already landed.

**Tech Stack:** PySpark 3.5.3, Apache Iceberg 1.6.1 (`iceberg-spark-runtime-3.5_2.12`),
Hadoop AWS 3.3.4 + AWS SDK bundle 1.12.262 (S3A filesystem, production only),
`pytest`, `ruff`.

This is Plan 2 of 5 for Step 1 (SIRENE lakehouse pipeline) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` for the full design and
`docs/superpowers/plans/2026-10-01-sirene-bootstrap-scaffold.md` (Plan 1) for the
bronze ingestion this plan builds on. Plan 3 (Airflow orchestration + daily diffs)
will call the functions built here from a DAG; that wiring is out of scope.

## Decisions made

- **Silver is a transient, recreated-each-run cleaned staging table**, not itself
  historized — only gold is historized. This avoids tracking history twice. `silver_to_gold`
  always reads the *current* silver table in full and reconciles it against gold.
- **Iceberg catalog: Hadoop catalog** (not Hive or REST) — the simplest option that
  gives ACID tables and time travel on plain object storage, with no extra catalog
  service to run. Good enough for a single local, single-writer pipeline.
- **Spark runs in local mode (`local[*]`)**, not a standalone cluster, for this plan.
  The transformation code uses only the DataFrame/Spark SQL API, so moving to a real
  cluster later (Plan 3, when Airflow submits jobs) is a one-line change to
  `spark_session.py` and does not touch any transformation logic.
- **SCD2 merge is a two-statement pattern**, not a single `MERGE INTO`: one `MERGE`
  closes changed current versions (`is_current = false`, `valid_to` set), then one
  `INSERT` adds new versions (brand-new establishments and just-closed ones — after
  the `MERGE` commits, both cases are simply "no current gold row for this `siret`").
  A single `MERGE INTO` cannot both close an old version and insert a new one for the
  same matched key in one pass.
- **Comparison for "did anything change" uses Spark's null-safe equality (`<=>`)**
  across all tracked (non-key) columns, so a field changing to/from `NULL` is also
  detected as a change.
- **Tests use a local, file-backed Iceberg Hadoop catalog**, not real Garage — fast,
  no network, no credentials. Only the final manual task exercises the real S3A path
  against Garage, reusing the bronze object Plan 1 already landed there.

## Architecture

```
s3a://lakehouse/bronze/sirene/stock/ingestion_date=.../stock.parquet  (Plan 1 output, raw strings)
                    │
          bronze_to_silver (clean_sirene_bronze: filter, type, rename)
                    ▼
     lakehouse.silver.sirene_etablissements   (Iceberg, recreated each run)
                    │
          silver_to_gold (ensure_gold_table + apply_scd2_merge)
                    ▼
lakehouse.gold.sirene_etablissements_historized   (Iceberg, SCD2: valid_from/valid_to/is_current)
```

---

### Task 1: PySpark + Iceberg test fixtures

**Files:**
- Modify: `pyproject.toml`
- Modify: `tests/conftest.py`
- Create: `tests/transform/__init__.py`
- Create: `tests/transform/test_spark_fixture_smoke.py`
- Create: `src/registry/transform/__init__.py`

- [ ] **Step 1: Verify Java is available**

Run: `java -version`
Expected: prints a Java version (PySpark requires a JVM; Java 11 or 17 both work). If
this fails, stop and install a JDK before continuing — nothing in this plan can run
without one.

- [ ] **Step 2: Add PySpark to dependencies**

Edit `pyproject.toml`'s `dependencies` list (in the `[project]` table):

```toml
dependencies = [
    "boto3>=1.34",
    "pyarrow>=16.0",
    "requests>=2.31",
    "pyspark==3.5.3",
]
```

Run: `uv sync --all-groups`
Expected: resolves and installs `pyspark` (and `py4j`), exits 0. This step downloads
~300MB and may take a few minutes.

- [ ] **Step 3: Create the transform package**

`src/registry/transform/__init__.py`:
```python
```

- [ ] **Step 4: Add the Spark/Iceberg test fixtures**

Append to `tests/conftest.py`:

```python
import uuid

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.appName("registry-tests")
        .master("local[1]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def table_suffix() -> str:
    return uuid.uuid4().hex[:8]
```

This downloads the Iceberg runtime jar (via Ivy) the first time the fixture is used —
also a one-time few-minutes delay.

- [ ] **Step 5: Write the failing smoke test**

`tests/transform/__init__.py`:
```python
```

`tests/transform/test_spark_fixture_smoke.py`:
```python
def test_spark_session_fixture_can_create_iceberg_table(spark_session, table_suffix):
    table = f"lakehouse.smoke.test_{table_suffix}"
    spark_session.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
    spark_session.sql(f"INSERT INTO {table} VALUES (1), (2)")

    rows = spark_session.table(table).collect()

    assert sorted(row.id for row in rows) == [1, 2]
```

- [ ] **Step 6: Run the test**

Run: `uv run pytest tests/transform/test_spark_fixture_smoke.py -v`
Expected: `1 passed` (allow a few minutes on first run for jar downloads; subsequent
runs are fast since Ivy caches the jars).

If this fails with a catalog or namespace error, the Hadoop catalog setup needs
adjusting before continuing — stop and investigate rather than proceeding with later
tasks on a broken fixture.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock tests/conftest.py tests/transform/__init__.py tests/transform/test_spark_fixture_smoke.py src/registry/transform/__init__.py
git commit -m "feat: add PySpark and Iceberg test fixtures"
```

---

### Task 2: `clean_sirene_bronze`

**Files:**
- Create: `src/registry/transform/bronze_to_silver.py`
- Test: `tests/transform/test_bronze_to_silver.py`

- [ ] **Step 1: Write the failing tests**

```python
from registry.transform.bronze_to_silver import clean_sirene_bronze

BRONZE_SCHEMA = [
    "siren",
    "nic",
    "siret",
    "statutDiffusionEtablissement",
    "dateCreationEtablissement",
    "etablissementSiege",
    "numeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "activitePrincipaleEtablissement",
    "etatAdministratifEtablissement",
    "dateDernierTraitementEtablissement",
]


def test_clean_sirene_bronze_types_and_renames_columns(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                "1966-01-01",
                "true",
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                "2023-05-12",
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_sirene_bronze(raw_df).collect()[0]

    assert row.siret == "55203253400019"
    assert row.etablissement_siege is True
    assert str(row.date_creation) == "1966-01-01"
    assert row.libelle_commune == "PARIS"


def test_clean_sirene_bronze_drops_rows_with_missing_siret(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                "1966-01-01",
                "true",
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                "2023-05-12",
            ),
            (
                "732829320",
                "00014",
                "",
                "O",
                "1994-03-15",
                "false",
                "12",
                "AV",
                "DES CHAMPS ELYSEES",
                "75008",
                "PARIS",
                "46.19B",
                "A",
                "2022-11-03",
            ),
        ],
        schema=BRONZE_SCHEMA,
    )

    result = clean_sirene_bronze(raw_df).collect()

    assert [row.siret for row in result] == ["55203253400019"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/transform/test_bronze_to_silver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.transform.bronze_to_silver'`

- [ ] **Step 3: Implement `clean_sirene_bronze`**

```python
"""Bronze-to-silver cleaning and typing of the SIRENE establishment stock."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_sirene_bronze(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("siret").isNotNull()) & (F.col("siret") != ""))
        .dropDuplicates(["siret"])
        .select(
            F.col("siren").alias("siren"),
            F.col("nic").alias("nic"),
            F.col("siret").alias("siret"),
            F.col("statutDiffusionEtablissement").alias("statut_diffusion"),
            F.to_date("dateCreationEtablissement", "yyyy-MM-dd").alias("date_creation"),
            (F.col("etablissementSiege") == "true").alias("etablissement_siege"),
            F.col("numeroVoieEtablissement").alias("numero_voie"),
            F.col("typeVoieEtablissement").alias("type_voie"),
            F.col("libelleVoieEtablissement").alias("libelle_voie"),
            F.col("codePostalEtablissement").alias("code_postal"),
            F.col("libelleCommuneEtablissement").alias("libelle_commune"),
            F.col("activitePrincipaleEtablissement").alias("activite_principale"),
            F.col("etatAdministratifEtablissement").alias("etat_administratif"),
            F.to_date("dateDernierTraitementEtablissement", "yyyy-MM-dd").alias(
                "date_dernier_traitement"
            ),
        )
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_bronze_to_silver.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/bronze_to_silver.py tests/transform/test_bronze_to_silver.py
git commit -m "feat: clean and type the SIRENE bronze stock into silver schema"
```

---

### Task 3: `bronze_to_silver`

**Files:**
- Modify: `src/registry/transform/bronze_to_silver.py`
- Test: `tests/transform/test_bronze_to_silver.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/transform/test_bronze_to_silver.py`:

```python
from registry.transform.bronze_to_silver import bronze_to_silver


def test_bronze_to_silver_writes_cleaned_iceberg_table(spark_session, tmp_path, table_suffix):
    bronze_df = spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                "1966-01-01",
                "true",
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                "2023-05-12",
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    bronze_path = str(tmp_path / "bronze_stock.parquet")
    bronze_df.write.parquet(bronze_path)
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"

    bronze_to_silver(spark_session, bronze_path, silver_table)

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
    assert result[0].siret == "55203253400019"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_bronze_to_silver.py -v`
Expected: FAIL with `ImportError: cannot import name 'bronze_to_silver'`

- [ ] **Step 3: Implement `bronze_to_silver`**

Add `from pyspark.sql import SparkSession` to the top of
`src/registry/transform/bronze_to_silver.py`, then append:

```python
def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_table: str) -> None:
    raw_df = spark.read.parquet(bronze_path)
    clean_df = clean_sirene_bronze(raw_df)
    clean_df.writeTo(silver_table).createOrReplace()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_bronze_to_silver.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/bronze_to_silver.py tests/transform/test_bronze_to_silver.py
git commit -m "feat: write cleaned SIRENE silver table from bronze Parquet"
```

---

### Task 4: `ensure_gold_table`

**Files:**
- Create: `src/registry/transform/silver_to_gold.py`
- Test: `tests/transform/test_silver_to_gold.py`

- [ ] **Step 1: Write the failing test**

```python
from registry.transform.silver_to_gold import ensure_gold_table


def test_ensure_gold_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"

    ensure_gold_table(spark_session, gold_table)
    ensure_gold_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.transform.silver_to_gold'`

- [ ] **Step 3: Implement `ensure_gold_table`**

```python
"""Silver-to-gold SCD2 historization for the SIRENE establishment registry."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession


def ensure_gold_table(spark: SparkSession, gold_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            siren STRING,
            nic STRING,
            siret STRING,
            statut_diffusion STRING,
            date_creation DATE,
            etablissement_siege BOOLEAN,
            numero_voie STRING,
            type_voie STRING,
            libelle_voie STRING,
            code_postal STRING,
            libelle_commune STRING,
            activite_principale STRING,
            etat_administratif STRING,
            date_dernier_traitement DATE,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        ) USING iceberg
    """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/silver_to_gold.py tests/transform/test_silver_to_gold.py
git commit -m "feat: add gold table DDL for the historized SIRENE registry"
```

---

### Task 5: `apply_scd2_merge` — initial load

**Files:**
- Modify: `src/registry/transform/silver_to_gold.py`
- Test: `tests/transform/test_silver_to_gold.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/transform/test_silver_to_gold.py`:

```python
import datetime as dt

from registry.transform.silver_to_gold import apply_scd2_merge

SILVER_SCHEMA = [
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
]


def test_apply_scd2_merge_inserts_new_establishments(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    ensure_gold_table(spark_session, gold_table)

    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2023, 5, 12),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()

    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].siret == "55203253400019"
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 1)
    assert rows[0].valid_to is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: FAIL with `ImportError: cannot import name 'apply_scd2_merge'`

- [ ] **Step 3: Implement `apply_scd2_merge`**

Append to `src/registry/transform/silver_to_gold.py`:

```python
TRACKED_COLUMNS = [
    "nic",
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
]


def apply_scd2_merge(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    spark.table(silver_table).createOrReplaceTempView("incoming")

    comparison = " AND ".join(f"incoming.{c} <=> gold.{c}" for c in TRACKED_COLUMNS)

    changed_sirets = spark.sql(f"""
        SELECT incoming.siret
        FROM incoming
        JOIN {gold_table} AS gold
          ON incoming.siret = gold.siret AND gold.is_current = true
        WHERE NOT ({comparison})
    """)
    changed_sirets.createOrReplaceTempView("changed_sirets")

    spark.sql(f"""
        MERGE INTO {gold_table} AS gold
        USING changed_sirets AS c
        ON gold.siret = c.siret AND gold.is_current = true
        WHEN MATCHED THEN UPDATE SET
            gold.valid_to = DATE('{run_date.isoformat()}'),
            gold.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT
            incoming.siren, incoming.nic, incoming.siret, incoming.statut_diffusion,
            incoming.date_creation, incoming.etablissement_siege, incoming.numero_voie,
            incoming.type_voie, incoming.libelle_voie, incoming.code_postal,
            incoming.libelle_commune, incoming.activite_principale,
            incoming.etat_administratif, incoming.date_dernier_traitement,
            DATE('{run_date.isoformat()}') AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            true AS is_current
        FROM incoming
        LEFT JOIN {gold_table} AS gold
          ON incoming.siret = gold.siret AND gold.is_current = true
        WHERE gold.siret IS NULL
    """)
```

(`changed_sirets` is used by the `MERGE`; by the time the final `INSERT` runs, the
`MERGE` has already committed, so any establishment that changed no longer has a
current row in `gold_table` — the `WHERE gold.siret IS NULL` check on the
*post-merge* state already covers both brand-new and just-changed establishments
without needing to reference `changed_sirets` again.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/silver_to_gold.py tests/transform/test_silver_to_gold.py
git commit -m "feat: insert new establishments into the gold SCD2 table"
```

---

### Task 6: `apply_scd2_merge` — changed establishment is versioned

**Files:**
- Test: `tests/transform/test_silver_to_gold.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/transform/test_silver_to_gold.py`:

```python
def test_apply_scd2_merge_closes_and_versions_changed_establishment(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    ensure_gold_table(spark_session, gold_table)

    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2023, 5, 12),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "10",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2026, 9, 30),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 2))

    rows = spark_session.table(gold_table).orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].numero_voie == "8"
    assert rows[0].is_current is False
    assert rows[0].valid_to == dt.date(2026, 10, 2)
    assert rows[1].numero_voie == "10"
    assert rows[1].is_current is True
    assert rows[1].valid_to is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v -k changed_establishment`
Expected: FAIL — either an assertion error (if 2 rows aren't produced correctly) or it
unexpectedly passes already. If it fails, inspect the row count/values printed by
pytest's assertion diff to see which part of the merge logic is wrong before fixing
`apply_scd2_merge`.

- [ ] **Step 3: Fix `apply_scd2_merge` if needed**

If Step 2 failed, adjust the implementation from Task 5 until the logic matches: a
changed establishment must end up with exactly two gold rows — the old one closed
(`is_current = false`, `valid_to` = the new run's date) and a new one open
(`is_current = true`, `valid_to` = `NULL`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add tests/transform/test_silver_to_gold.py
git commit -m "test: verify SCD2 merge versions a changed establishment"
```

---

### Task 7: `apply_scd2_merge` — unchanged establishment is not re-versioned

**Files:**
- Test: `tests/transform/test_silver_to_gold.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/transform/test_silver_to_gold.py`:

```python
def test_apply_scd2_merge_does_not_version_unchanged_establishment(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    ensure_gold_table(spark_session, gold_table)
    row = (
        "552032534",
        "00019",
        "55203253400019",
        "O",
        dt.date(1966, 1, 1),
        True,
        "8",
        "RUE",
        "DE LA PAIX",
        "75002",
        "PARIS",
        "70.10Z",
        "A",
        dt.date(2023, 5, 12),
    )

    spark_session.createDataFrame([row], schema=SILVER_SCHEMA).writeTo(
        silver_table
    ).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    spark_session.createDataFrame([row], schema=SILVER_SCHEMA).writeTo(
        silver_table
    ).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 2))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 1)
```

This is the negative-case counterpart to Task 6: re-running the merge with identical
data must not create a spurious second version.

- [ ] **Step 2: Run test to verify it fails or passes for the right reason**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v -k unchanged_establishment`
Expected: `1 passed`. If the earlier implementation is correct, this should already
pass — it is still run and committed as an explicit regression guard.

- [ ] **Step 3: Run the full test file**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: `4 passed`

- [ ] **Step 4: Commit**

```bash
git add tests/transform/test_silver_to_gold.py
git commit -m "test: verify SCD2 merge does not re-version unchanged establishments"
```

---

### Task 8: `silver_to_gold` orchestration

**Files:**
- Modify: `src/registry/transform/silver_to_gold.py`
- Test: `tests/transform/test_silver_to_gold.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/transform/test_silver_to_gold.py`:

```python
from registry.transform.silver_to_gold import silver_to_gold


def test_silver_to_gold_creates_table_and_merges(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2023, 5, 12),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()

    silver_to_gold(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v -k creates_table_and_merges`
Expected: FAIL with `ImportError: cannot import name 'silver_to_gold'`

- [ ] **Step 3: Implement `silver_to_gold`**

Append to `src/registry/transform/silver_to_gold.py`:

```python
def silver_to_gold(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    ensure_gold_table(spark, gold_table)
    apply_scd2_merge(spark, silver_table, gold_table, run_date)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_silver_to_gold.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/silver_to_gold.py tests/transform/test_silver_to_gold.py
git commit -m "feat: orchestrate gold table creation and SCD2 merge"
```

---

### Task 9: Production Spark session factory (`spark_session.py`)

**Files:**
- Create: `src/registry/transform/spark_session.py`
- Test: `tests/transform/test_spark_session.py`

**Note:** `lakehouse_spark_configs` returns a plain `dict` instead of being folded
directly into `build_lakehouse_session` so it can be unit-tested without creating a
second `SparkSession` — PySpark only allows one active session per JVM process, and
the test suite already has one running via the session-scoped `spark_session`
fixture; calling `SparkSession.builder.getOrCreate()` again would silently return
that existing session instead of applying new config.

- [ ] **Step 1: Write the failing test**

```python
from registry.transform.spark_session import lakehouse_spark_configs


def test_lakehouse_spark_configs_sets_s3a_and_iceberg_catalog(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lakehouse")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:3900")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-key")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")
    monkeypatch.setenv("S3_REGION", "garage")

    configs = lakehouse_spark_configs()

    assert configs["spark.sql.catalog.lakehouse.warehouse"] == "s3a://lakehouse/warehouse"
    assert configs["spark.hadoop.fs.s3a.endpoint"] == "http://localhost:3900"
    assert configs["spark.hadoop.fs.s3a.endpoint.region"] == "garage"
    assert configs["spark.hadoop.fs.s3a.access.key"] == "test-key"
    assert configs["spark.hadoop.fs.s3a.secret.key"] == "test-secret"
```

**Note (post-implementation):** the manual verification in Task 11 initially failed
with `AWSBadRequestException: Authorization header malformed, unexpected scope:
.../us-east-1/s3/aws4_request`. The S3A connector was signing requests with the
default `us-east-1` region while Garage is configured with `s3_region = "garage"`
(in `garage.toml`) — SigV4 signatures are region-bound, so a mismatch is rejected.
Fixed by explicitly setting `spark.hadoop.fs.s3a.endpoint.region` from `S3_REGION`
(Hadoop 3.3.2+ added this property specifically for non-AWS S3-compatible endpoints,
where the SDK cannot infer the signing region from the hostname).

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_spark_session.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.transform.spark_session'`

- [ ] **Step 3: Implement `spark_session.py`**

```python
"""Production SparkSession factory wired to the Garage/S3A-backed Iceberg lakehouse."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"


def lakehouse_spark_configs() -> dict[str, str]:
    bucket = os.environ["LAKEHOUSE_BUCKET"]
    return {
        "spark.jars.packages": f"{ICEBERG_PACKAGE},{HADOOP_AWS_PACKAGE},{AWS_SDK_PACKAGE}",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakehouse.type": "hadoop",
        "spark.sql.catalog.lakehouse.warehouse": f"s3a://{bucket}/warehouse",
        "spark.hadoop.fs.s3a.endpoint": os.environ["S3_ENDPOINT_URL"],
        "spark.hadoop.fs.s3a.endpoint.region": os.environ.get("S3_REGION", "us-east-1"),
        "spark.hadoop.fs.s3a.access.key": os.environ["S3_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["S3_SECRET_KEY"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }


def build_lakehouse_session(app_name: str = "registry") -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in lakehouse_spark_configs().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/transform/test_spark_session.py -v`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/spark_session.py tests/transform/test_spark_session.py
git commit -m "feat: add production Spark session factory for the Garage lakehouse"
```

---

### Task 10: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass — 7 from Plan 1 plus 10 new ones from this plan (1 smoke +
2 + 1 + 1 + 1 + 1 + 1 + 1 + 1 across Tasks 1-9) = 17 total.

- [ ] **Step 2: Run lint and format check**

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: `All checks passed!` and no files needing reformatting. If formatting is
needed, run `uv run ruff format .` and commit with `git add -u && git commit -m
"style: apply ruff formatting"`.

---

### Task 11: Manual end-to-end verification against real Garage

**Files:** none (manual verification only)

This reuses the bronze object Plan 1's manual verification already landed in Garage
(`bronze/sirene/stock/ingestion_date=<date>/stock.parquet`), so no new bootstrap run
is needed — just find the date it used.

- [ ] **Step 1: Ensure Garage is running and find the bronze object's date**

```bash
docker compose up -d garage
set -a && source .env && set +a
uv run python -c "
import boto3, os
client = boto3.client(
    's3',
    endpoint_url=os.environ['S3_ENDPOINT_URL'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY'],
    region_name=os.environ['S3_REGION'],
)
for obj in client.list_objects_v2(Bucket='lakehouse', Prefix='bronze/sirene/stock/')['Contents']:
    print(obj['Key'])
"
```

Expected: prints one key of the form
`bronze/sirene/stock/ingestion_date=<date>/stock.parquet`. Use that exact `<date>`
in the next step.

- [ ] **Step 2: Run bronze_to_silver and silver_to_gold against real Garage**

```bash
uv run python -c "
from registry.transform.spark_session import build_lakehouse_session
from registry.transform.bronze_to_silver import bronze_to_silver
from registry.transform.silver_to_gold import silver_to_gold
import datetime as dt

spark = build_lakehouse_session()
bronze_path = 's3a://lakehouse/bronze/sirene/stock/ingestion_date=<date>/stock.parquet'
silver_table = 'lakehouse.silver.sirene_etablissements'
gold_table = 'lakehouse.gold.sirene_etablissements_historized'

bronze_to_silver(spark, bronze_path, silver_table)
silver_to_gold(spark, silver_table, gold_table, dt.date.today())

spark.table(gold_table).show(truncate=False)
spark.stop()
"
```

(Replace `<date>` with the value found in Step 1.)

Expected: prints 2 rows (the fixture has 2 establishments), both with
`is_current=true` and `valid_to=NULL`.

- [ ] **Step 3: Simulate a change and verify historization**

```bash
uv run python -c "
from registry.transform.spark_session import build_lakehouse_session
from registry.transform.silver_to_gold import silver_to_gold
import datetime as dt

spark = build_lakehouse_session()
silver_table = 'lakehouse.silver.sirene_etablissements'
gold_table = 'lakehouse.gold.sirene_etablissements_historized'

spark.sql(f\"\"\"
    UPDATE {silver_table}
    SET numero_voie = '99'
    WHERE siret = '55203253400019'
\"\"\")
silver_to_gold(spark, silver_table, gold_table, dt.date.today() + dt.timedelta(days=1))

spark.table(gold_table).orderBy('siret', 'valid_from').show(truncate=False)
spark.stop()
"
```

Expected: 3 rows total — the unchanged establishment still has 1 current row, and
the modified establishment (`siret='55203253400019'`) now has 2 rows: one closed
(`is_current=false`, `numero_voie='8'`) and one current (`is_current=true`,
`numero_voie='99'`).

If this step fails with a jar resolution or S3A connection error, check that the
pinned Iceberg/Hadoop-AWS/AWS-SDK versions in `spark_session.py` are compatible with
the installed PySpark's bundled Hadoop version
(`uv run python -c "import pyspark; print(pyspark.__version__)"` and check PySpark's
release notes for its bundled Hadoop version) and adjust the pinned coordinates if
not — this is the same kind of environment drift Plan 1 hit with the MinIO image.
