# BODACC Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land BODACC legal announcements into the lakehouse — bronze through
silver — so Plan 2 of Step 2 (entity resolution) has clean, typed announcement
data to match against the SIRENE gold table.

**Architecture:** One ingestion module (`registry.ingestion.bodacc`) handles both
the initial 12-month bootstrap and the daily diffs, because — unlike SIRENE, whose
bootstrap (bulk CSV) and diffs (REST API) are genuinely different sources — BODACC
bootstrap and diffs are the *same* Opendatasoft API, just called with a wider or
narrower date window. A single `run_ingestion(since, until, run_type)` function
serves both. A new `registry.transform.bodacc_to_silver` module mirrors Plan 2's
`bronze_to_silver` pattern exactly: silver is recreated fresh each run (the batch
this run is responsible for), to be consumed and historized by the entity
resolution step Plan 2 of Step 2 will add on top.

**Tech Stack:** `requests`, `pyarrow`, `boto3` (all already project dependencies —
no new ones needed for this plan), PySpark (silver transformation, reusing Step 1's
local-Iceberg-catalog pytest fixture).

This is Plan 1 of Step 2 (BODACC entity resolution) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md` for the full
design. Step 2's Plan 2 (Splink-based entity resolution against the SIRENE gold
table) and Plan 3 (orchestration) build on this plan's output and are out of scope
here.

## Decisions made

- **One ingestion module for bootstrap and diff**, not two (contrast with Step 1's
  separate `sirene_bootstrap.py`/`sirene_diff.py`, which genuinely needed to differ
  since one reads a bulk CSV and the other calls a REST API). BODACC bootstrap and
  diff both call the same Opendatasoft endpoint with different `since`/`until`
  bounds — a `run_type` parameter (`"bootstrap"` or `"diff"`) only affects the
  bronze object key, not the fetch logic.
- **SIREN extraction tries a direct `siren` field first, falling back to parsing
  the free-text `registre` field** (e.g. `"334 393 806 RCS PARIS"`) via regex. This
  hedges against genuine uncertainty about which field the live API actually
  populates reliably — this project's INSEE Sirene API integration (Step 1, Plan 3)
  hit exactly this kind of "my best-confidence reconstruction of the schema turned
  out subtly wrong" problem, so Task 9's manual verification explicitly checks the
  real response shape before trusting this logic on real data.
- **The Opendatasoft endpoint, dataset slug, and field names below are this plan's
  best-confidence reconstruction of the public BODACC dataset, not verified against
  a live call** — this environment has not exercised the real API yet. Task 9
  flags exactly where to check and adjust, the same honest-caveat pattern Step 1
  used for the SIRENE stock URL and Sirene API shape.
- **Silver (`lakehouse.silver.bodacc_annonces`) is recreated fresh each run**, same
  semantics as Plan 2 of Step 1's SIRENE silver table: it represents only the batch
  this run fetched (the full 12-month bootstrap, or one day's diff), not an
  accumulating history. The entity-resolution plan that consumes it is responsible
  for historizing results into gold — silver itself stays transient by design.
- **Pagination uses offset/limit** (Opendatasoft's documented pagination
  mechanism), unlike the Sirene API's cursor-based pagination (Step 1, Plan 3) —
  these are two different upstream APIs with two different, equally legitimate
  pagination conventions.

## Architecture

```
BODACC Opendatasoft API (where=dateparution range, limit/offset pagination)
        │
        │  run_ingestion(bucket, since, until, work_dir, run_type)
        ▼
bronze/bodacc/{run_type}/ingestion_date=.../bodacc.parquet   (run_type: bootstrap | diff)
        │
        │  bronze_to_silver (Spark, mirrors Plan 2's pattern)
        ▼
lakehouse.silver.bodacc_annonces   (recreated fresh each run)
```

---

### Task 1: `extract_siren_from_registre`

**Files:**
- Create: `src/registry/ingestion/bodacc.py`
- Test: `tests/ingestion/test_bodacc.py`

- [ ] **Step 1: Write the failing tests**

```python
from registry.ingestion.bodacc import extract_siren_from_registre


def test_extract_siren_from_registre_with_spaces():
    assert extract_siren_from_registre("334 393 806 RCS PARIS") == "334393806"


def test_extract_siren_from_registre_without_spaces():
    assert extract_siren_from_registre("334393806 RCS PARIS") == "334393806"


def test_extract_siren_from_registre_returns_none_when_absent():
    assert extract_siren_from_registre("RCS PARIS") is None


def test_extract_siren_from_registre_returns_none_for_short_number():
    assert extract_siren_from_registre("12 RCS PARIS 2024") is None


def test_extract_siren_from_registre_returns_none_for_none_input():
    assert extract_siren_from_registre(None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.ingestion.bodacc'`

- [ ] **Step 3: Implement `extract_siren_from_registre`**

```python
"""Ingestion of BODACC legal announcements (bootstrap and daily diffs)."""

from __future__ import annotations

import re

SIREN_IN_TEXT_PATTERN = re.compile(r"\b(\d[\d ]{0,11}\d)\b")


def extract_siren_from_registre(registre: str | None) -> str | None:
    if not registre:
        return None
    match = SIREN_IN_TEXT_PATTERN.search(registre)
    if not match:
        return None
    digits = match.group(1).replace(" ", "")
    if len(digits) != 9:
        return None
    return digits
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/bodacc.py tests/ingestion/test_bodacc.py
git commit -m "feat: extract SIREN from BODACC registre free text"
```

---

### Task 2: `normalize_announcement`

**Files:**
- Modify: `src/registry/ingestion/bodacc.py`
- Test: `tests/ingestion/test_bodacc.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/ingestion/test_bodacc.py`:

```python
from registry.ingestion.bodacc import normalize_announcement


def test_normalize_announcement_falls_back_to_registre_parsing():
    raw = {
        "id": "BX202500012345",
        "dateparution": "2025-10-15",
        "numeroannonce": "12345",
        "typeavis_lib": "Jugement",
        "familleavis_lib": "Procedures collectives",
        "tribunal": "Tribunal de commerce de Paris",
        "commercant": "DUPONT BATIMENT SARL",
        "siren": None,
        "registre": "334 393 806 RCS PARIS",
        "ville": "PARIS",
        "cp": "75002",
    }

    result = normalize_announcement(raw)

    assert result["id"] == "BX202500012345"
    assert result["siren_declared"] == "334393806"
    assert result["denomination"] is None  # not set on the raw dict in this test


def test_normalize_announcement_prefers_direct_siren_field():
    raw = {
        "id": "BX202500012346",
        "dateparution": "2025-10-16",
        "numeroannonce": "12346",
        "typeavis_lib": "Jugement",
        "familleavis_lib": "Procedures collectives",
        "tribunal": "Tribunal de commerce de Lyon",
        "commercant": "MARTIN TRAVAUX SARL",
        "siren": "552032534",
        "registre": "999999999 RCS LYON",
        "ville": "LYON",
        "cp": "69001",
    }

    result = normalize_announcement(raw)

    assert result["siren_declared"] == "552032534"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: FAIL with `ImportError: cannot import name 'normalize_announcement'`

- [ ] **Step 3: Implement `normalize_announcement`**

Append to `src/registry/ingestion/bodacc.py`:

```python
BODACC_COLUMNS = [
    "id",
    "dateparution",
    "numeroannonce",
    "typeavis_lib",
    "familleavis_lib",
    "tribunal",
    "commercant",
    "siren_declared",
    "ville",
    "cp",
    "denomination",
]


def normalize_announcement(raw: dict) -> dict:
    siren = raw.get("siren") or extract_siren_from_registre(raw.get("registre"))
    return {
        "id": raw.get("id"),
        "dateparution": raw.get("dateparution"),
        "numeroannonce": raw.get("numeroannonce"),
        "typeavis_lib": raw.get("typeavis_lib"),
        "familleavis_lib": raw.get("familleavis_lib"),
        "tribunal": raw.get("tribunal"),
        "commercant": raw.get("commercant"),
        "siren_declared": siren,
        "ville": raw.get("ville"),
        "cp": raw.get("cp"),
        "denomination": raw.get("denomination"),
    }
```

Note: `denomination` is read straight through here (distinct from `commercant`,
which is the free-text trade name) — both are carried into silver because Plan 2 of
Step 2 (entity resolution) will need whichever one is populated for name matching;
this plan doesn't decide between them.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/bodacc.py tests/ingestion/test_bodacc.py
git commit -m "feat: normalize BODACC announcements to a flat bronze schema"
```

---

### Task 3: `fetch_bodacc_announcements`

**Files:**
- Modify: `src/registry/ingestion/bodacc.py`
- Test: `tests/ingestion/test_bodacc.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/ingestion/test_bodacc.py`:

```python
import datetime as dt

import responses

from registry.ingestion.bodacc import BODACC_API_URL, fetch_bodacc_announcements


def _raw_announcement(id_: str, siren: str) -> dict:
    return {
        "id": id_,
        "dateparution": "2025-10-15",
        "numeroannonce": "1",
        "typeavis_lib": "Jugement",
        "familleavis_lib": "Procedures collectives",
        "tribunal": "Tribunal de commerce de Paris",
        "commercant": "DUPONT BATIMENT SARL",
        "siren": siren,
        "registre": None,
        "ville": "PARIS",
        "cp": "75002",
        "denomination": None,
    }


@responses.activate
def test_fetch_bodacc_announcements_follows_offset_pagination():
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": [_raw_announcement("A", "111111111")]},
        status=200,
    )
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": [_raw_announcement("B", "222222222")]},
        status=200,
    )
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": []},
        status=200,
    )

    records = fetch_bodacc_announcements(dt.date(2025, 10, 1), dt.date(2025, 10, 16), page_size=1)

    assert [r["id"] for r in records] == ["A", "B"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: FAIL with `ImportError: cannot import name 'fetch_bodacc_announcements'`

- [ ] **Step 3: Implement `fetch_bodacc_announcements`**

Add `import requests` to the top of `src/registry/ingestion/bodacc.py`, then
append:

```python
BODACC_API_URL = (
    "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "annonces-commerciales/records"
)


def fetch_bodacc_announcements(since: dt.date, until: dt.date, page_size: int = 100) -> list[dict]:
    records: list[dict] = []
    offset = 0
    where_clause = (
        f"dateparution >= date'{since.isoformat()}' AND dateparution < date'{until.isoformat()}'"
    )

    while True:
        response = requests.get(
            BODACC_API_URL,
            params={"where": where_clause, "limit": page_size, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        records.extend(normalize_announcement(r) for r in results)

        if len(results) < page_size:
            break
        offset += page_size

    return records
```

Also add `import datetime as dt` to the top of the file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: `8 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/bodacc.py tests/ingestion/test_bodacc.py
git commit -m "feat: fetch BODACC announcements with offset pagination"
```

---

### Task 4: `bronze_object_key` and `write_announcements_to_bronze`

**Files:**
- Modify: `src/registry/ingestion/bodacc.py`
- Test: `tests/ingestion/test_bodacc.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/ingestion/test_bodacc.py`:

```python
import io

import boto3
import pyarrow.parquet as pq
from moto import mock_aws

from registry.ingestion.bodacc import bronze_object_key, write_announcements_to_bronze


def test_bronze_object_key_formats_run_type_and_date():
    key = bronze_object_key(dt.date(2026, 10, 3), "bootstrap")

    assert key == "bronze/bodacc/bootstrap/ingestion_date=2026-10-03/bodacc.parquet"


@mock_aws
def test_write_announcements_to_bronze_uploads_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.ingestion.bodacc.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    records = [normalize_announcement(_raw_announcement("A", "111111111"))]

    key = write_announcements_to_bronze(
        records, bucket="lakehouse", work_dir=tmp_path, run_type="diff"
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
    assert table.column("id").to_pylist() == ["A"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: FAIL with `ImportError: cannot import name 'bronze_object_key'`

- [ ] **Step 3: Implement `bronze_object_key` and `write_announcements_to_bronze`**

Add to the top of `src/registry/ingestion/bodacc.py`:

```python
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from registry.ingestion.storage import get_s3_client, upload_file
```

Append:

```python
def bronze_object_key(ingestion_date: dt.date, run_type: str) -> str:
    return f"bronze/bodacc/{run_type}/ingestion_date={ingestion_date.isoformat()}/bodacc.parquet"


def write_announcements_to_bronze(
    records: list[dict], bucket: str, work_dir: Path, run_type: str
) -> str:
    table = pa.Table.from_pylist(
        records, schema=pa.schema([(name, pa.string()) for name in BODACC_COLUMNS])
    )
    parquet_path = work_dir / "bodacc.parquet"
    pq.write_table(table, parquet_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today(), run_type)
    upload_file(client, parquet_path, bucket, key)
    return key
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: `10 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/bodacc.py tests/ingestion/test_bodacc.py
git commit -m "feat: write BODACC announcements to bronze as Parquet"
```

---

### Task 5: `run_ingestion`

**Files:**
- Modify: `src/registry/ingestion/bodacc.py`
- Test: `tests/ingestion/test_bodacc.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/ingestion/test_bodacc.py`:

```python
from registry.ingestion.bodacc import run_ingestion


@responses.activate
@mock_aws
def test_run_ingestion_fetches_and_uploads_to_bronze(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.ingestion.bodacc.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": [_raw_announcement("A", "111111111")]},
        status=200,
    )

    key = run_ingestion(
        bucket="lakehouse",
        since=dt.date(2025, 10, 1),
        until=dt.date(2025, 10, 16),
        work_dir=tmp_path,
        run_type="bootstrap",
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
    assert "bronze/bodacc/bootstrap/" in key
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: FAIL with `ImportError: cannot import name 'run_ingestion'`

- [ ] **Step 3: Implement `run_ingestion`**

Append to `src/registry/ingestion/bodacc.py`:

```python
def run_ingestion(
    bucket: str, since: dt.date, until: dt.date, work_dir: Path, run_type: str
) -> str:
    records = fetch_bodacc_announcements(since, until)
    return write_announcements_to_bronze(records, bucket, work_dir, run_type)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_bodacc.py -v`
Expected: `11 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/bodacc.py tests/ingestion/test_bodacc.py
git commit -m "feat: orchestrate BODACC bootstrap and diff ingestion"
```

---

### Task 6: `clean_bodacc_bronze`

**Files:**
- Create: `src/registry/transform/bodacc_to_silver.py`
- Test: `tests/transform/test_bodacc_to_silver.py`

- [ ] **Step 1: Write the failing tests**

```python
from pyspark.sql.types import StringType, StructField, StructType

from registry.transform.bodacc_to_silver import clean_bodacc_bronze

BRONZE_COLUMN_NAMES = [
    "id",
    "dateparution",
    "numeroannonce",
    "typeavis_lib",
    "familleavis_lib",
    "tribunal",
    "commercant",
    "siren_declared",
    "ville",
    "cp",
    "denomination",
]

BRONZE_SCHEMA = StructType([StructField(name, StringType()) for name in BRONZE_COLUMN_NAMES])


def test_clean_bodacc_bronze_types_and_renames_columns(spark_session):
    raw_df = spark_session.createDataFrame(
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

    row = clean_bodacc_bronze(raw_df).collect()[0]

    assert row.id == "BX202500012345"
    assert str(row.date_parution) == "2025-10-15"
    assert row.siren_declared == "334393806"
    assert row.famille_avis == "Procedures collectives"


def test_clean_bodacc_bronze_drops_rows_with_missing_id(spark_session):
    raw_df = spark_session.createDataFrame(
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
            ),
            (
                "",
                "2025-10-16",
                "12346",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Lyon",
                "MARTIN TRAVAUX SARL",
                None,
                "LYON",
                "69001",
                None,
            ),
        ],
        schema=BRONZE_SCHEMA,
    )

    result = clean_bodacc_bronze(raw_df).collect()

    assert [row.id for row in result] == ["BX202500012345"]
```

**Note (post-implementation):** the fixture originally used a plain list of column
names as the schema. That fails with `PySparkValueError: [CANNOT_DETERMINE_TYPE]`
because `denomination` is `None` in every row — Spark can't infer a type from an
all-`None` sample column (the same issue Plan 4's seed script hit). Fixed by using
an explicit all-`StringType` `StructType` instead, matching bronze's actual
string-typed convention (shown above).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.transform.bodacc_to_silver'`

- [ ] **Step 3: Implement `clean_bodacc_bronze`**

```python
"""Bronze-to-silver cleaning and typing of BODACC legal announcements."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_bodacc_bronze(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("id").isNotNull()) & (F.col("id") != ""))
        .dropDuplicates(["id"])
        .select(
            F.col("id").alias("id"),
            F.to_date("dateparution", "yyyy-MM-dd").alias("date_parution"),
            F.col("numeroannonce").alias("numero_annonce"),
            F.col("typeavis_lib").alias("type_avis"),
            F.col("familleavis_lib").alias("famille_avis"),
            F.col("tribunal").alias("tribunal"),
            F.col("commercant").alias("commercant"),
            F.col("denomination").alias("denomination"),
            F.col("siren_declared").alias("siren_declared"),
            F.col("ville").alias("ville"),
            F.col("cp").alias("code_postal"),
        )
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/bodacc_to_silver.py tests/transform/test_bodacc_to_silver.py
git commit -m "feat: clean and type BODACC announcements into silver schema"
```

---

### Task 7: `bronze_to_silver`

**Files:**
- Modify: `src/registry/transform/bodacc_to_silver.py`
- Test: `tests/transform/test_bodacc_to_silver.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/transform/test_bodacc_to_silver.py`:

```python
from registry.transform.bodacc_to_silver import bronze_to_silver


def test_bronze_to_silver_writes_cleaned_iceberg_table(spark_session, tmp_path, table_suffix):
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

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
    assert result[0].id == "BX202500012345"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py -v`
Expected: FAIL with `ImportError: cannot import name 'bronze_to_silver'`

- [ ] **Step 3: Implement `bronze_to_silver`**

Add `from pyspark.sql import SparkSession` to the top of
`src/registry/transform/bodacc_to_silver.py`, then append:

```python
def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_table: str) -> None:
    raw_df = spark.read.parquet(bronze_path)
    clean_df = clean_bodacc_bronze(raw_df)
    clean_df.writeTo(silver_table).createOrReplace()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/transform/test_bodacc_to_silver.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/transform/bodacc_to_silver.py tests/transform/test_bodacc_to_silver.py
git commit -m "feat: write cleaned BODACC silver table from bronze Parquet"
```

---

### Task 8: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite and lint**

```bash
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Expected: all 42 tests pass (28 from Step 1 plus 11 from Tasks 1-5 plus 3 from
Tasks 6-7), lint and format clean.

---

### Task 9: Manual verification against the real BODACC API and Garage

**Files:** none (manual verification only)

- [ ] **Step 1: Sanity-check the real API shape before trusting the ingestion code**

```bash
curl -s "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records?where=dateparution%20%3E%3D%20date%272026-09-01%27&limit=1" | python3 -m json.tool
```

Compare the printed JSON against what `normalize_announcement` (Task 2) expects
(`id`, `dateparution`, `numeroannonce`, `typeavis_lib`, `familleavis_lib`,
`tribunal`, `commercant`, `siren`, `registre`, `ville`, `cp`, `denomination`). If
the dataset slug, endpoint version, or field names differ, update `BODACC_API_URL`
and/or `normalize_announcement` before proceeding — this was flagged as unverified
in "Decisions made."

- [ ] **Step 2: Run the real 12-month bootstrap**

```bash
docker compose up -d garage
set -a && source .env && set +a
uv run python -c "
import datetime as dt
from registry.ingestion.bodacc import run_ingestion
from pathlib import Path

today = dt.date.today()
key = run_ingestion(
    bucket='lakehouse',
    since=today.replace(year=today.year - 1),
    until=today,
    work_dir=Path('/tmp'),
    run_type='bootstrap',
)
print(key)
"
```

Expected: prints a key of the form
`bronze/bodacc/bootstrap/ingestion_date=<today>/bodacc.parquet`. Note how many
announcements this fetched (printed by `fetch_bodacc_announcements` if you add a
temporary `print(len(records))`, or check the Parquet file's row count directly)
to sanity-check the volume is plausible for 12 months of a department/national
feed, not suspiciously tiny or huge.

- [ ] **Step 3: Run bronze_to_silver against the real bootstrap output**

```bash
uv run python -c "
from registry.transform.spark_session import build_lakehouse_session
from registry.transform.bodacc_to_silver import bronze_to_silver
import datetime as dt

spark = build_lakehouse_session()
bronze_path = f's3a://lakehouse/bronze/bodacc/bootstrap/ingestion_date={dt.date.today().isoformat()}/bodacc.parquet'
bronze_to_silver(spark, bronze_path, 'lakehouse.silver.bodacc_annonces')
spark.table('lakehouse.silver.bodacc_annonces').show(5, truncate=False)
print('Row count:', spark.table('lakehouse.silver.bodacc_annonces').count())
spark.stop()
"
```

Expected: prints a handful of representative rows and a row count matching Step 2.

- [ ] **Step 4: Run a one-day diff-style call to confirm the shared code path works for both run types**

```bash
uv run python -c "
import datetime as dt
from pathlib import Path
from registry.ingestion.bodacc import run_ingestion

today = dt.date.today()
key = run_ingestion(
    bucket='lakehouse',
    since=today - dt.timedelta(days=1),
    until=today,
    work_dir=Path('/tmp'),
    run_type='diff',
)
print(key)
"
```

Expected: prints a key of the form
`bronze/bodacc/diff/ingestion_date=<today>/bodacc.parquet` — same function,
different `run_type` and a one-day window instead of a one-year one.
