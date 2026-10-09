# DECP Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Download the national DECP consolidated file, filter it locally to this project's scope (department 08 buyers, last 12 months, current market version only), and land the result in bronze.

**Architecture:** Unlike BODACC's incremental Opendatasoft API, DECP is published as a single full-replacement national Parquet file (~236 MB, verified by direct download before writing the spec) — there's no `since`/`until` endpoint to call. Every run downloads the current file and filters it locally with PyArrow; there's no bootstrap/diff distinction to build.

**Tech Stack:** `requests` (download), PyArrow (read + filter + write Parquet) — no new dependencies; same `get_s3_client`/`upload_file` helpers Steps 1-2 already use.

This is Plan 1 of Step 3 (DECP public procurement linkage) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-08-decp-procurement-design.md` for the full
design. Resolution, gold historization, orchestration, dbt tests, and serving
are each their own plan, written and executed one at a time — out of scope here.

## Decisions made

- **No bootstrap/diff split.** BODACC's ingestion had two modes because its API
  supports genuine incremental date-range queries. DECP's source is a single
  file that gets fully replaced upstream; "ingest" always means "download the
  current file, filter to scope, land it" — there's nothing to diff against.
- **Filtering happens locally with PyArrow, not server-side.** data.gouv.fr has
  no query API for this resource (confirmed while writing the spec — only a
  bulk file download). At ~236 MB, downloading the whole file and filtering
  in-memory is simple and fast enough that building anything cleverer (e.g.
  chasing data.gouv.fr's generic tabular API) isn't justified.
- **The resource URL is an env var (`DECP_PARQUET_URL`), not a hardcoded
  constant.** Same reasoning as `SIRENE_STOCK_URL` in Step 1: this is a
  data.gouv.fr resource permalink, not a documented stable API endpoint like
  BODACC's Opendatasoft dataset slug — treating it as configuration, not code,
  matches how the project already handles this exact kind of URL.
- **`department` and `since` are explicit parameters, not computed inside the
  ingestion module.** Mirrors BODACC/SIRENE's convention of never calling
  `dt.date.today()` deep in business logic — the future orchestration plan's DAG
  computes `since = today - 365 days` and passes it in, keeping this module's
  functions pure and testable with fixed dates.
- **No manual-verification deferral this time.** Every prior plan's manual
  verification task was blocked on `SIRENE_API_KEY`. DECP's source needs no
  authentication at all — Task 7 is a real, runnable verification, not a
  placeholder for later.

## Architecture

```
data.gouv.fr DECP resource (DECP_PARQUET_URL, full-replacement national file)
        │
        ▼  download_decp_national_file
local: decp_national.parquet (~236 MB, ~3.3M rows, all years, all departments)
        │
        ▼  filter_decp_to_scope(department="08", since=<12 months ago>)
local: decp_scoped.parquet (department-08 buyers, last 12 months, current version only)
        │
        ▼  upload_file
bronze/decp/ingestion_date=<date>/marches.parquet
```

---

### Task 1: `bronze_object_key`

**Files:**
- Create: `src/registry/ingestion/decp.py`
- Test: `tests/ingestion/test_decp.py`

- [ ] **Step 1: Write the failing test**

Create `tests/ingestion/test_decp.py`:

```python
import datetime as dt

from registry.ingestion.decp import bronze_object_key


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 9))

    assert key == "bronze/decp/ingestion_date=2026-10-09/marches.parquet"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_decp.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.ingestion.decp'`

- [ ] **Step 3: Write minimal implementation**

Create `src/registry/ingestion/decp.py`:

```python
"""Ingestion of DECP (public procurement) consolidated data: a single
full-replacement national Parquet file, filtered locally to this project's
scope (see docs/superpowers/specs/2026-10-08-decp-procurement-design.md)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file


def bronze_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/decp/ingestion_date={ingestion_date.isoformat()}/marches.parquet"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingestion/test_decp.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/decp.py tests/ingestion/test_decp.py
git commit -m "feat: add DECP bronze object key formatting"
```

---

### Task 2: `filter_decp_to_scope`

**Files:**
- Modify: `src/registry/ingestion/decp.py`
- Modify: `tests/ingestion/test_decp.py`

This is the plan's one piece of real logic — tested thoroughly with small
hand-built PyArrow tables, no network or infrastructure needed.

- [ ] **Step 1: Write the failing tests**

Add to `tests/ingestion/test_decp.py`:

```python
import pyarrow as pa
import pyarrow.parquet as pq

from registry.ingestion.decp import filter_decp_to_scope


def _make_national_table() -> pa.Table:
    return pa.table(
        {
            "uid": ["M1", "M2", "M3", "M4"],
            "acheteur_departement_code": ["08", "51", "08", "08"],
            "datePublicationDonnees": pa.array(
                [
                    dt.date(2026, 5, 1),
                    dt.date(2026, 5, 1),
                    dt.date(2024, 1, 1),
                    dt.date(2026, 5, 1),
                ],
                type=pa.date32(),
            ),
            "donneesActuelles": [True, True, True, False],
        }
    )


def test_filter_decp_to_scope_keeps_only_matching_rows(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(
        national_path, department="08", since=dt.date(2025, 10, 9)
    )

    assert result.column("uid").to_pylist() == ["M1"]


def test_filter_decp_to_scope_excludes_wrong_department(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(
        national_path, department="08", since=dt.date(2025, 10, 9)
    )

    assert "M2" not in result.column("uid").to_pylist()


def test_filter_decp_to_scope_excludes_rows_before_since(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(
        national_path, department="08", since=dt.date(2025, 10, 9)
    )

    assert "M3" not in result.column("uid").to_pylist()


def test_filter_decp_to_scope_excludes_superseded_modifications(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(
        national_path, department="08", since=dt.date(2025, 10, 9)
    )

    assert "M4" not in result.column("uid").to_pylist()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_decp.py -v`
Expected: FAIL with `ImportError: cannot import name 'filter_decp_to_scope'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/ingestion/decp.py`:

```python
def filter_decp_to_scope(parquet_path: Path, department: str, since: dt.date) -> pa.Table:
    table = pq.read_table(parquet_path)
    mask = pc.and_(
        pc.and_(
            pc.equal(table["acheteur_departement_code"], department),
            pc.greater_equal(table["datePublicationDonnees"], pa.scalar(since, type=pa.date32())),
        ),
        pc.equal(table["donneesActuelles"], True),
    )
    return table.filter(mask)
```

**Note (post-implementation):** PyArrow's `ChunkedArray` doesn't support Python's
`&` operator directly (`TypeError: unsupported operand type(s) for &`) — fixed by
using `pc.and_()` explicitly instead, as shown above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_decp.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/decp.py tests/ingestion/test_decp.py
git commit -m "feat: filter the national DECP file to department/date/current scope"
```

---

### Task 3: `download_decp_national_file`

**Files:**
- Modify: `src/registry/ingestion/decp.py`
- Modify: `tests/ingestion/test_decp.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/ingestion/test_decp.py`:

```python
import responses

from registry.ingestion.decp import download_decp_national_file


@responses.activate
def test_download_decp_national_file_writes_response_body(tmp_path):
    url = "https://example.test/decp.parquet"
    responses.add(responses.GET, url, body=b"fake-parquet-bytes", status=200)
    dest_path = tmp_path / "decp_national.parquet"

    download_decp_national_file(url, dest_path)

    assert dest_path.read_bytes() == b"fake-parquet-bytes"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_decp.py::test_download_decp_national_file_writes_response_body -v`
Expected: FAIL with `ImportError: cannot import name 'download_decp_national_file'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/ingestion/decp.py`:

```python
def download_decp_national_file(url: str, dest_path: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, None)) as response:
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingestion/test_decp.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/decp.py tests/ingestion/test_decp.py
git commit -m "feat: download the national DECP file"
```

---

### Task 4: `run_ingestion` orchestration

**Files:**
- Modify: `src/registry/ingestion/decp.py`
- Modify: `tests/ingestion/test_decp.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/ingestion/test_decp.py`:

```python
import io

import boto3
from moto import mock_aws

from registry.ingestion.decp import run_ingestion


@responses.activate
@mock_aws
def test_run_ingestion_uploads_filtered_parquet_to_bronze(tmp_path, monkeypatch):
    # moto only intercepts requests to real AWS-style endpoints, not custom ones
    # like Garage's, so get_s3_client is swapped for a moto-compatible client here
    # (same workaround used in test_sirene_bootstrap.py).
    monkeypatch.setattr(
        "registry.ingestion.decp.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )

    national_path = tmp_path / "source_national.parquet"
    pq.write_table(_make_national_table(), national_path)

    url = "https://example.test/decp.parquet"
    responses.add(responses.GET, url, body=national_path.read_bytes(), status=200)

    key = run_ingestion(
        url,
        bucket="lakehouse",
        department="08",
        since=dt.date(2025, 10, 9),
        work_dir=tmp_path,
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    result_table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert result_table.column("uid").to_pylist() == ["M1"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_decp.py::test_run_ingestion_uploads_filtered_parquet_to_bronze -v`
Expected: FAIL with `ImportError: cannot import name 'run_ingestion'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/registry/ingestion/decp.py`:

```python
def run_ingestion(
    url: str, bucket: str, department: str, since: dt.date, work_dir: Path
) -> str:
    national_path = work_dir / "decp_national.parquet"
    scoped_path = work_dir / "decp_scoped.parquet"

    download_decp_national_file(url, national_path)
    scoped_table = filter_decp_to_scope(national_path, department, since)
    pq.write_table(scoped_table, scoped_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today())
    upload_file(client, scoped_path, bucket, key)
    return key
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingestion/test_decp.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/decp.py tests/ingestion/test_decp.py
git commit -m "feat: orchestrate DECP download, scope filter, and bronze upload"
```

---

### Task 5: Wire `DECP_PARQUET_URL` into the environment

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Add the env var**

Add to `.env.example`, after `SIRENE_STOCK_URL=`:

```
# Direct download URL for the consolidated national DECP (public procurement)
# Parquet file. Get the current resource link from
# https://www.data.gouv.fr/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/
DECP_PARQUET_URL=
```

- [ ] **Step 2: Add the real value to `.env` (not committed)**

```bash
grep -q "^DECP_PARQUET_URL=" .env || echo "DECP_PARQUET_URL=https://www.data.gouv.fr/api/1/datasets/r/11cea8e8-df3e-4ed1-932b-781e2635e432" >> .env
```

- [ ] **Step 3: Commit**

```bash
git add .env.example
git commit -m "feat: add DECP_PARQUET_URL environment variable"
```

---

### Task 6: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all tests pass (77 pre-existing + 7 new in `test_decp.py` = 84).

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

### Task 7: Manual verification — real department-08 ingestion

**Files:** none (manual verification only)

Unlike every prior manual-verification task in this project, this one needs no
API key — DECP's source requires no authentication. The only precondition is
Garage running.

- [ ] **Step 1: Start Garage**

```bash
docker compose up -d garage
```

- [ ] **Step 2: Run the real ingestion**

```bash
set -a && source .env && set +a
uv run python -c "
import datetime as dt
from pathlib import Path
from registry.ingestion.decp import run_ingestion
import os

key = run_ingestion(
    url=os.environ['DECP_PARQUET_URL'],
    bucket=os.environ['LAKEHOUSE_BUCKET'],
    department='08',
    since=dt.date.today() - dt.timedelta(days=365),
    work_dir=Path('/tmp'),
)
print(f'Landed at {key}')
"
```

Expected: downloads the ~236 MB national file, filters it, and prints a bronze
key like `bronze/decp/ingestion_date=2026-10-09/marches.parquet`. Based on the
volume check done while writing the spec, expect roughly 1,000-1,700 rows in the
scoped file (the exact count may differ slightly as the source's trailing
12-month window has moved since that check).

- [ ] **Step 3: Verify the row count directly**

```bash
uv run python -c "
import pyarrow.parquet as pq
print(pq.read_table('/tmp/decp_scoped.parquet').num_rows)
"
```

Expected: a row count in the same ballpark as Step 2's output confirmed.

---

## Self-review notes

- **Spec coverage:** this plan covers exactly the spec's "Data flow" steps 1-3
  (download, filter, land in bronze/silver) up through the bronze landing —
  cleaning into `silver.decp_marches` and resolution are explicitly the next
  plan's job, not duplicated here.
- **Placeholder scan:** none found.
- **Type consistency:** `filter_decp_to_scope`'s `(parquet_path, department, since)`
  signature matches its use in `run_ingestion` and in Task 7's manual
  verification exactly. `bronze_object_key`'s path format matches the one used
  in `run_ingestion`.
