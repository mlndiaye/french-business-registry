# SIRENE Bootstrap Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold the Python project and stand up an S3-compatible object store
locally, then build a tested bootstrap extractor that downloads the full SIRENE
establishment stock file, converts it to Parquet, and lands it in the bronze layer of
the data lake.

**Architecture:** A `src/` layout Python package (`registry`) with an `ingestion`
subpackage holding two modules: `storage.py` (generic, backend-agnostic S3 helpers)
and `sirene_bootstrap.py` (SIRENE-specific download/convert/upload orchestration).
**Update (original design used MinIO; switched to Garage during implementation —
see Task 2 note below.)** The object store runs as a single Docker Compose service.
All ingestion logic is unit-tested with `moto` (S3 mocking) and `responses` (HTTP
mocking) — no network or real object store access is required for the automated test
suite; a final manual task verifies the real path end-to-end against a running
container.

**Tech Stack:** Python 3.12, `uv`, `boto3`, `pyarrow`, `requests`, `pytest`, `moto`,
`responses`, `ruff`, Docker Compose, Garage (S3-compatible object storage).

This is Plan 1 of 5 for Step 1 (SIRENE lakehouse pipeline) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` for the full design.
Later plans (Spark/Iceberg SCD2 transformations, Airflow orchestration + daily diffs,
dbt tests, Postgres/FastAPI serving) build on top of what this plan produces (a
working bronze ingestion path) and are out of scope here.

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `src/registry/__init__.py`
- Create: `src/registry/ingestion/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "french-business-registry"
version = "0.1.0"
description = "Multi-source ELT platform unifying French public business data into a historized registry"
requires-python = ">=3.12"
dependencies = [
    "boto3>=1.34",
    "pyarrow>=16.0",
    "requests>=2.31",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "ruff>=0.6",
    "moto[s3]>=5.0",
    "responses>=0.25",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/registry"]

[tool.ruff]
line-length = 100
src = ["src"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

- [ ] **Step 2: Create `.python-version`**

```
3.12
```

- [ ] **Step 3: Create empty package files**

`src/registry/__init__.py`:
```python
```

`src/registry/ingestion/__init__.py`:
```python
```

- [ ] **Step 4: Install dependencies**

Run: `uv sync --all-groups`
Expected: resolves and installs all dependencies, creates `.venv/` and `uv.lock`,
exits 0.

- [ ] **Step 5: Verify lint passes on the empty scaffold**

Run: `uv run ruff check .`
Expected: `All checks passed!`

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .python-version uv.lock src/registry/__init__.py src/registry/ingestion/__init__.py
git commit -m "chore: scaffold Python project with uv"
```

---

### Task 2: Object storage via Docker Compose

**Note (post-implementation):** the original design used MinIO. MinIO's Docker
images stopped being freely pullable from Docker Hub and quay.io during
implementation (repository access denied even after `docker login`). Replaced with
**Garage**, a drop-in S3-compatible object store with a freely available image
(`dxflrs/garage`). The `storage.py` module is backend-agnostic (plain `boto3`), so
no application code changed — only the Compose service, its config file, and the
env var names (`MINIO_*` → generic `S3_*`).

**Files:**
- Create: `docker-compose.yml`
- Create: `garage.toml`
- Create: `.env.example`

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  garage:
    image: dxflrs/garage:v1.0.1
    ports:
      - "3900:3900"
      - "3903:3903"
    volumes:
      - ./garage.toml:/etc/garage.toml:ro
      - garage_data:/data

volumes:
  garage_data:
```

- [ ] **Step 2: Create `garage.toml`**

```toml
metadata_dir = "/data/meta"
data_dir = "/data/store"
db_engine = "lmdb"

replication_factor = 1

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "ee69fde8e7620fd2f5b379a2bf92caf6183055a28fba005cbeef5d1ff8d41266"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"

[admin]
api_bind_addr = "[::]:3903"
admin_token = "4ac5bf8a364885a7f2b7ffe7e0cb5982273112789f74b4e88a770ef6edd88e2f"
```

These secrets are for a single-node local dev cluster only (no real network
exposure) — equivalent in sensitivity to the `minioadmin`/`minioadmin123` dev
defaults the original design would have used.

- [ ] **Step 3: Create `.env.example`**

```
S3_ENDPOINT_URL=http://localhost:3900
# Generated by `garage key create` during the one-time cluster bootstrap
# (see Task 10 below).
S3_ACCESS_KEY=
S3_SECRET_KEY=
S3_REGION=garage
LAKEHOUSE_BUCKET=lakehouse
# Direct download URL for the full SIRENE establishment stock file.
# Get the current link from https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
SIRENE_STOCK_URL=
```

- [ ] **Step 4: Verify `.env` is ignored by git**

Run: `grep -c '^\.env$' .gitignore`
Expected: `1` (the standard Python `.gitignore` template already ignores `.env`)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml garage.toml .env.example
git commit -m "feat: switch object storage backend to Garage"
```

---

### Task 3: `storage.get_s3_client` and `storage.ensure_bucket`

**Files:**
- Create: `src/registry/ingestion/storage.py`
- Test: `tests/ingestion/test_storage.py`

- [ ] **Step 1: Write the failing tests**

```python
import boto3
from moto import mock_aws

from registry.ingestion.storage import ensure_bucket, get_s3_client


def test_get_s3_client_uses_env_configuration(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:3900")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-key")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")

    client = get_s3_client()

    assert client.meta.endpoint_url == "http://localhost:3900"


@mock_aws
def test_ensure_bucket_is_idempotent():
    client = boto3.client("s3", region_name="us-east-1")

    ensure_bucket(client, "lakehouse")
    ensure_bucket(client, "lakehouse")

    response = client.list_buckets()
    bucket_names = [b["Name"] for b in response["Buckets"]]
    assert bucket_names == ["lakehouse"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_storage.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.ingestion.storage'`

- [ ] **Step 3: Implement `storage.py`**

```python
"""S3-compatible storage helpers for the data lake (backend-agnostic: MinIO, Garage, AWS)."""

from __future__ import annotations

import os

import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_storage.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/storage.py tests/ingestion/test_storage.py
git commit -m "feat: add S3 client and bucket helpers for MinIO"
```

---

### Task 4: `storage.upload_file`

**Files:**
- Modify: `src/registry/ingestion/storage.py`
- Test: `tests/ingestion/test_storage.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/ingestion/test_storage.py`:

```python
from pathlib import Path

from registry.ingestion.storage import upload_file


@mock_aws
def test_upload_file_creates_object(tmp_path: Path):
    client = boto3.client("s3", region_name="us-east-1")
    local_file = tmp_path / "data.parquet"
    local_file.write_bytes(b"fake-parquet-bytes")

    upload_file(client, local_file, bucket="lakehouse", key="bronze/sirene/data.parquet")

    body = client.get_object(Bucket="lakehouse", Key="bronze/sirene/data.parquet")["Body"].read()
    assert body == b"fake-parquet-bytes"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_storage.py -v`
Expected: FAIL with `ImportError: cannot import name 'upload_file'`

- [ ] **Step 3: Implement `upload_file`**

Append to `src/registry/ingestion/storage.py`:

```python
from pathlib import Path


def upload_file(client, local_path: Path, bucket: str, key: str) -> None:
    ensure_bucket(client, bucket)
    client.upload_file(str(local_path), bucket, key)
```

(Move the `from pathlib import Path` import to the top of the file, next to the
existing `import os`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_storage.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/storage.py tests/ingestion/test_storage.py
git commit -m "feat: add upload_file helper for the data lake"
```

---

### Task 5: `sirene_bootstrap.bronze_object_key`

**Files:**
- Create: `src/registry/ingestion/sirene_bootstrap.py`
- Test: `tests/ingestion/test_sirene_bootstrap.py`

- [ ] **Step 1: Write the failing test**

```python
import datetime as dt

from registry.ingestion.sirene_bootstrap import bronze_object_key


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 1))

    assert key == "bronze/sirene/stock/ingestion_date=2026-10-01/stock.parquet"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.ingestion.sirene_bootstrap'`

- [ ] **Step 3: Implement `bronze_object_key`**

```python
"""Bootstrap ingestion of the full SIRENE establishment stock file into the bronze layer."""

from __future__ import annotations

import datetime as dt


def bronze_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/sirene/stock/ingestion_date={ingestion_date.isoformat()}/stock.parquet"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/sirene_bootstrap.py tests/ingestion/test_sirene_bootstrap.py
git commit -m "feat: add bronze object key formatting for SIRENE stock"
```

---

### Task 6: `sirene_bootstrap.convert_csv_to_parquet`

**Files:**
- Create: `tests/fixtures/sirene_stock_sample.csv`
- Create: `tests/conftest.py`
- Modify: `src/registry/ingestion/sirene_bootstrap.py`
- Test: `tests/ingestion/test_sirene_bootstrap.py`

- [ ] **Step 1: Create the fixture CSV**

`tests/fixtures/sirene_stock_sample.csv`:

```
siren,nic,siret,statutDiffusionEtablissement,dateCreationEtablissement,etablissementSiege,numeroVoieEtablissement,typeVoieEtablissement,libelleVoieEtablissement,codePostalEtablissement,libelleCommuneEtablissement,activitePrincipaleEtablissement,etatAdministratifEtablissement,dateDernierTraitementEtablissement
552032534,00019,55203253400019,O,1966-01-01,true,8,RUE,DE LA PAIX,75002,PARIS,70.10Z,A,2023-05-12
732829320,00014,73282932000014,O,1994-03-15,false,12,AV,DES CHAMPS ELYSEES,75008,PARIS,46.19B,A,2022-11-03
```

- [ ] **Step 2: Create `tests/conftest.py`**

```python
from pathlib import Path

import pytest


@pytest.fixture
def fixture_csv_path() -> Path:
    return Path(__file__).parent / "fixtures" / "sirene_stock_sample.csv"
```

- [ ] **Step 3: Write the failing test**

Append to `tests/ingestion/test_sirene_bootstrap.py`:

```python
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from registry.ingestion.sirene_bootstrap import convert_csv_to_parquet


def test_convert_csv_to_parquet_preserves_all_columns_as_strings(
    tmp_path: Path, fixture_csv_path: Path
):
    parquet_path = tmp_path / "out.parquet"

    convert_csv_to_parquet(fixture_csv_path, parquet_path)

    table = pq.read_table(parquet_path)
    assert table.num_rows == 2
    assert table.column("siren").to_pylist() == ["552032534", "732829320"]
    assert all(field.type == pa.string() for field in table.schema)
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: FAIL with `ImportError: cannot import name 'convert_csv_to_parquet'`

- [ ] **Step 5: Implement `convert_csv_to_parquet`**

Add to the top of `src/registry/ingestion/sirene_bootstrap.py`:

```python
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
```

Append the function:

```python
def convert_csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    with open(csv_path, encoding="utf-8") as f:
        header = f.readline().strip().split(",")

    column_types = {name: pa.string() for name in header}
    table = pa_csv.read_csv(
        csv_path,
        convert_options=pa_csv.ConvertOptions(column_types=column_types),
    )
    pq.write_table(table, parquet_path)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: `2 passed`

- [ ] **Step 7: Commit**

```bash
git add tests/fixtures/sirene_stock_sample.csv tests/conftest.py src/registry/ingestion/sirene_bootstrap.py tests/ingestion/test_sirene_bootstrap.py
git commit -m "feat: convert SIRENE stock CSV to Parquet, preserving raw columns"
```

---

### Task 7: `sirene_bootstrap.download_sirene_stock`

**Files:**
- Modify: `src/registry/ingestion/sirene_bootstrap.py`
- Test: `tests/ingestion/test_sirene_bootstrap.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/ingestion/test_sirene_bootstrap.py`:

```python
import responses

from registry.ingestion.sirene_bootstrap import download_sirene_stock


@responses.activate
def test_download_sirene_stock_writes_response_body(tmp_path: Path):
    url = "https://example.test/stock.csv"
    responses.add(responses.GET, url, body=b"siren,nic\n123,001\n", status=200)
    dest_path = tmp_path / "stock.csv"

    download_sirene_stock(url, dest_path)

    assert dest_path.read_bytes() == b"siren,nic\n123,001\n"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: FAIL with `ImportError: cannot import name 'download_sirene_stock'`

- [ ] **Step 3: Implement `download_sirene_stock`**

Add `import requests` to the top of `src/registry/ingestion/sirene_bootstrap.py`, then
append:

```python
def download_sirene_stock(url: str, dest_path: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, None)) as response:
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/sirene_bootstrap.py tests/ingestion/test_sirene_bootstrap.py
git commit -m "feat: download SIRENE stock file via streaming HTTP GET"
```

---

### Task 8: `sirene_bootstrap.run_bootstrap` (end-to-end orchestration)

**Files:**
- Modify: `src/registry/ingestion/sirene_bootstrap.py`
- Test: `tests/ingestion/test_sirene_bootstrap.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/ingestion/test_sirene_bootstrap.py`:

```python
import io

from moto import mock_aws

from registry.ingestion.sirene_bootstrap import run_bootstrap


@responses.activate
@mock_aws
def test_run_bootstrap_uploads_parquet_to_bronze(
    tmp_path: Path, monkeypatch, fixture_csv_path: Path
):
    # moto only intercepts requests to real AWS-style endpoints, not custom ones
    # like MinIO's, so get_s3_client is swapped for a moto-compatible client here.
    monkeypatch.setattr(
        "registry.ingestion.sirene_bootstrap.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )

    url = "https://example.test/stock.csv"
    responses.add(responses.GET, url, body=fixture_csv_path.read_bytes(), status=200)

    key = run_bootstrap(url, bucket="lakehouse", work_dir=tmp_path)

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 2
```

Add `import boto3` to the top of `tests/ingestion/test_sirene_bootstrap.py` (needed by
this test, alongside the existing `pyarrow.parquet as pq` import from Task 6).

**Note:** moto's `mock_aws` only intercepts requests sent to real AWS-style endpoint
hostnames — it does not intercept custom `endpoint_url`s such as MinIO's. Setting
`MINIO_ENDPOINT_URL` to a local address and letting `get_s3_client()` use it would
therefore attempt a real (failing) network connection in tests. Patching
`get_s3_client` itself to return a plain moto-backed client avoids this without
changing any production code.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: FAIL with `ImportError: cannot import name 'run_bootstrap'`

- [ ] **Step 3: Implement `run_bootstrap`**

Add to the top of `src/registry/ingestion/sirene_bootstrap.py`:

```python
from registry.ingestion.storage import get_s3_client, upload_file
```

Append:

```python
def run_bootstrap(stock_url: str, bucket: str, work_dir: Path) -> str:
    csv_path = work_dir / "stock.csv"
    parquet_path = work_dir / "stock.parquet"

    download_sirene_stock(stock_url, csv_path)
    convert_csv_to_parquet(csv_path, parquet_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today())
    upload_file(client, parquet_path, bucket, key)
    return key
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_sirene_bootstrap.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/sirene_bootstrap.py tests/ingestion/test_sirene_bootstrap.py
git commit -m "feat: orchestrate SIRENE bootstrap download, convert, and upload"
```

---

### Task 9: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all 7 tests pass (3 in `test_storage.py`, 4 in `test_sirene_bootstrap.py`).

- [ ] **Step 2: Run lint**

Run: `uv run ruff check .`
Expected: `All checks passed!`

- [ ] **Step 3: Fix formatting if needed**

Run: `uv run ruff format --check .`
If it reports files that would be reformatted, run `uv run ruff format .`, then:

```bash
git add -u
git commit -m "style: apply ruff formatting"
```

If it already reports no changes needed, skip the commit.

---

### Task 10: Manual end-to-end verification against a real object store

**Files:** none (manual verification only)

**Note (post-implementation):** Garage requires a one-time cluster bootstrap
(layout assignment + access key creation) that MinIO would not have needed — MinIO
accepts a root user/password upfront and creates buckets on demand via the S3 API.
Steps 1-2 below cover this.

- [ ] **Step 1: Start Garage**

```bash
cp .env.example .env
docker compose up -d garage
docker compose ps
```

Expected: `garage` service shows as `running`.

- [ ] **Step 2: Bootstrap the single-node cluster layout and create a bucket + key**

```bash
docker compose exec garage /garage node id
```

Expected: prints a node ID of the form `<hex pubkey>@127.0.0.1:3901`. Use it below.

```bash
docker compose exec garage /garage layout assign <node_id> -z dc1 -c 1G
docker compose exec garage /garage layout apply --version 1
docker compose exec garage /garage bucket create lakehouse
docker compose exec garage /garage key create registry-key
```

The last command prints a **Key ID** and **Secret key** — copy them into `.env` as
`S3_ACCESS_KEY` and `S3_SECRET_KEY`.

```bash
docker compose exec garage /garage bucket allow --read --write --owner lakehouse --key registry-key
```

- [ ] **Step 3: Serve the fixture CSV over HTTP**

```bash
python -m http.server 8001 --directory tests/fixtures &
```

- [ ] **Step 4: Load environment variables and run the bootstrap**

```bash
set -a && source .env && set +a
uv run python -c "
from pathlib import Path
from registry.ingestion.sirene_bootstrap import run_bootstrap

key = run_bootstrap(
    'http://localhost:8001/sirene_stock_sample.csv',
    bucket='lakehouse',
    work_dir=Path('/tmp'),
)
print(key)
"
```

Expected: prints a key of the form
`bronze/sirene/stock/ingestion_date=<today's date>/stock.parquet`.

- [ ] **Step 5: Verify the object landed in Garage**

```bash
uv run python -c "
import boto3, os
client = boto3.client(
    's3',
    endpoint_url=os.environ['S3_ENDPOINT_URL'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY'],
    region_name=os.environ['S3_REGION'],
)
for obj in client.list_objects_v2(Bucket='lakehouse')['Contents']:
    print(obj['Key'])
"
```

Expected: prints the same key from Step 4.

- [ ] **Step 6: Stop the fixture HTTP server**

```bash
kill %1
```

(Leave `docker compose`'s Garage running — Plan 2 will reuse it.)
