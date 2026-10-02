# Airflow Daily Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a daily-differential extractor for the Sirene API, and an Airflow DAG
that runs it end-to-end each day: fetch the établissements updated since the last
run, land them in bronze, clean them into silver, and merge them into the historized
gold table built in Plan 2.

**Architecture:** A new `sirene_diff.py` ingestion module mirrors Plan 1's bootstrap
module (fetch → normalize → write to bronze), reusing the exact same bronze column
schema so Plan 2's `clean_sirene_bronze`/`bronze_to_silver`/`silver_to_gold` work
unchanged on diff data. Airflow runs in Docker (LocalExecutor, Postgres metadata DB,
a custom image with a JDK for PySpark) and orchestrates a 3-task TaskFlow DAG whose
`since`/`until` window comes from Airflow's own logical-date scheduling, not a
custom state store — making each run idempotent and backfillable.

**Tech Stack:** `requests` (already a dependency), Apache Airflow 2.10.3 (Docker
only — not added to the project's Python environment), PostgreSQL 16, Docker Compose.

This is Plan 3 of 5 for Step 1 (SIRENE lakehouse pipeline) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` for the full design,
Plan 1 (`2026-10-01-sirene-bootstrap-scaffold.md`) for the bootstrap extractor this
mirrors, and Plan 2 (`2026-10-01-spark-bronze-silver-gold.md`) for the transformation
functions this DAG calls. Plan 4 (dbt tests) and Plan 5 (Postgres sync + FastAPI
serving) are out of scope here.

## Decisions made

- **Diff bronze data uses the exact same 14-column schema as the bootstrap stock
  bronze** (`siren`, `nic`, `siret`, ... — see Plan 1/2). `normalize_etablissement`
  flattens the Sirene API's nested JSON into this flat shape, so Plan 2's
  `clean_sirene_bronze` and everything downstream needs no changes to handle diff
  data.
- **`since`/`until` come from Airflow's logical date (`data_interval_start`/
  `data_interval_end`)**, not a custom "last successful run" table. This is the
  idiomatic Airflow pattern: it makes every run idempotent and backfillable
  (`airflow dags backfill` just re-derives the right window from the run's logical
  date) without extra state to maintain.
- **Correction to a Plan 2 decision:** Plan 2 said cluster-mode Spark submission
  would arrive "when Airflow submits jobs." Having now built the orchestration, the
  honest call is to **keep `local[*]` mode** even from Airflow tasks. A real
  multi-node Spark cluster doesn't add anything defensible on a single laptop with
  this data volume — the orchestration value (idempotent DAG, retries, backfill) is
  what this plan demonstrates, not distributed execution. The real "why Spark"
  payoff is reserved for the entity-resolution step in Steps 2-3, where volume
  actually justifies it.
- **The Airflow DAG file has no automated pytest coverage.** Testing it properly
  would mean installing `apache-airflow` (a large dependency with its own pinned
  transitive versions) into the project's local environment alongside `pyspark`,
  `boto3`, etc. — a real risk of version conflicts for a framework dependency that
  would only be used to parse-check a thin wrapper around already-tested functions
  (`run_daily_diff`, `bronze_to_silver`, `silver_to_gold`). Instead, the DAG's
  correctness is verified manually (Task 9): Airflow's own UI/CLI surfaces DAG
  import errors immediately, and the actual business logic is already covered by
  Tasks 1-4's unit tests.
- **The Airflow image is a custom Dockerfile**, not the `_PIP_ADDITIONAL_REQUIREMENTS`
  env var some quick-starts use — that mechanism is explicitly documented by Airflow
  as dev-only and unconstrained. The Dockerfile installs this project with `pip
  install --constraint <airflow's own constraints file>`, the documented-safe way to
  add dependencies to an Airflow image, and adds a JDK via `apt-get` for PySpark.
- **PostgreSQL is introduced now for Airflow's metadata database**, not SQLite — the
  same Postgres instance (a different logical database) will be reused in Plan 5 for
  the registry's serving store, avoiding running two separate Postgres containers.
- **The Sirene API's exact field names/paths (`adresseEtablissement`,
  `periodesEtablissement[0]`, `curseurSuivant`, the `/api-sirene/3.11/siret`
  endpoint) are this plan's best-confidence reconstruction from public Sirene API
  documentation, not verified against a live call** — this environment has no
  provisioned API key and no access to api.insee.fr to confirm. Task 10 (deferred)
  flags exactly where to double-check and adjust once a real key is available.
- **Task 10 (real-API end-to-end run) is explicitly deferred.** The user does not
  yet have a Sirene API key (free signup at portail-api.insee.fr). Tasks 1-9 are
  fully executable and verifiable now without one; Task 10 is documented but not
  required to consider this plan complete.

## Architecture

```
Airflow DAG "sirene_daily_pipeline" (schedule: @daily)
  ┌─────────────────────┐   ┌────────────────────────┐   ┌───────────────────────┐
  │ extract_daily_diff  │──▶│ transform_bronze_to_    │──▶│ transform_silver_to_  │
  │ (Sirene API → bronze)│   │ silver (Plan 2)        │   │ gold (Plan 2, SCD2)   │
  └─────────────────────┘   └────────────────────────┘   └───────────────────────┘
        since/until = data_interval_start/end (Airflow logical date)

Infra: Docker Compose — garage (Plan 1/2), postgres (Airflow metadata), airflow-init,
airflow-webserver, airflow-scheduler (custom image: apache/airflow + JDK + this
project installed via pip with Airflow's constraints file).
```

---

### Task 1: `normalize_etablissement`

**Files:**
- Create: `src/registry/ingestion/sirene_diff.py`
- Test: `tests/ingestion/test_sirene_diff.py`

- [ ] **Step 1: Write the failing test**

```python
from registry.ingestion.sirene_diff import normalize_etablissement


def test_normalize_etablissement_flattens_nested_api_response():
    raw = {
        "siren": "552032534",
        "nic": "00019",
        "siret": "55203253400019",
        "statutDiffusionEtablissement": "O",
        "dateCreationEtablissement": "1966-01-01",
        "etablissementSiege": True,
        "dateDernierTraitementEtablissement": "2026-10-02",
        "adresseEtablissement": {
            "numeroVoieEtablissement": "99",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA PAIX",
            "codePostalEtablissement": "75002",
            "libelleCommuneEtablissement": "PARIS",
        },
        "periodesEtablissement": [
            {
                "dateFin": None,
                "dateDebut": "2026-10-02",
                "etatAdministratifEtablissement": "A",
                "activitePrincipaleEtablissement": "70.10Z",
            }
        ],
    }

    result = normalize_etablissement(raw)

    assert result == {
        "siren": "552032534",
        "nic": "00019",
        "siret": "55203253400019",
        "statutDiffusionEtablissement": "O",
        "dateCreationEtablissement": "1966-01-01",
        "etablissementSiege": "true",
        "numeroVoieEtablissement": "99",
        "typeVoieEtablissement": "RUE",
        "libelleVoieEtablissement": "DE LA PAIX",
        "codePostalEtablissement": "75002",
        "libelleCommuneEtablissement": "PARIS",
        "activitePrincipaleEtablissement": "70.10Z",
        "etatAdministratifEtablissement": "A",
        "dateDernierTraitementEtablissement": "2026-10-02",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.ingestion.sirene_diff'`

- [ ] **Step 3: Implement `normalize_etablissement`**

```python
"""Daily differential ingestion from the Sirene API into the bronze layer."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file

SIRENE_API_URL = "https://api.insee.fr/api-sirene/3.11/siret"

BRONZE_COLUMNS = [
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


def normalize_etablissement(raw: dict) -> dict:
    adresse = raw.get("adresseEtablissement") or {}
    periodes = raw.get("periodesEtablissement") or [{}]
    periode_courante = periodes[0]
    return {
        "siren": raw.get("siren"),
        "nic": raw.get("nic"),
        "siret": raw.get("siret"),
        "statutDiffusionEtablissement": raw.get("statutDiffusionEtablissement"),
        "dateCreationEtablissement": raw.get("dateCreationEtablissement"),
        "etablissementSiege": "true" if raw.get("etablissementSiege") else "false",
        "numeroVoieEtablissement": adresse.get("numeroVoieEtablissement"),
        "typeVoieEtablissement": adresse.get("typeVoieEtablissement"),
        "libelleVoieEtablissement": adresse.get("libelleVoieEtablissement"),
        "codePostalEtablissement": adresse.get("codePostalEtablissement"),
        "libelleCommuneEtablissement": adresse.get("libelleCommuneEtablissement"),
        "activitePrincipaleEtablissement": periode_courante.get("activitePrincipaleEtablissement"),
        "etatAdministratifEtablissement": periode_courante.get("etatAdministratifEtablissement"),
        "dateDernierTraitementEtablissement": raw.get("dateDernierTraitementEtablissement"),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/sirene_diff.py tests/ingestion/test_sirene_diff.py
git commit -m "feat: flatten Sirene API establishment records to the bronze schema"
```

---

### Task 2: `fetch_sirene_updates`

**Files:**
- Modify: `src/registry/ingestion/sirene_diff.py`
- Test: `tests/ingestion/test_sirene_diff.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/ingestion/test_sirene_diff.py`:

```python
import datetime as dt

import responses

from registry.ingestion.sirene_diff import SIRENE_API_URL, fetch_sirene_updates


def _etablissement(siret: str, numero_voie: str) -> dict:
    return {
        "siren": siret[:9],
        "nic": siret[9:],
        "siret": siret,
        "statutDiffusionEtablissement": "O",
        "dateCreationEtablissement": "1966-01-01",
        "etablissementSiege": True,
        "dateDernierTraitementEtablissement": "2026-10-02",
        "adresseEtablissement": {
            "numeroVoieEtablissement": numero_voie,
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA PAIX",
            "codePostalEtablissement": "75002",
            "libelleCommuneEtablissement": "PARIS",
        },
        "periodesEtablissement": [
            {
                "dateFin": None,
                "dateDebut": "2026-10-02",
                "etatAdministratifEtablissement": "A",
                "activitePrincipaleEtablissement": "70.10Z",
            }
        ],
    }


@responses.activate
def test_fetch_sirene_updates_returns_normalized_records_for_single_page():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_etablissement("55203253400019", "99")],
        },
        status=200,
    )

    records = fetch_sirene_updates("test-api-key", dt.date(2026, 10, 1), dt.date(2026, 10, 2))

    assert len(records) == 1
    assert records[0]["siret"] == "55203253400019"
    assert records[0]["numeroVoieEtablissement"] == "99"
    assert responses.calls[0].request.headers["X-INSEE-Api-Key-Integration"] == "test-api-key"


@responses.activate
def test_fetch_sirene_updates_follows_pagination_cursor():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "*", "curseurSuivant": "PAGE2"},
            "etablissements": [_etablissement("55203253400019", "99")],
        },
        status=200,
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "PAGE2", "curseurSuivant": "PAGE2"},
            "etablissements": [_etablissement("73282932000014", "12")],
        },
        status=200,
    )

    records = fetch_sirene_updates("test-api-key", dt.date(2026, 10, 1), dt.date(2026, 10, 2))

    assert [r["siret"] for r in records] == ["55203253400019", "73282932000014"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: FAIL with `ImportError: cannot import name 'fetch_sirene_updates'`

- [ ] **Step 3: Implement `fetch_sirene_updates`**

Append to `src/registry/ingestion/sirene_diff.py`:

```python
def fetch_sirene_updates(api_key: str, since: dt.date, until: dt.date) -> list[dict]:
    records: list[dict] = []
    curseur = "*"
    headers = {"X-INSEE-Api-Key-Integration": api_key}
    query = f"dateDernierTraitementEtablissement:[{since.isoformat()} TO {until.isoformat()}]"

    while True:
        response = requests.get(
            SIRENE_API_URL,
            headers=headers,
            params={"q": query, "nombre": 1000, "curseur": curseur},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        records.extend(normalize_etablissement(e) for e in payload["etablissements"])

        next_curseur = payload["header"]["curseurSuivant"]
        if next_curseur == curseur:
            break
        curseur = next_curseur

    return records
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/sirene_diff.py tests/ingestion/test_sirene_diff.py
git commit -m "feat: fetch Sirene API updates with cursor-based pagination"
```

---

### Task 3: `diff_object_key` and `write_diff_to_bronze`

**Files:**
- Modify: `src/registry/ingestion/sirene_diff.py`
- Test: `tests/ingestion/test_sirene_diff.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/ingestion/test_sirene_diff.py`:

```python
import io

import boto3
from moto import mock_aws
import pyarrow.parquet as pq

from registry.ingestion.sirene_diff import diff_object_key, write_diff_to_bronze


def test_diff_object_key_formats_ingestion_date():
    key = diff_object_key(dt.date(2026, 10, 2))

    assert key == "bronze/sirene/diff/ingestion_date=2026-10-02/diff.parquet"


@mock_aws
def test_write_diff_to_bronze_uploads_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.ingestion.sirene_diff.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    records = [normalize_etablissement(_etablissement("55203253400019", "99"))]

    key = write_diff_to_bronze(records, bucket="lakehouse", work_dir=tmp_path)

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
    assert table.column("siret").to_pylist() == ["55203253400019"]
```

`normalize_etablissement` and `_etablissement` are already imported/defined from
Tasks 1 and 2 earlier in this same file — no new imports needed beyond the ones
shown above.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: FAIL with `ImportError: cannot import name 'diff_object_key'`

- [ ] **Step 3: Implement `diff_object_key` and `write_diff_to_bronze`**

Append to `src/registry/ingestion/sirene_diff.py`:

```python
def diff_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/sirene/diff/ingestion_date={ingestion_date.isoformat()}/diff.parquet"


def write_diff_to_bronze(records: list[dict], bucket: str, work_dir: Path) -> str:
    table = pa.Table.from_pylist(
        records, schema=pa.schema([(name, pa.string()) for name in BRONZE_COLUMNS])
    )
    parquet_path = work_dir / "diff.parquet"
    pq.write_table(table, parquet_path)

    client = get_s3_client()
    key = diff_object_key(dt.date.today())
    upload_file(client, parquet_path, bucket, key)
    return key
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/ingestion/sirene_diff.py tests/ingestion/test_sirene_diff.py
git commit -m "feat: write Sirene diff records to bronze as Parquet"
```

---

### Task 4: `run_daily_diff`

**Files:**
- Modify: `src/registry/ingestion/sirene_diff.py`
- Test: `tests/ingestion/test_sirene_diff.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/ingestion/test_sirene_diff.py`:

```python
from registry.ingestion.sirene_diff import run_daily_diff


@responses.activate
@mock_aws
def test_run_daily_diff_fetches_and_uploads_to_bronze(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.ingestion.sirene_diff.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_etablissement("55203253400019", "99")],
        },
        status=200,
    )

    key = run_daily_diff(
        "test-api-key", "lakehouse", dt.date(2026, 10, 1), dt.date(2026, 10, 2), tmp_path
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: FAIL with `ImportError: cannot import name 'run_daily_diff'`

- [ ] **Step 3: Implement `run_daily_diff`**

Append to `src/registry/ingestion/sirene_diff.py`:

```python
def run_daily_diff(
    api_key: str, bucket: str, since: dt.date, until: dt.date, work_dir: Path
) -> str:
    records = fetch_sirene_updates(api_key, since, until)
    return write_diff_to_bronze(records, bucket, work_dir)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_sirene_diff.py -v`
Expected: `6 passed`

- [ ] **Step 5: Run the full suite and lint**

Run: `uv run pytest -v && uv run ruff check . && uv run ruff format --check .`
Expected: all pass (23 tests total: 17 from Plans 1-2 + 6 new). If formatting is
needed, run `uv run ruff format .` first.

- [ ] **Step 6: Commit**

```bash
git add src/registry/ingestion/sirene_diff.py tests/ingestion/test_sirene_diff.py
git commit -m "feat: orchestrate the daily Sirene diff extraction"
```

---

### Task 5: Airflow Docker image

**Files:**
- Create: `Dockerfile.airflow`

- [ ] **Step 1: Create `Dockerfile.airflow`**

```dockerfile
FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY pyproject.toml /opt/airflow/project/pyproject.toml
COPY src /opt/airflow/project/src
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt" \
    /opt/airflow/project
```

- [ ] **Step 2: Build the image**

Run: `docker build -f Dockerfile.airflow -t registry-airflow:local .`
Expected: builds successfully, exits 0. This installs a JDK (~300MB) and this
project's dependencies (pyspark, boto3, pyarrow, requests) — allow several minutes.

- [ ] **Step 3: Smoke-check the image**

Run:
```bash
docker run --rm --entrypoint bash registry-airflow:local -c "java -version"
docker run --rm registry-airflow:local python -c "import registry.ingestion.sirene_diff; import pyspark; print('ok')"
```
Expected: both print successfully (`openjdk version ...` and `ok`). The first command
needs `--entrypoint bash` — the base image's own entrypoint otherwise treats `java`
as an (invalid) Airflow CLI subcommand rather than running it directly.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile.airflow
git commit -m "feat: add Airflow Docker image with JDK and project dependencies"
```

---

### Task 6: Airflow + Postgres via Docker Compose

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env.example`

- [ ] **Step 1: Append the Postgres and Airflow services to `docker-compose.yml`**

Add to the `services:` section (alongside the existing `garage` service):

```yaml
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow-init:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    entrypoint: /bin/bash
    command: -c "airflow db migrate && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"

  airflow-webserver:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      S3_ENDPOINT_URL: ${S3_ENDPOINT_URL}
      S3_ACCESS_KEY: ${S3_ACCESS_KEY}
      S3_SECRET_KEY: ${S3_SECRET_KEY}
      S3_REGION: ${S3_REGION}
      LAKEHOUSE_BUCKET: ${LAKEHOUSE_BUCKET}
      SIRENE_API_KEY: ${SIRENE_API_KEY}
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "8080:8080"
    command: webserver

  airflow-scheduler:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      S3_ENDPOINT_URL: ${S3_ENDPOINT_URL}
      S3_ACCESS_KEY: ${S3_ACCESS_KEY}
      S3_SECRET_KEY: ${S3_SECRET_KEY}
      S3_REGION: ${S3_REGION}
      LAKEHOUSE_BUCKET: ${LAKEHOUSE_BUCKET}
      SIRENE_API_KEY: ${SIRENE_API_KEY}
    volumes:
      - ./dags:/opt/airflow/dags
    command: scheduler
```

Add `postgres_data:` to the `volumes:` section (alongside the existing
`garage_data:`).

- [ ] **Step 2: Append to `.env.example`**

```
AIRFLOW_UID=50000
# Free signup at https://portail-api.insee.fr to get a Sirene API key.
SIRENE_API_KEY=
```

- [ ] **Step 3: Bring up Postgres and initialize Airflow**

```bash
cp .env.example .env   # only if .env does not already exist from Plan 1
docker compose up -d postgres
docker compose up airflow-init
```

Expected: `airflow-init` logs show `Database migrating done!` and the admin user
created, then the container exits with code 0 (`docker compose ps -a` shows
`airflow-init` as `Exited (0)`).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: add Airflow and Postgres services via docker compose"
```

---

### Task 7: The DAG

**Files:**
- Create: `dags/sirene_daily_pipeline.py`

**Note:** per the "Decisions made" section above, this file has no automated pytest
coverage — its correctness is verified manually in Task 9.

- [ ] **Step 1: Create `dags/sirene_daily_pipeline.py`**

```python
"""Daily DAG: Sirene API diff -> bronze -> silver -> gold (SCD2 merge)."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="sirene_daily_pipeline",
    schedule="@daily",
    start_date=dt.datetime(2026, 10, 1),
    catchup=False,
)
def sirene_daily_pipeline():
    @task
    def extract_daily_diff(data_interval_start=None, data_interval_end=None) -> str:
        from registry.ingestion.sirene_diff import run_daily_diff

        return run_daily_diff(
            api_key=os.environ["SIRENE_API_KEY"],
            bucket=os.environ["LAKEHOUSE_BUCKET"],
            since=data_interval_start.date(),
            until=data_interval_end.date(),
            work_dir=Path("/tmp"),
        )

    @task
    def transform_bronze_to_silver(bronze_key: str) -> str:
        from registry.transform.bronze_to_silver import bronze_to_silver
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        silver_table = "lakehouse.silver.sirene_etablissements"
        spark = build_lakehouse_session()
        try:
            bronze_to_silver(spark, f"s3a://{bucket}/{bronze_key}", silver_table)
        finally:
            spark.stop()
        return silver_table

    @task
    def transform_silver_to_gold(silver_table: str, logical_date=None) -> None:
        from registry.transform.silver_to_gold import silver_to_gold
        from registry.transform.spark_session import build_lakehouse_session

        gold_table = "lakehouse.gold.sirene_etablissements_historized"
        spark = build_lakehouse_session()
        try:
            silver_to_gold(spark, silver_table, gold_table, logical_date.date())
        finally:
            spark.stop()

    bronze_key = extract_daily_diff()
    silver_table = transform_bronze_to_silver(bronze_key)
    transform_silver_to_gold(silver_table)


sirene_daily_pipeline()
```

(The `registry.*` imports are inside each task function, not at module level — this
is standard Airflow practice: the DAG *file* is parsed frequently by the scheduler,
so heavy imports like `pyspark` are deferred to task execution time to keep DAG
parsing fast.)

- [ ] **Step 2: Commit**

```bash
git add dags/sirene_daily_pipeline.py
git commit -m "feat: add the daily Sirene orchestration DAG"
```

---

### Task 8: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite and lint**

Run: `uv run pytest -v && uv run ruff check . && uv run ruff format --check .`
Expected: `23 passed` (17 from Plans 1-2 plus 6 from Task 1-4 of this plan) and
`All checks passed!`. This plan added no new local Python dependencies — Airflow
lives only inside `Dockerfile.airflow` — so nothing else should have changed.

---

### Task 9: Manual verification — Airflow infra and DAG wiring (executable now)

**Files:** none (manual verification only)

- [ ] **Step 1: Start the full stack**

```bash
docker compose up -d garage postgres airflow-webserver airflow-scheduler
docker compose ps
```

Expected: all four services show as `running` (or `healthy` for `garage`/`postgres`).
Give the webserver ~30s to finish starting.

- [ ] **Step 2: Confirm the DAG parses with no import errors**

```bash
docker compose exec airflow-webserver airflow dags list-import-errors
```

Expected: empty output (no import errors). If this prints an error, the DAG file or
the image's dependency install has a problem — fix it before continuing; do not
proceed to triggering a run with a broken import.

- [ ] **Step 3: Confirm the DAG is registered**

```bash
docker compose exec airflow-webserver airflow dags list | grep sirene_daily_pipeline
```

Expected: prints a line for `sirene_daily_pipeline`.

- [ ] **Step 4: Unpause the DAG**

New DAGs are paused by default on creation — the scheduler will not execute a
paused DAG's runs even if one is queued.

```bash
docker compose exec airflow-webserver airflow dags unpause sirene_daily_pipeline
```

Expected: prints `sirene_daily_pipeline | False` under `is_paused`.

- [ ] **Step 5: Trigger a manual run and observe the expected failure point**

```bash
docker compose exec airflow-webserver airflow dags trigger sirene_daily_pipeline
```

Then check task status after a minute:

```bash
docker compose exec airflow-webserver airflow tasks states-for-dag-run sirene_daily_pipeline <run_id>
```

(`<run_id>` is printed by the `trigger` command, or found via `airflow dags
list-runs -d sirene_daily_pipeline`.)

Expected: `extract_daily_diff` fails, and `transform_bronze_to_silver` /
`transform_silver_to_gold` both show `upstream_failed` (confirming the DAG
correctly refused to run downstream tasks on a failed upstream, rather than
running them with a bad/missing `bronze_key`). The failure itself is correct
without a real `SIRENE_API_KEY` (Task 10 below) — what this step actually
verifies is that the DAG scheduled the run, resolved `data_interval_start`/
`data_interval_end`, executed the first task, and propagated the failure
downstream correctly: the orchestration wiring works.

Confirm the failure reason in the task log is a `requests.exceptions.HTTPError:
401 Client Error: Unauthorized for url: https://api.insee.fr/...` (or a
`KeyError: 'SIRENE_API_KEY'` if the variable is entirely unset rather than
empty) — not an import error or an unrelated Python exception, which would
indicate a real bug rather than the expected missing-credential gap.

`airflow tasks logs` is not a valid subcommand on this Airflow version — read
the log file directly instead (each task/run gets its own file):

```bash
docker compose exec airflow-scheduler find /opt/airflow/logs/dag_id=sirene_daily_pipeline -name "*.log"
docker compose exec airflow-scheduler cat "/opt/airflow/logs/dag_id=sirene_daily_pipeline/run_id=<run_id>/task_id=extract_daily_diff/attempt=1.log"
```

Note this must run against `airflow-scheduler`, not `airflow-webserver` — task
logs are written to the executing container's local filesystem, and this
Compose setup does not mount a shared log volume between the two.

---

### Task 10: Manual verification — full pipeline against the real Sirene API (deferred)

**Files:** none (manual verification only)

**This task requires a Sirene API key and is not required to consider this plan
complete.** Do this once a key is obtained from https://portail-api.insee.fr.

- [ ] **Step 1: Set the real API key**

Edit `.env` and set `SIRENE_API_KEY` to the real key, then:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

- [ ] **Step 2: Verify the live API call shape before trusting a full run**

Before triggering the DAG, sanity-check the endpoint and response shape assumed by
`fetch_sirene_updates` (Task 2) against the real API directly:

```bash
curl -s -H "X-INSEE-Api-Key-Integration: $SIRENE_API_KEY" \
  "https://api.insee.fr/api-sirene/3.11/siret?q=dateDernierTraitementEtablissement:%5B2026-10-01%20TO%202026-10-02%5D&nombre=1" | python3 -m json.tool
```

Compare the printed JSON structure against what `normalize_etablissement` (Task 1)
expects (`adresseEtablissement.*`, `periodesEtablissement[0].*`,
`header.curseurSuivant`). If the real API's field names, nesting, or endpoint
version differ from this plan's assumptions, update `SIRENE_API_URL` and/or
`normalize_etablissement` to match before proceeding — this was flagged as
unverified in the "Decisions made" section.

- [ ] **Step 3: Trigger the DAG and verify the full path**

```bash
docker compose exec airflow-webserver airflow dags trigger sirene_daily_pipeline
```

Expected: all three tasks succeed. Confirm the gold table was updated:

```bash
set -a && source .env && set +a
uv run python -c "
from registry.transform.spark_session import build_lakehouse_session
spark = build_lakehouse_session()
spark.table('lakehouse.gold.sirene_etablissements_historized').orderBy('siret', 'valid_from').show(truncate=False)
spark.stop()
"
```
