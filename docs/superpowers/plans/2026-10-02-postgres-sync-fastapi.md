# Postgres Sync and FastAPI Serving Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out Step 1 with a serving layer: sync the historized gold table into
Postgres, then expose it through a small FastAPI service (search an establishment,
view its version history) — the lakehouse stays the analytical/batch store, Postgres
becomes the low-latency point-lookup store the API actually talks to.

**Architecture:** A sync job (`registry.serving.sync_gold_to_postgres`) reads the
gold Iceberg table via PySpark and writes it to a Postgres table via the Spark JDBC
writer (truncate + overwrite — simple and plenty fast at this data volume). The
FastAPI app (`registry.api`) never touches Spark or Iceberg directly; it queries
Postgres through a small repository class behind a `Protocol`, which is what makes
the endpoints unit-testable without a real database (FastAPI dependency injection +
a fake repository in tests).

**Tech Stack:** FastAPI, Uvicorn, `psycopg2-binary`, the PostgreSQL JDBC driver
(Spark-side only), PostgreSQL 16 (already running since Plan 3, as a second
database in the same instance).

This is Plan 5 of 5 — the last plan for Step 1 (SIRENE lakehouse pipeline) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` for the full design,
Plan 2 (`2026-10-01-spark-bronze-silver-gold.md`) for the gold table this syncs, and
Plan 3 (`2026-10-02-airflow-daily-orchestration.md`) for the Postgres instance this
reuses. Entity resolution against BODACC/DECP (Steps 2-3) is out of scope here.

## Decisions made

- **Refinement of the spec's "current state only" sync decision:** the original
  design said the Postgres sync would materialize only `is_current = true` rows.
  Building the API now surfaces why that's incomplete — the API must also serve
  version *history* (`GET /etablissements/{siret}/history`), and querying Iceberg
  live from the API for that would mean two different query paths (Postgres for
  search, Spark for history) for no real benefit at this data volume. The sync now
  copies the **entire historized gold table** into Postgres — still a simple,
  fast point-lookup store at this scale, just with all versions instead of only
  the current one. Search still filters `WHERE is_current = true`.
- **Sync mode: truncate + overwrite, not incremental upsert.** At this project's
  scale (hundreds to low thousands of establishments), re-copying the whole table
  every run is simpler than tracking what changed, and "sync" only ever runs after
  a full `silver_to_gold` pass anyway. `truncate=true` (not drop-and-recreate) is
  used specifically so the table's indexes survive repeated syncs.
- **The sync job (`ensure_postgres_table`, `sync_gold_to_postgres`) has no
  automated pytest coverage**, for the same reason Plan 3's Airflow DAG doesn't:
  it's thin wiring (read a table, write a table) around two already-tested or
  battle-tested systems (Plan 2's gold table, Postgres itself), with no business
  logic of its own to unit-test. It's verified manually against the real Garage
  gold table and the real Postgres instance (Task 7).
- **The FastAPI layer *is* unit-tested**, unlike the sync job — because FastAPI's
  dependency injection gives a clean, idiomatic seam for it: the endpoints depend
  on an `EtablissementRepository` `Protocol`, and tests substitute a fake
  in-memory implementation via `app.dependency_overrides`. This tests real routing,
  response serialization, and error handling (404s) without touching a database.
- **Postgres connections are opened per-request, with no pooling.** At this
  project's traffic (a local demo API, not a production service), connection
  pooling would be solving a problem that doesn't exist yet.
- **FastAPI runs via `uvicorn` directly on the host, not in a new Docker
  container.** Every other piece of long-running infra in this project (Garage,
  Postgres, Airflow) earned its container because it's a persistent service other
  components depend on; a dev-mode API server for local demoing doesn't need one.
- **The serving Postgres database (`registry`) is a second database in the same
  Postgres instance** Plan 3 introduced for Airflow's metadata — not a new
  container. An init script provisions it on fresh deployments; this environment's
  already-initialized container needs one manual `CREATE DATABASE` (Task 2), since
  Postgres only runs `/docker-entrypoint-initdb.d/` scripts on a brand-new data
  directory.

## Architecture

```
lakehouse.gold.sirene_etablissements_historized (Iceberg, Garage)
          │
          │  sync_gold_to_postgres (PySpark, JDBC writer, truncate+overwrite)
          ▼
postgres: registry.sirene_etablissements_historized (all versions, indexed on siret)
          │
          │  PostgresEtablissementRepository (psycopg2)
          ▼
FastAPI (registry.api.main)
  GET /health
  GET /etablissements/search?q=...          -> current rows matching siret/siren/commune
  GET /etablissements/{siret}/history        -> all versions for one establishment, or 404
```

---

### Task 1: Dependencies and environment variables

**Files:**
- Modify: `pyproject.toml`
- Modify: `.env.example`
- Modify: `.env`

- [ ] **Step 1: Add dependencies**

Edit `pyproject.toml`'s `dependencies` list:

```toml
dependencies = [
    "boto3>=1.34",
    "pyarrow>=16.0",
    "requests>=2.31",
    "pyspark==3.5.3",
    "dbt-spark[session]>=1.11,<1.12",
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "psycopg2-binary>=2.9",
]
```

Add to the `dev` group in `[dependency-groups]` (needed for FastAPI's `TestClient`):

```toml
dev = [
    "pytest>=8.0",
    "ruff>=0.6",
    "moto[s3]>=5.0",
    "responses>=0.25",
    "httpx>=0.27",
]
```

Run: `uv sync --all-groups`
Expected: resolves and installs `fastapi`, `uvicorn`, `psycopg2-binary`, `httpx`
(already verified to resolve cleanly alongside the rest of this project's pinned
dependencies before writing this plan). Exits 0.

- [ ] **Step 2: Add Postgres serving connection variables**

Append to `.env.example`:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_SERVING_DB=registry
```

(Reuses the `airflow`/`airflow` credentials Plan 3 already set up for the Postgres
container — this is a second *database* inside the same instance, not a new user.)

Append the same lines to `.env` (not committed):

```bash
cat >> .env << 'EOF'
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_SERVING_DB=registry
EOF
```

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock .env.example
git commit -m "feat: add FastAPI, psycopg2, and serving Postgres env vars"
```

---

### Task 2: Provision the serving database

**Files:**
- Create: `postgres/init-registry-db.sql`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Create the init script (for fresh deployments)**

```sql
CREATE DATABASE registry;
```

- [ ] **Step 2: Mount it into the postgres service**

In `docker-compose.yml`'s `postgres` service, add to `volumes:`:

```yaml
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./postgres/init-registry-db.sql:/docker-entrypoint-initdb.d/init-registry-db.sql:ro
```

(Postgres only runs scripts in `/docker-entrypoint-initdb.d/` the first time it
initializes an *empty* data directory — this makes a fresh `docker compose up`
provision `registry` automatically, but has no effect on this environment's
already-initialized container, hence Step 3.)

- [ ] **Step 3: Create the database on the already-running container**

```bash
docker compose up -d postgres
docker compose exec postgres psql -U airflow -d airflow -c "CREATE DATABASE registry;"
```

Expected: `CREATE DATABASE` (or, if already run before, an error that it already
exists — harmless, confirms the state is as intended).

- [ ] **Step 4: Verify**

```bash
docker compose exec postgres psql -U airflow -d registry -c "\conninfo"
```

Expected: confirms a successful connection to the `registry` database.

- [ ] **Step 5: Commit**

```bash
git add postgres/init-registry-db.sql docker-compose.yml
git commit -m "feat: provision a registry serving database in Postgres"
```

---

### Task 3: Sync job

**Files:**
- Create: `src/registry/serving/__init__.py`
- Create: `src/registry/serving/sync_gold_to_postgres.py`

**Note:** no automated test for this task — see "Decisions made" above. It is
verified manually in Task 7, against the real Garage gold table and the real
Postgres instance.

- [ ] **Step 1: Create the package**

`src/registry/serving/__init__.py`:
```python
```

- [ ] **Step 2: Create `sync_gold_to_postgres.py`**

```python
"""Sync the historized gold table into Postgres for low-latency API serving."""

from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession

from registry.transform.spark_session import lakehouse_spark_configs

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

GOLD_TABLE = "lakehouse.gold.sirene_etablissements_historized"
POSTGRES_TABLE = "sirene_etablissements_historized"


def build_sync_session(app_name: str = "registry-sync") -> SparkSession:
    configs = lakehouse_spark_configs()
    configs["spark.jars.packages"] = f"{configs['spark.jars.packages']},{POSTGRES_JDBC_PACKAGE}"
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def build_jdbc_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_SERVING_DB", "registry")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def build_psycopg2_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_SERVING_DB', 'registry')} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )


def ensure_postgres_table(dsn: str) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
                siren TEXT,
                nic TEXT,
                siret TEXT,
                statut_diffusion TEXT,
                date_creation DATE,
                etablissement_siege BOOLEAN,
                numero_voie TEXT,
                type_voie TEXT,
                libelle_voie TEXT,
                code_postal TEXT,
                libelle_commune TEXT,
                activite_principale TEXT,
                etat_administratif TEXT,
                date_dernier_traitement DATE,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{POSTGRES_TABLE}_siret "
            f"ON {POSTGRES_TABLE} (siret)"
        )
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{POSTGRES_TABLE}_siret_valid_from "
            f"ON {POSTGRES_TABLE} (siret, valid_from)"
        )
        conn.commit()


def sync_gold_to_postgres(spark: SparkSession, jdbc_url: str, pg_user: str, pg_password: str) -> int:
    df = spark.table(GOLD_TABLE)
    df.write.option("truncate", "true").jdbc(
        url=jdbc_url,
        table=POSTGRES_TABLE,
        mode="overwrite",
        properties={
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        },
    )
    return df.count()


def run_sync(spark: SparkSession) -> int:
    ensure_postgres_table(build_psycopg2_dsn())
    return sync_gold_to_postgres(
        spark, build_jdbc_url(), os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"]
    )
```

- [ ] **Step 3: Commit**

```bash
git add src/registry/serving/__init__.py src/registry/serving/sync_gold_to_postgres.py
git commit -m "feat: sync the historized gold table into Postgres"
```

---

### Task 4: API repository

**Files:**
- Create: `src/registry/api/__init__.py`
- Create: `src/registry/api/repository.py`

**Note:** no automated test for `PostgresEtablissementRepository` — same reasoning
as Task 3 (thin wiring around psycopg2, no business logic). The `EtablissementRepository`
protocol it implements is what Task 5's tests substitute a fake for.

- [ ] **Step 1: Create the package**

`src/registry/api/__init__.py`:
```python
```

- [ ] **Step 2: Create `repository.py`**

```python
"""Data access for the SIRENE establishment registry serving store."""

from __future__ import annotations

import os
from typing import Protocol

import psycopg2
import psycopg2.extras

POSTGRES_TABLE = "sirene_etablissements_historized"


class EtablissementRepository(Protocol):
    def search(self, q: str) -> list[dict]: ...
    def history(self, siret: str) -> list[dict]: ...


class PostgresEtablissementRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def search(self, q: str) -> list[dict]:
        pattern = f"%{q}%"
        query = f"""
            SELECT * FROM {POSTGRES_TABLE}
            WHERE is_current = true
              AND (siret = %(q)s OR siren = %(q)s OR libelle_commune ILIKE %(pattern)s)
            ORDER BY siret
            LIMIT 50
        """
        return self._fetch(query, {"q": q, "pattern": pattern})

    def history(self, siret: str) -> list[dict]:
        query = f"""
            SELECT * FROM {POSTGRES_TABLE}
            WHERE siret = %(siret)s
            ORDER BY valid_from
        """
        return self._fetch(query, {"siret": siret})

    def _fetch(self, query: str, params: dict) -> list[dict]:
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]


def build_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_SERVING_DB', 'registry')} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )
```

- [ ] **Step 3: Commit**

```bash
git add src/registry/api/__init__.py src/registry/api/repository.py
git commit -m "feat: add Postgres-backed repository for the establishment registry"
```

---

### Task 5: FastAPI app

**Files:**
- Create: `src/registry/api/main.py`
- Test: `tests/api/__init__.py`
- Test: `tests/api/test_main.py`

- [ ] **Step 1: Write the failing tests**

`tests/api/__init__.py`:
```python
```

`tests/api/test_main.py`:
```python
import datetime as dt

from fastapi.testclient import TestClient

from registry.api.main import app, get_repository

SAMPLE_ROW = {
    "siren": "552032534",
    "nic": "00019",
    "siret": "55203253400019",
    "statut_diffusion": "O",
    "date_creation": dt.date(1966, 1, 1),
    "etablissement_siege": True,
    "numero_voie": "8",
    "type_voie": "RUE",
    "libelle_voie": "DE LA PAIX",
    "code_postal": "75002",
    "libelle_commune": "PARIS",
    "activite_principale": "70.10Z",
    "etat_administratif": "A",
    "date_dernier_traitement": dt.date(2023, 5, 12),
    "valid_from": dt.date(2026, 10, 1),
    "valid_to": None,
    "is_current": True,
}


class FakeRepository:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def search(self, q: str) -> list[dict]:
        return [r for r in self._rows if r["siret"] == q or r["siren"] == q]

    def history(self, siret: str) -> list[dict]:
        return [r for r in self._rows if r["siret"] == siret]


def test_health_returns_ok():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_search_returns_matching_establishment():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/search", params={"q": "55203253400019"})

    assert response.status_code == 200
    assert response.json()[0]["siret"] == "55203253400019"
    app.dependency_overrides.clear()


def test_search_returns_empty_list_for_no_match():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/search", params={"q": "00000000000000"})

    assert response.status_code == 200
    assert response.json() == []
    app.dependency_overrides.clear()


def test_history_returns_404_for_unknown_siret():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/00000000000000/history")

    assert response.status_code == 404
    app.dependency_overrides.clear()


def test_history_returns_versions_for_known_siret():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/55203253400019/history")

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["siret"] == "55203253400019"
    app.dependency_overrides.clear()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/api/test_main.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.api.main'`

- [ ] **Step 3: Implement `main.py`**

```python
"""FastAPI service exposing the SIRENE establishment registry."""

from __future__ import annotations

import datetime as dt

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from registry.api.repository import EtablissementRepository, PostgresEtablissementRepository, build_dsn

app = FastAPI(title="French Business Registry API")


class Etablissement(BaseModel):
    siren: str
    nic: str
    siret: str
    statut_diffusion: str | None = None
    date_creation: dt.date | None = None
    etablissement_siege: bool | None = None
    numero_voie: str | None = None
    type_voie: str | None = None
    libelle_voie: str | None = None
    code_postal: str | None = None
    libelle_commune: str | None = None
    activite_principale: str | None = None
    etat_administratif: str | None = None
    date_dernier_traitement: dt.date | None = None
    valid_from: dt.date
    valid_to: dt.date | None = None
    is_current: bool


def get_repository() -> EtablissementRepository:
    return PostgresEtablissementRepository(build_dsn())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/etablissements/search", response_model=list[Etablissement])
def search_etablissements(
    q: str, repo: EtablissementRepository = Depends(get_repository)
) -> list[dict]:
    return repo.search(q)


@app.get("/etablissements/{siret}/history", response_model=list[Etablissement])
def get_history(
    siret: str, repo: EtablissementRepository = Depends(get_repository)
) -> list[dict]:
    rows = repo.history(siret)
    if not rows:
        raise HTTPException(status_code=404, detail="Establishment not found")
    return rows
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/api/test_main.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/api/main.py tests/api/__init__.py tests/api/test_main.py
git commit -m "feat: add FastAPI endpoints for establishment search and history"
```

---

### Task 6: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite and lint**

```bash
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Expected: all 28 tests pass (23 from Plans 1-4 plus 5 new from Task 5), lint and
format clean.

---

### Task 7: Manual verification — sync against the real lakehouse

**Files:** none (manual verification only)

- [ ] **Step 1: Ensure Garage and Postgres are running**

```bash
docker compose up -d garage postgres
set -a && source .env && set +a
```

- [ ] **Step 2: Run the sync**

```bash
uv run python -c "
from registry.serving.sync_gold_to_postgres import build_sync_session, run_sync

spark = build_sync_session()
count = run_sync(spark)
print(f'Synced {count} rows')
spark.stop()
"
```

Expected: prints `Synced 3 rows` (the 2 untouched establishments plus the extra
historized version Plan 2's manual verification created for the changed one — 3
total rows across 2 establishments).

- [ ] **Step 3: Verify directly in Postgres**

```bash
docker compose exec postgres psql -U airflow -d registry -c \
  "SELECT siret, numero_voie, valid_from, valid_to, is_current FROM sirene_etablissements_historized ORDER BY siret, valid_from;"
```

Expected: 3 rows, matching Plan 2's manual verification output exactly (one siret
with two versions — `numero_voie` `8` then `99` — the other with one).

- [ ] **Step 4: Confirm idempotency**

```bash
uv run python -c "
from registry.serving.sync_gold_to_postgres import build_sync_session, run_sync
spark = build_sync_session()
print(f'Synced {run_sync(spark)} rows')
spark.stop()
"
docker compose exec postgres psql -U airflow -d registry -c "SELECT count(*) FROM sirene_etablissements_historized;"
```

Expected: still 3 rows — re-running the sync doesn't duplicate anything (truncate
+ overwrite semantics).

---

### Task 8: Manual verification — the API against real data

**Files:** none (manual verification only)

- [ ] **Step 1: Start the API**

```bash
set -a && source .env && set +a
uv run uvicorn registry.api.main:app --reload --port 8000
```

Expected: starts without errors, logs `Uvicorn running on http://127.0.0.1:8000`.

- [ ] **Step 2: Health check**

```bash
curl -s http://localhost:8000/health
```

Expected: `{"status":"ok"}`.

- [ ] **Step 3: Search**

```bash
curl -s "http://localhost:8000/etablissements/search?q=PARIS" | python3 -m json.tool
```

Expected: both establishments (both are in Paris), each showing only their
*current* version (`is_current: true`).

```bash
curl -s "http://localhost:8000/etablissements/search?q=73282932000014" | python3 -m json.tool
```

Expected: exactly one result, the unchanged establishment.

- [ ] **Step 4: History**

```bash
curl -s "http://localhost:8000/etablissements/55203253400019/history" | python3 -m json.tool
```

Expected: 2 entries — `numero_voie: "8"` with `is_current: false`, then
`numero_voie: "99"` with `is_current: true` — the same changed establishment from
Plan 2's manual verification, now served through the API.

```bash
curl -s -o /dev/null -w "%{http_code}\n" "http://localhost:8000/etablissements/00000000000000/history"
```

Expected: `404`.

- [ ] **Step 5: Stop the server**

Ctrl-C the `uvicorn` process.

---

### Task 9: Close out Step 1 — update the README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a "Running the pipeline" section**

Append to `README.md`:

```markdown
## Running the pipeline

This is Step 1 (Data Engineering) of a three-project portfolio — see
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` for the design and
`docs/superpowers/plans/` for the implementation plans this was built from.

1. **Infra**: `cp .env.example .env` (fill in the generated values per Plan 1's
   Task 10), then `docker compose up -d garage postgres airflow-webserver
   airflow-scheduler`.
2. **Bootstrap** (once): lands the full SIRENE stock file in the bronze layer —
   see Plan 1, Task 10.
3. **Transform**: bronze -> silver -> gold (SCD2) via `registry.transform` — see
   Plan 2.
4. **Orchestrate**: the `sirene_daily_pipeline` Airflow DAG runs steps 2-3 daily
   against the real Sirene API once `SIRENE_API_KEY` is set — see Plan 3.
5. **Test and document the gold table**: `cd dbt && uv run dbt test
   --profiles-dir . --target prod` — see Plan 4.
6. **Serve**: sync gold into Postgres and run the API —

   ```bash
   uv run python -c "
   from registry.serving.sync_gold_to_postgres import build_sync_session, run_sync
   spark = build_sync_session()
   run_sync(spark)
   spark.stop()
   "
   uv run uvicorn registry.api.main:app --port 8000
   ```

   Then `curl "http://localhost:8000/etablissements/search?q=<siret or commune>"`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add end-to-end pipeline instructions to the README"
```
