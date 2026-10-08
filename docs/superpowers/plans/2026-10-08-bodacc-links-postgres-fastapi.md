# BODACC Links Postgres Sync + FastAPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve BODACC legal announcements through the existing FastAPI app — `GET /etablissements/{siret}/annonces-legales` — the way Step 1's Plan 5 served the SIRENE registry: sync a table into Postgres, query it through a small repository behind a `Protocol`.

**Architecture:** A new sync job joins `gold.bodacc_sirene_links` with `silver.bodacc_annonces` (the match metadata plus the announcement's actual content — date, type, tribunal, denomination) into one denormalized Postgres table, `bodacc_annonces_legales`. A new repository class queries it by `siret_siege`. Both plug into the *same* FastAPI app and Postgres instance Plan 5 already built — no new infrastructure, no new dependencies.

**Tech Stack:** Same as Plan 5 — PySpark JDBC writer, `psycopg2`, FastAPI, the existing `registry` Postgres database.

This is the last of the three sub-projects identified after the BODACC matching orchestration plan shipped (the other two — orchestration, dbt tests — are already done). See `docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md`'s architecture diagram (`sync to Postgres (same pattern as Step 1) ──► FastAPI: GET /etablissements/{siret}/annonces-legales`) and Step 1's Plan 5 (`2026-10-02-postgres-sync-fastapi.md`) for the pattern this mirrors.

## Decisions made

- **Sync a join, not just the links table.** `gold.bodacc_sirene_links` alone has no announcement content (no date, no denomination, no tribunal) — just match metadata. An endpoint that returned only announcement IDs and confidence scores wouldn't be a usable "legal announcements" endpoint for the project's stated KYC/due-diligence use case. The sync job joins `bodacc_sirene_links` to `silver.bodacc_annonces` on `bodacc_announcement_id = id` and writes one denormalized Postgres table. Same sync complexity as Plan 5 (one Spark job, one `truncate+overwrite` write) — the only difference is the query that builds the DataFrame.
- **Only resolved links are synced** (`WHERE siret_siege IS NOT NULL`, via an inner join). `unresolved` rows have no `siret_siege` to key a per-establishment endpoint on, so there's nothing to join them to; they stay queryable in the lakehouse (e.g. for the evaluation work) but have no reason to exist in a table whose whole purpose is siret lookup.
- **No new environment variables or Postgres database.** This reuses the exact same `registry` database and `POSTGRES_*` credentials Plan 5 already provisioned — a second table in an already-existing database, not new infrastructure.
- **The sync job has no automated pytest coverage**, same reasoning as Plan 5's `sync_gold_to_postgres.py`: thin wiring (read two tables, join, write one table) around already-tested systems, no business logic of its own. Verified manually (Task 6, deferred).
- **The new repository *is* unit-tested**, same reasoning as Plan 5's `PostgresEtablissementRepository` — FastAPI's dependency injection gives a clean seam to substitute a fake.
- **An empty result is `200` with `[]`, not `404`** — unlike `/etablissements/{siret}/history`. Most establishments legitimately have zero BODACC announcements ever; that's a normal state, not a "resource not found" error. `404` stays reserved for `/history`, where an unknown `siret` really does mean "no such establishment in the registry."
- **Both repository classes' identical `_fetch` helper is extracted into one module-level function** in `repository.py`. This plan would otherwise introduce a second byte-for-byte copy of it — a small, directly-motivated deduplication, not a speculative abstraction.
- **The two sync jobs stay separate, independently runnable functions** (`sync_gold_to_postgres.run_sync` and the new `sync_bodacc_links_to_postgres.run_sync`), not combined into one entrypoint — each syncs a different gold table on its own schedule, and combining them would just be premature coupling.

## Architecture

```
lakehouse.gold.bodacc_sirene_links  ─┐
                                      ├─ JOIN (bodacc_announcement_id = id)
lakehouse.silver.bodacc_annonces    ─┘
          │
          │  sync_bodacc_links_to_postgres (PySpark, JDBC writer, truncate+overwrite)
          ▼
postgres: registry.bodacc_annonces_legales (resolved links only, indexed on siret_siege)
          │
          │  PostgresAnnoncesLegalesRepository (psycopg2)
          ▼
FastAPI (registry.api.main)
  GET /etablissements/{siret}/annonces-legales  -> current announcements linked to this siret, or []
```

---

### Task 1: Sync job

**Files:**
- Create: `src/registry/serving/sync_bodacc_links_to_postgres.py`

No automated test — see "Decisions made". Verified manually in Task 6 (deferred).

- [ ] **Step 1: Create the sync job**

```python
"""Sync the joined BODACC links + announcement content into Postgres for the
/annonces-legales endpoint. Reuses the generic session/connection helpers from
sync_gold_to_postgres.py — only the query and target table are specific here."""

from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession

from registry.serving.sync_gold_to_postgres import build_jdbc_url, build_psycopg2_dsn

BODACC_LINKS_POSTGRES_TABLE = "bodacc_annonces_legales"


def ensure_bodacc_links_postgres_table(dsn: str) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {BODACC_LINKS_POSTGRES_TABLE} (
                siret_siege TEXT,
                bodacc_announcement_id TEXT,
                siren_bodacc TEXT,
                match_method TEXT,
                match_confidence DOUBLE PRECISION,
                date_parution DATE,
                type_avis TEXT,
                famille_avis TEXT,
                tribunal TEXT,
                commercant TEXT,
                denomination TEXT,
                ville TEXT,
                code_postal TEXT,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{BODACC_LINKS_POSTGRES_TABLE}_siret_siege "
            f"ON {BODACC_LINKS_POSTGRES_TABLE} (siret_siege)"
        )
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"uq_{BODACC_LINKS_POSTGRES_TABLE}_announcement_valid_from "
            f"ON {BODACC_LINKS_POSTGRES_TABLE} (bodacc_announcement_id, valid_from)"
        )
        conn.commit()


def build_joined_links_df(spark: SparkSession):
    return spark.sql("""
        SELECT
            links.siret_siege,
            links.bodacc_announcement_id,
            links.siren_bodacc,
            links.match_method,
            links.match_confidence,
            a.date_parution,
            a.type_avis,
            a.famille_avis,
            a.tribunal,
            a.commercant,
            a.denomination,
            a.ville,
            a.code_postal,
            links.valid_from,
            links.valid_to,
            links.is_current
        FROM lakehouse.gold.bodacc_sirene_links AS links
        JOIN lakehouse.silver.bodacc_annonces AS a
          ON links.bodacc_announcement_id = a.id
        WHERE links.siret_siege IS NOT NULL
    """)


def sync_bodacc_links_to_postgres(
    spark: SparkSession, jdbc_url: str, pg_user: str, pg_password: str
) -> int:
    df = build_joined_links_df(spark)
    df.write.option("truncate", "true").jdbc(
        url=jdbc_url,
        table=BODACC_LINKS_POSTGRES_TABLE,
        mode="overwrite",
        properties={
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        },
    )
    return df.count()


def run_sync(spark: SparkSession) -> int:
    ensure_bodacc_links_postgres_table(build_psycopg2_dsn())
    return sync_bodacc_links_to_postgres(
        spark, build_jdbc_url(), os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"]
    )
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `uv run python -c "from registry.serving.sync_bodacc_links_to_postgres import run_sync"`
Expected: no output, exit code 0.

- [ ] **Step 3: Commit**

```bash
git add src/registry/serving/sync_bodacc_links_to_postgres.py
git commit -m "feat: sync joined BODACC links and announcement content into Postgres"
```

---

### Task 2: Repository

**Files:**
- Modify: `src/registry/api/repository.py`

No automated test for `PostgresAnnoncesLegalesRepository` — same reasoning as `PostgresEtablissementRepository` (thin wiring around psycopg2). The `AnnoncesLegalesRepository` protocol it implements is what Task 3's tests substitute a fake for.

- [ ] **Step 1: Extract the shared `_fetch` helper**

Replace in `src/registry/api/repository.py`:

```python
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
```

with:

```python
def _fetch(dsn: str, query: str, params: dict) -> list[dict]:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


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
        return _fetch(self._dsn, query, {"q": q, "pattern": pattern})

    def history(self, siret: str) -> list[dict]:
        query = f"""
            SELECT * FROM {POSTGRES_TABLE}
            WHERE siret = %(siret)s
            ORDER BY valid_from
        """
        return _fetch(self._dsn, query, {"siret": siret})
```

- [ ] **Step 2: Run the existing API tests to confirm the refactor didn't break anything**

Run: `uv run pytest tests/api/test_main.py -v`
Expected: `5 passed` (unchanged behavior, pure refactor).

- [ ] **Step 3: Add the BODACC links repository**

Add to `src/registry/api/repository.py`:

```python
BODACC_LINKS_POSTGRES_TABLE = "bodacc_annonces_legales"


class AnnoncesLegalesRepository(Protocol):
    def get_by_siret(self, siret: str) -> list[dict]: ...


class PostgresAnnoncesLegalesRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def get_by_siret(self, siret: str) -> list[dict]:
        query = f"""
            SELECT * FROM {BODACC_LINKS_POSTGRES_TABLE}
            WHERE siret_siege = %(siret)s AND is_current = true
            ORDER BY date_parution DESC
        """
        return _fetch(self._dsn, query, {"siret": siret})
```

- [ ] **Step 4: Commit**

```bash
git add src/registry/api/repository.py
git commit -m "feat: add Postgres-backed repository for BODACC legal announcements"
```

---

### Task 3: FastAPI endpoint

**Files:**
- Modify: `src/registry/api/main.py`
- Modify: `tests/api/test_main.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/api/test_main.py`:

```python
SAMPLE_ANNONCE = {
    "siret_siege": "55203253400019",
    "bodacc_announcement_id": "BX202500012345",
    "siren_bodacc": "552032534",
    "match_method": "exact_siren",
    "match_confidence": 1.0,
    "date_parution": dt.date(2025, 10, 15),
    "type_avis": "Jugement",
    "famille_avis": "Procedures collectives",
    "tribunal": "Tribunal de commerce de Paris",
    "commercant": "DUPONT BATIMENT SARL",
    "denomination": None,
    "ville": "PARIS",
    "code_postal": "75002",
    "valid_from": dt.date(2026, 10, 7),
    "valid_to": None,
    "is_current": True,
}


class FakeAnnoncesLegalesRepository:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def get_by_siret(self, siret: str) -> list[dict]:
        return [r for r in self._rows if r["siret_siege"] == siret]


def test_annonces_legales_returns_matching_rows():
    app.dependency_overrides[get_annonces_legales_repository] = (
        lambda: FakeAnnoncesLegalesRepository([SAMPLE_ANNONCE])
    )
    client = TestClient(app)

    response = client.get("/etablissements/55203253400019/annonces-legales")

    assert response.status_code == 200
    assert response.json()[0]["bodacc_announcement_id"] == "BX202500012345"
    app.dependency_overrides.clear()


def test_annonces_legales_returns_empty_list_for_siret_with_no_announcements():
    app.dependency_overrides[get_annonces_legales_repository] = (
        lambda: FakeAnnoncesLegalesRepository([SAMPLE_ANNONCE])
    )
    client = TestClient(app)

    response = client.get("/etablissements/00000000000000/annonces-legales")

    assert response.status_code == 200
    assert response.json() == []
    app.dependency_overrides.clear()
```

Update the import line at the top of the file:

```python
from registry.api.main import app, get_annonces_legales_repository, get_repository
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/api/test_main.py -v`
Expected: FAIL with `ImportError: cannot import name 'get_annonces_legales_repository'`

- [ ] **Step 3: Implement the endpoint**

Add to `src/registry/api/main.py`'s imports:

```python
from registry.api.repository import (
    AnnoncesLegalesRepository,
    EtablissementRepository,
    PostgresAnnoncesLegalesRepository,
    PostgresEtablissementRepository,
    build_dsn,
)
```

Add the new model and endpoint:

```python
class AnnonceLegale(BaseModel):
    siret_siege: str
    bodacc_announcement_id: str
    siren_bodacc: str | None = None
    match_method: str
    match_confidence: float | None = None
    date_parution: dt.date | None = None
    type_avis: str | None = None
    famille_avis: str | None = None
    tribunal: str | None = None
    commercant: str | None = None
    denomination: str | None = None
    ville: str | None = None
    code_postal: str | None = None
    valid_from: dt.date
    valid_to: dt.date | None = None
    is_current: bool


def get_annonces_legales_repository() -> AnnoncesLegalesRepository:
    return PostgresAnnoncesLegalesRepository(build_dsn())


@app.get("/etablissements/{siret}/annonces-legales", response_model=list[AnnonceLegale])
def get_annonces_legales(
    siret: str,
    repo: AnnoncesLegalesRepository = Depends(get_annonces_legales_repository),
) -> list[dict]:
    return repo.get_by_siret(siret)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/api/test_main.py -v`
Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/api/main.py tests/api/test_main.py
git commit -m "feat: add FastAPI endpoint for BODACC legal announcements by siret"
```

---

### Task 4: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite and lint**

```bash
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Expected: all 77 tests pass (75 pre-existing + 2 new in `test_main.py`), lint and format clean.

---

### Task 5: Manual verification — sync against real data — deferred

**Files:** none (deferred; same reasoning as every prior manual-verification task in this project)

This cannot run until the BODACC matching orchestration DAG has actually populated `gold.bodacc_sirene_links` with real department-08 data — still blocked on `SIRENE_API_KEY` and `SIRENE_CANDIDATES_BRONZE_KEY`.

- [ ] **Step 1:** Once real linked data exists, run:

```bash
docker compose up -d garage postgres
set -a && source .env && set +a
uv run python -c "
from registry.serving.sync_bodacc_links_to_postgres import run_sync
from registry.serving.sync_gold_to_postgres import build_sync_session

spark = build_sync_session()
count = run_sync(spark)
print(f'Synced {count} rows')
spark.stop()
"
```

- [ ] **Step 2:** Verify directly in Postgres:

```bash
docker compose exec postgres psql -U airflow -d registry -c \
  "SELECT siret_siege, match_method, date_parution, type_avis FROM bodacc_annonces_legales ORDER BY siret_siege, date_parution DESC LIMIT 20;"
```

---

### Task 6: Manual verification — the API against real data — deferred

**Files:** none (deferred; same blocker as Task 5)

- [ ] **Step 1:** Once Task 5 has synced real data:

```bash
set -a && source .env && set +a
uv run uvicorn registry.api.main:app --reload --port 8000
```

- [ ] **Step 2:**

```bash
curl -s "http://localhost:8000/etablissements/<a real siret from department 08>/annonces-legales" | python3 -m json.tool
```

Expected: a list of real BODACC announcements linked to that establishment, each with real `date_parution`, `type_avis`, `tribunal`, etc.

```bash
curl -s "http://localhost:8000/etablissements/00000000000000/annonces-legales"
```

Expected: `[]` with a `200` status (not `404`).

---

### Task 7: Close out Step 2 — update the README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a "Step 2: BODACC entity resolution" section**

Append to `README.md`:

```markdown
## Step 2: BODACC entity resolution

See `docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md` for the
design and `docs/superpowers/plans/` (2026-10-03 through 2026-10-08) for the
implementation plans this was built from.

1. **Ingest**: `registry.ingestion.bodacc` (bootstrap + daily diff) and
   `registry.matching.sirene_candidates` (department-scoped SIRENE candidates).
2. **Match and historize**: the `bodacc_matching_pipeline` Airflow DAG runs the
   exact/fuzzy/unresolved cascade daily against the backlog of unlinked
   announcements, merging results into `gold.bodacc_sirene_links` (SCD2).
3. **Test**: `cd dbt && uv run dbt test --profiles-dir . --target prod --select
   source:lakehouse.bodacc_sirene_links`.
4. **Evaluate**: `registry.matching.evaluation` runs the blind-holdout
   precision/recall measurement described in the spec.
5. **Serve**: sync the joined links+announcements into Postgres and query the API —

   ```bash
   uv run python -c "
   from registry.serving.sync_bodacc_links_to_postgres import run_sync
   from registry.serving.sync_gold_to_postgres import build_sync_session
   spark = build_sync_session()
   run_sync(spark)
   spark.stop()
   "
   ```

   Then `curl "http://localhost:8000/etablissements/<siret>/annonces-legales"`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add Step 2 (BODACC entity resolution) instructions to the README"
```

---

## Self-review notes

- **Spec coverage:** the spec's `GET /etablissements/{siret}/annonces-legales` endpoint is implemented, backed by the join decided above (a deliberate, discussed refinement of the spec's bare architecture diagram, not a deviation from it).
- **Placeholder scan:** none found.
- **Type consistency:** `BODACC_LINKS_POSTGRES_TABLE` column names/order in the DDL match `build_joined_links_df`'s `SELECT` exactly, which match `AnnonceLegale`'s Pydantic fields exactly, which match `SAMPLE_ANNONCE`'s test fixture keys exactly.
