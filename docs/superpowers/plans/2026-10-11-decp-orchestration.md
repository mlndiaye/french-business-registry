# DECP Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A daily Airflow DAG that ingests DECP, cleans it to silver, resolves/validates titulaires, and historizes into `gold.decp_marches_links` — the DECP equivalent of `bodacc_matching_pipeline`, but structurally simpler because there's no expensive matching stage to avoid re-running.

**Architecture:** Three TaskFlow tasks mirroring `bodacc_matching_pipeline.py`'s shape: extract, transform to silver, resolve+historize. Unlike BODACC's DAG, there is no backlog/"unlinked" computation step — DECP's resolution is a cheap case-check plus a join (Plan 2), not a trained Splink model, so every run simply re-resolves the *entire* current scoped silver table rather than only new records. This mirrors `decp_to_silver`'s own `createOrReplace` reasoning (Plan 2): each run's scope is already the complete current universe, not an incremental diff.

**Tech Stack:** Airflow TaskFlow API, PySpark — no Splink, no new dependencies, no new environment variables beyond `DECP_PARQUET_URL` (already added to `.env`/`.env.example` in Plan 1; this plan wires it into the Airflow containers).

This is Plan 4 of Step 3 (DECP public procurement linkage). See
`docs/superpowers/specs/2026-10-08-decp-procurement-design.md` for the full
design and Plans 1-3 (`2026-10-09-decp-ingestion.md`,
`2026-10-09-decp-entity-resolution.md`,
`2026-10-10-decp-gold-links-data-quality.md`) for the functions this wires
together. dbt tests and serving (Postgres + FastAPI) are each their own plan,
deferred here — mirroring Step 2's split.

## Decisions made

- **No backlog/"find unlinked markets" step, unlike BODACC's DAG.**
  `bodacc_matching_pipeline` computes a backlog specifically to avoid re-running
  Splink training on already-resolved announcements (genuinely expensive).
  DECP's resolution (Plan 2) is a `CASE WHEN` plus a join against SIRENE gold —
  cheap enough to simply re-run over the entire current scope every day. Since
  `silver.decp_marches` and `silver.decp_marches_links` are both rebuilt via
  `createOrReplace` from the full current scope each run anyway (Plans 1-2's own
  reasoning), introducing a backlog concept here would add complexity that
  solves a problem DECP doesn't have.
- **No new Spark session setup beyond `build_lakehouse_session()`.** BODACC's
  DAG needed a checkpoint directory and Splink's similarity jar because it runs
  a real Splink `Linker`. DECP's resolution task does neither — it's plain
  DataFrame operations, so the shared session factory is used as-is.
- **No new SIRENE-candidates-style snapshot pointer.** BODACC's DAG reads
  SIRENE candidates from a fixed bronze snapshot because it needed a
  denomination-bearing, department-scoped candidate pool Step 1's gold table
  didn't have. DECP's validation step (Plan 2) joins directly against
  `gold.sirene_etablissements_historized` — the table Step 1 already produces —
  so there's nothing new to snapshot or point at.
- **Manual verification is attempted for real this time, including the
  Airflow stack itself** — not just the underlying Python functions. Every
  piece of this DAG has already been verified against real data without
  Airflow (Plans 1-3's own manual verifications); this plan's distinct
  contribution is proving the *DAG wiring itself* works, which needs Airflow
  actually running. Nothing blocks this on credentials, so Task 3 attempts it
  rather than deferring — this is the heaviest manual verification in the
  project so far (the full `postgres` + `airflow-init` + `airflow-webserver` +
  `airflow-scheduler` stack, not just Garage), and if first-time Airflow
  initialization turns out to be slow or flaky in this environment, that will
  be reported honestly rather than assumed away.

## Architecture

```
data.gouv.fr decp.parquet (full national file, re-downloaded every run)
        │
        ▼  extract_decp (run_ingestion: download + filter to dept 08 / 12mo / current)
bronze/decp/ingestion_date=.../marches.parquet
        │
        ▼  transform_bronze_to_silver (createOrReplace — full current scope, not a diff)
silver.decp_marches
        │
        ▼  resolve_and_historize:
           resolve_decp_titulaires -> validate_against_sirene (vs. gold.sirene_etablissements_historized)
           -> historize_decp_links (SCD2 merge, keyed on uid)
gold.decp_marches_links
```

---

### Task 1: Daily orchestration DAG

**Files:**
- Create: `dags/decp_matching_pipeline.py`

No pytest — thin wiring, same convention as `bodacc_matching_pipeline.py` and
`sirene_daily_pipeline.py`.

- [ ] **Step 1: Write the DAG**

Create `dags/decp_matching_pipeline.py`:

```python
"""Daily DAG: DECP national file -> filtered bronze -> silver -> resolve/validate
-> gold (SCD2 merge). No backlog step and no Splink — see Plan 4's "Decisions
made" for why this DAG is structurally simpler than bodacc_matching_pipeline."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="decp_matching_pipeline",
    schedule="@daily",
    start_date=dt.datetime(2026, 10, 1),
    catchup=False,
)
def decp_matching_pipeline():
    @task
    def extract_decp(logical_date=None) -> str:
        from registry.ingestion.decp import run_ingestion

        since = logical_date.date() - dt.timedelta(days=365)
        return run_ingestion(
            url=os.environ["DECP_PARQUET_URL"],
            bucket=os.environ["LAKEHOUSE_BUCKET"],
            department="08",
            since=since,
            work_dir=Path("/tmp"),
        )

    @task
    def transform_bronze_to_silver(bronze_key: str) -> str:
        from registry.transform.decp_to_silver import bronze_to_silver
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        silver_table = "lakehouse.silver.decp_marches"
        spark = build_lakehouse_session()
        try:
            bronze_to_silver(spark, f"s3a://{bucket}/{bronze_key}", silver_table)
        finally:
            spark.stop()
        return silver_table

    @task
    def resolve_and_historize(silver_table: str, logical_date=None) -> None:
        from registry.matching.decp_gold_links import historize_decp_links
        from registry.matching.decp_resolution import (
            resolve_decp_titulaires,
            validate_against_sirene,
        )
        from registry.transform.spark_session import build_lakehouse_session

        gold_table = "lakehouse.gold.decp_marches_links"
        silver_links_table = "lakehouse.silver.decp_marches_links"

        spark = build_lakehouse_session()
        try:
            decp_df = spark.table(silver_table)
            resolved_df = resolve_decp_titulaires(decp_df)

            sirene_gold_df = spark.table("lakehouse.gold.sirene_etablissements_historized")
            validated_df = validate_against_sirene(resolved_df, sirene_gold_df)

            historize_decp_links(
                spark, validated_df, silver_links_table, gold_table, logical_date.date()
            )
        finally:
            spark.stop()

    bronze_key = extract_decp()
    silver_table = transform_bronze_to_silver(bronze_key)
    resolve_and_historize(silver_table)


decp_matching_pipeline()
```

- [ ] **Step 2: Verify the DAG file is syntactically valid**

Run: `uv run python -c "import ast; ast.parse(open('dags/decp_matching_pipeline.py').read())"`
Expected: no output, exit code 0 (full Airflow DAG-parsing verification happens
in Task 3, once the Airflow stack is actually running).

- [ ] **Step 3: Commit**

```bash
git add dags/decp_matching_pipeline.py
git commit -m "feat: add daily DECP orchestration DAG"
```

---

### Task 2: Wire `DECP_PARQUET_URL` into the Airflow services

**Files:**
- Modify: `docker-compose.yml`

`DECP_PARQUET_URL` itself was already added to `.env`/`.env.example` in Plan 1 —
this task only wires it into the two Airflow containers, mirroring how
`SIRENE_CANDIDATES_BRONZE_KEY` was wired in for the BODACC DAG.

- [ ] **Step 1: Add the env var to both Airflow services**

In the `airflow-webserver` service's `environment:` block, add after
`SIRENE_CANDIDATES_BRONZE_KEY: ${SIRENE_CANDIDATES_BRONZE_KEY}`:

```yaml
      DECP_PARQUET_URL: ${DECP_PARQUET_URL}
```

Repeat the identical line in the `airflow-scheduler` service's `environment:`
block.

- [ ] **Step 2: Verify the compose file is still valid**

Run: `docker compose config --quiet`
Expected: no output, exit code 0.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: wire DECP_PARQUET_URL into Airflow services"
```

---

### Task 3: Manual verification — the real Airflow stack

**Files:** none (manual verification only)

No deferral — nothing about DECP needs a credential. This is the heaviest
manual verification in the project so far (the full Airflow stack, not just
Garage), attempted for real rather than assumed.

- [ ] **Step 1: Start the full stack**

```bash
set -a && source .env && set +a
docker compose up -d garage postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Expected: `airflow-init` completes successfully (runs migrations, creates the
default user); the other services report healthy/running. If this is the
first time the Airflow images are built, this step may take several minutes —
that's expected, not a failure.

- [ ] **Step 2: Confirm the DAG is parsed and visible**

```bash
docker compose exec airflow-webserver airflow dags list | grep decp_matching_pipeline
```

Expected: the DAG appears in the list, with no import errors. If it's missing,
check `docker compose exec airflow-webserver airflow dags list-import-errors`
before going further.

- [ ] **Step 3: Trigger a real run**

```bash
docker compose exec airflow-webserver airflow dags trigger decp_matching_pipeline
```

Then poll until it finishes:

```bash
docker compose exec airflow-webserver airflow dags list-runs -d decp_matching_pipeline
```

Expected: eventually shows a run with state `success`. If any task fails,
inspect its logs with
`docker compose exec airflow-webserver airflow tasks logs decp_matching_pipeline <task_id> <run_id>`
before concluding the DAG itself is broken — the most likely failure mode is
an environment issue (e.g. a missing package in the Airflow image), not a
logic bug, since every function this DAG calls already has its own passing
real-data verification from Plans 1-3.

- [ ] **Step 4: Verify the real result**

```bash
uv run python -c "
from registry.transform.spark_session import build_lakehouse_session
spark = build_lakehouse_session()
print('gold row count:', spark.table('lakehouse.gold.decp_marches_links').count())
spark.table('lakehouse.gold.decp_marches_links').filter('is_current = true').show(5)
spark.stop()
"
```

Expected: a row count in the same ballpark as Plans 1-3's manual runs
(~1,293), now produced by the DAG itself rather than by ad hoc scripts.

- [ ] **Step 5: Confirm idempotency — trigger a second run**

```bash
docker compose exec airflow-webserver airflow dags trigger decp_matching_pipeline
```

Wait for it to finish, then re-check the gold row count. Expected: the row
count is unchanged (no duplicate versions) unless the trailing 12-month window
genuinely picked up new/changed real markets since the first run — either
outcome is correct; report whichever actually happens.

---

## Self-review notes

- **Spec coverage:** the spec's full data flow (ingest → silver → resolve →
  validate → historize) is now runnable end-to-end via a single daily DAG,
  completing Step 3's orchestration layer the way Plan 4 of Step 2 did for
  BODACC.
- **Placeholder scan:** none found.
- **Type consistency:** `resolve_and_historize`'s calls to
  `resolve_decp_titulaires`/`validate_against_sirene`/`historize_decp_links`
  match those functions' exact signatures from Plans 2-3. Table names
  (`lakehouse.silver.decp_marches`, `lakehouse.silver.decp_marches_links`,
  `lakehouse.gold.decp_marches_links`) match what Plans 1-3's own manual
  verifications already used.
