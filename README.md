# french-business-registry
Multi-source ELT platform unifying French public business data (SIRENE, BODACC, public procurement) into a historized registry, with data quality testing and large-scale entity resolution.

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
