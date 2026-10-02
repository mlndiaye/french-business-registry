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
