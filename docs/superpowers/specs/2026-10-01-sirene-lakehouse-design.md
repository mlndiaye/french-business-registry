# Design: SIRENE Lakehouse Pipeline (Step 1)

## Context

`french-business-registry` is the Data Engineering / Big Data project of a three-project
portfolio built to support a 2027 end-of-studies internship search in Data / AI
Engineering (France). The repository must demonstrate real engineering judgment on
real, messy French public data — not a toy ELT exercise.

### The problem

French public business data is spread across sources that do not reconcile cleanly:

- **SIRENE** (INSEE): authoritative company/establishment registry, with a reliable
  identifier (SIREN/SIRET).
- **BODACC**: legal announcements (incorporation, bankruptcy, management changes),
  fed by many different publishers, identifiers not always present or reliable.
- **DECP**: public procurement data, notoriously messy — supplier SIRET fields are
  frequently missing, truncated, or wrong because thousands of public buyers each
  publish in their own format.

Answering a question like "which companies won public contracts and then went
bankrupt within 12 months" requires linking records across these sources even when a
shared, reliable identifier is absent — a genuine entity resolution problem, not a
simple SQL join.

### Scope decomposition

The project is built incrementally, one source at a time, each a separately
demonstrable milestone:

1. **Step 1 (this document)**: SIRENE alone. Build the lakehouse pipeline and the
   historized registry. No entity resolution yet — SIRENE has a reliable identifier.
2. **Step 2**: add BODACC. First real entity resolution case, on relatively clean data.
3. **Step 3**: add DECP. The hard case — dirty data, where fuzzy matching must prove
   its value and precision/recall must be measured and reported honestly.

This document specs **Step 1 only**. Steps 2 and 3 will get their own specs once
Step 1 is built and validated.

## Decisions made

- **Lakehouse architecture from Step 1**, not introduced later: object storage +
  open table format + distributed processing end-to-end, even though SIRENE alone
  would run fine on a simpler local stack. Rationale: the portfolio needs to show
  data lake / lakehouse patterns that are recognizable and currently in demand on the
  market, and the laptop (36 GB RAM) comfortably supports the full stack in Docker.
- **Object storage: MinIO** (S3-compatible, self-hosted via Docker). Many companies
  run on-prem/private-cloud stacks on MinIO rather than AWS S3 directly; the code
  written against the S3 API would run unchanged against real S3.
- **Table format: Apache Iceberg**, chosen over Delta Lake. Iceberg is a vendor-neutral
  open table format with broad adoption outside the Databricks ecosystem (AWS Glue/
  Athena, Snowflake, Trino), which is a stronger "why this" answer in interview than
  Delta Lake's closer association with Databricks. It also gives native time travel,
  directly useful for the historization story.
- **Processing: PySpark**, using the DataFrame/Spark SQL API rather than Python UDFs,
  so that execution happens in the JVM (Catalyst/Tungsten) with no JVM↔Python
  serialization overhead — performance equivalent to Scala for this workload shape.
- **Ingestion strategy**: bootstrap once from the full SIRENE stock file, then apply
  daily diffs from the Sirene API. This mirrors a real incremental production
  pipeline and gives a concrete, recurring case for the historization logic, rather
  than a coarser monthly snapshot-diff approach.
- **Historization: SCD Type 2, all fields** (not a curated subset of "key" fields).
  Any change on any column opens a new version. This is both more faithful to a real
  audit/KYC registry and simpler to implement generically (whole-row comparison)
  than a field-by-field tracking policy that would need per-field justification.
- **dbt stays in the stack**, via the `dbt-spark` adapter, for tests and documentation
  on top of the gold Iceberg table — reusing existing dbt expertise rather than
  replacing it.
- **Serving layer stays PostgreSQL** behind a FastAPI service: the lakehouse is built
  for batch analytical workloads, not low-latency point lookups, so the current state
  is synced out to a serving store — a standard lakehouse-plus-serving-store pattern.
- **Rejected**: Airbyte for ingestion (adds connector-platform infrastructure with no
  real payoff for ~2-3 ad hoc sources); BigQuery/cloud warehouse as the primary engine
  (already a known skill, contradicts the "runs on my laptop" constraint); Kafka/
  streaming (no source needs real-time delivery — SIRENE updates daily at most);
  Hadoop/Hive (not relevant on the current market).

## Architecture

```
SIRENE stock (bootstrap, once)   ─┐
SIRENE API diffs (daily)         ─┴─► MinIO bronze (raw Parquet)
                                            │
                                    Spark job (cleaning / normalization)
                                            ▼
                                    MinIO silver (Iceberg)
                                            │
                                    Spark SQL MERGE INTO (SCD2)
                                            ▼
                                    MinIO gold (historized Iceberg) ──► dbt (tests + docs)
                                            │
                                    periodic sync (current state only)
                                            ▼
                                    PostgreSQL (serving) ──► FastAPI (search + history)

Orchestration: Airflow (daily DAG: extract → bronze→silver → silver→gold MERGE → dbt test → sync to Postgres)
Local infra:   Docker Compose (MinIO, Spark standalone, Airflow, Postgres, FastAPI)
```

### Components

- **Extractor (Python)**: one-off bootstrap job for the full SIRENE stock file, plus a
  daily job calling the Sirene API for records updated since the last successful run.
  Writes raw Parquet into MinIO bronze, partitioned by ingestion date.
- **Bronze → silver (Spark job)**: type casting, address/text normalization, rejection
  of malformed rows into a separate "rejects" path (does not fail the whole run).
  Writes a cleaned Iceberg table.
- **Silver → gold (Spark SQL `MERGE INTO`)**: implements SCD2. For each incoming
  record: if any column differs from the current version, close the current version
  (`valid_to = run_date`) and insert a new one (`valid_from = run_date`,
  `valid_to = NULL`, `is_current = true`); if the establishment is new, insert its
  first version.
- **dbt (dbt-spark adapter)**: schema and data tests plus documentation on the gold
  table.
- **Sync job**: materializes the current-state subset of the gold table into
  PostgreSQL for low-latency serving.
- **FastAPI service**: search a company by SIREN/name, view its change history.
- **Airflow**: orchestrates the daily DAG; the bootstrap job runs once, manually
  triggered, outside the recurring DAG.

## Data flow

**Bootstrap (one-time)**: download the full SIRENE stock file → land in bronze → Spark
job reads it, casts types, and initializes the gold Iceberg table with
`valid_from = stock date`, `valid_to = NULL`, `is_current = true` for every row.

**Daily**: Airflow triggers the extractor, which calls the Sirene API for updates
since the last successful run → lands in bronze → Spark job compares incoming records
to the current gold snapshot → `MERGE INTO` closes changed versions and inserts new
ones as described above.

## Data quality / error handling

- Idempotency is native to the `MERGE INTO` upsert semantics: re-running a given day
  does not create duplicate versions.
- dbt tests on the gold table: SIREN not null, uniqueness of `(siren, valid_from)`,
  freshness test on the latest ingestion timestamp.
- Malformed bronze records are quarantined to a "rejects" path rather than failing
  the pipeline run.
- Airflow tasks that call the Sirene API are configured with retries to absorb
  transient network errors and rate limiting.

## Testing

- `pytest` unit tests on the extraction logic (API responses mocked) and on the
  transformation logic (local Spark test sessions).
- dbt tests as listed above.
- One end-to-end integration test on a small fixture dataset (a few hundred rows)
  that asserts a modified establishment produces two distinct historized versions.

## Deliverable for Step 1

A historized SIRENE registry, queryable through an API, built on a reproducible,
orchestrated lakehouse pipeline — without entity resolution, since Step 1 has a
single source with a reliable identifier. This is the foundation Steps 2 and 3 will
extend with BODACC/DECP ingestion and entity resolution.

## Out of scope for this document

- Entity resolution design (Splink, DuckDB vs Spark backend) — specced when Step 2
  starts.
- BODACC/DECP ingestion specifics.
- CI/CD pipeline details beyond "GitHub Actions runs lint and tests."
- Alerting (e.g., Slack/email on Airflow task failure) — noted as a possible
  future addition, not required for Step 1.
