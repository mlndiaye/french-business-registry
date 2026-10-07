# Design: BODACC Matching Orchestration (Step 2)

## Context

Step 2's entity resolution building blocks are now implemented and tested, but
nothing runs them automatically: `exact_siren_matches`, `run_matching_cascade`'s
Splink fuzzy stage, `combine_match_results`, `resolve_unresolved_matches`, and
`historize_match_results` (gold SCD2) all exist as pure, tested functions, but are
only ever called manually from a scratch script or a test. Step 1 solved the
equivalent problem for SIRENE with a daily Airflow DAG
(`dags/sirene_daily_pipeline.py`). This is that same orchestration layer for BODACC
matching.

This is one of three remaining Step 2 sub-projects identified after the gold
historization + evaluation plan shipped (the other two — dbt tests on
`gold.bodacc_sirene_links`, and a Postgres sync + FastAPI endpoint for BODACC
links — are each their own future spec/plan, deliberately out of scope here).

## Decisions made

- **The DAG only re-ingests BODACC, not SIRENE candidates.** Establishments change
  far less often than legal announcements, and re-fetching department-08 candidates
  from the Sirene API daily would add cost and complexity for no real benefit at
  this project's scale. The DAG reads SIRENE candidates from the fixed bronze
  snapshot produced once by the (still-deferred) manual bootstrap run, via a new
  required env var `SIRENE_CANDIDATES_BRONZE_KEY` pointing at that one Parquet
  object. Refreshing candidates is a separate, manual, out-of-scope concern.
- **Matching runs against the full backlog, not just the day's diff.** A new
  `find_unlinked_announcements` function anti-joins `silver.bodacc_annonces`
  against `gold.bodacc_sirene_links` (current rows only) to find every announcement
  with no current link yet — not only the rows from today's diff. This makes the
  pipeline self-healing: a DAG failure, a backfill, or any gap doesn't permanently
  skip announcements the way diff-only tracking would.
- **One Spark task does matching + historization**, not two. Exact matching,
  Splink's linker, `combine_match_results`, `resolve_unresolved_matches`, and
  `historize_match_results` all operate on in-memory DataFrames within the same
  Spark session — splitting them across Airflow tasks would force an unnecessary
  intermediate write/read round-trip (Airflow's XCom can't carry a Spark
  DataFrame). `extract` and `transform_to_silver` stay separate tasks, mirroring
  `sirene_daily_pipeline.py`, because each genuinely needs a different Iceberg
  table as its handoff.
- **`run_matching_cascade` is new, untested (pytest) code**, wiring
  `exact_siren_matches` → `unmatched_announcements` → Splink
  (`build_linker`/`train_linker`/`predict_fuzzy_matches`) →
  `extract_fuzzy_match_candidates` → `best_fuzzy_match_per_announcement` →
  `combine_match_results` → `resolve_unresolved_matches`. Every function it calls is
  already unit-tested individually; this function's only job is to sequence them
  with a real Splink `SparkAPI`, the same reasoning `splink_matching.py` itself was
  built on — verified manually, not mocked.
- **The DAG itself is thin wiring**, same convention as `sirene_daily_pipeline.py`:
  no pytest, correctness comes from the unit-tested functions it calls plus manual
  verification once `SIRENE_API_KEY` and `SIRENE_CANDIDATES_BRONZE_KEY` are set.

## Architecture

```
BODACC API (daily diff)
        │
        ▼
bronze/bodacc/... (Parquet)
        │
        ▼
silver.bodacc_annonces (Iceberg)
        │
        ▼  find_unlinked_announcements (anti-join vs gold, is_current=true)
   backlog of unlinked announcements
        │
        ▼  run_matching_cascade(backlog, sirene_candidates, SparkAPI)
   combined matches (exact_siren / splink_fuzzy / unresolved)
        │
        ▼  historize_match_results (SCD2 merge)
gold.bodacc_sirene_links
```

SIRENE candidates come from a fixed bronze Parquet snapshot
(`SIRENE_CANDIDATES_BRONZE_KEY`), read directly — no silver transform needed for
this side, since candidates aren't historized, only the match results are.

## Data flow

**Daily** (`@daily`, `catchup=False`, same as `sirene_daily_pipeline`):

1. `extract_bodacc_daily_diff` — `run_ingestion(..., run_type="diff")`, returns the
   bronze key for the day's new/updated announcements.
2. `transform_bronze_to_silver` — `bodacc_to_silver.bronze_to_silver`, merges into
   `silver.bodacc_annonces`, returns the silver table name.
3. `run_matching_and_historize`:
   - `ensure_gold_links_table` (idempotent — handles the very first run, when gold
     doesn't exist yet, by creating it empty before the anti-join).
   - Read the full `silver.bodacc_annonces` table (not just today's diff — see
     "Decisions made") and the full `gold.bodacc_sirene_links` table.
   - `find_unlinked_announcements(bodacc_df, gold_links_df)` → backlog.
   - Read SIRENE candidates from `SIRENE_CANDIDATES_BRONZE_KEY`.
   - `run_matching_cascade(backlog, candidates, SparkAPI(spark_session=spark))` →
     combined matches (including `unresolved` rows for the backlog).
   - `historize_match_results(spark, matches, silver_links_table, gold_links_table,
     run_date)`.

## Data quality / error handling

- An announcement already linked (`is_current=true` in gold) is never re-matched
  unless it's removed from gold first — this plan doesn't add a "force rematch"
  path, since nothing in the spec calls for one and it would add untested surface
  area for a case that doesn't occur in practice yet.
- If `SIRENE_CANDIDATES_BRONZE_KEY` points at a missing object, the task fails
  loudly (Spark's `read.parquet` raises) rather than silently matching against
  nothing — consistent with the project's "fail closed" pattern elsewhere.

## Testing

- `find_unlinked_announcements`: pytest, small Spark fixtures, following the exact
  same style as `exact_siren.py`'s `unmatched_announcements`.
- `run_matching_cascade` and the DAG: no pytest, manual verification once
  `SIRENE_API_KEY` and `SIRENE_CANDIDATES_BRONZE_KEY` are available — same
  deferred-task pattern as every prior Splink/Airflow piece in this project.

## Out of scope

- dbt tests for `gold.bodacc_sirene_links` (separate spec/plan).
- Postgres sync + FastAPI endpoint for BODACC links (separate spec/plan).
- Refreshing SIRENE candidates automatically.
- A "force rematch" / re-resolution path for already-linked announcements.
