# Design: BODACC Entity Resolution (Step 2)

## Context

`french-business-registry` is the Data Engineering / Big Data project of a
three-project portfolio built to support a 2027 end-of-studies internship search in
Data / AI Engineering (France). Step 1 (specced in
`docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md` and built across 5
implementation plans) delivered a historized SIRENE establishment registry — bronze
to gold on Iceberg/Garage, orchestrated by Airflow, tested with dbt, served through
a Postgres-backed FastAPI. It deliberately avoided entity resolution: SIRENE has a
reliable identifier, so there was nothing to resolve.

Step 1's spec explicitly deferred this: "Entity resolution design (Splink, DuckDB vs
Spark backend) — specced when Step 2 starts." This document is that spec.

### The problem Step 2 solves

BODACC (Bulletin officiel des annonces civiles et commerciales) publishes legal
announcements — incorporations, bankruptcies, management changes — but it
references the **company** (SIREN, the legal entity), not an **establishment**
(SIRET). A procedure such as a liquidation applies to the legal person, not to one
specific location. Some announcements carry a clean, trustworthy SIREN field; others
don't, or carry one that's missing, malformed, or doesn't resolve against SIRENE.
Where it's missing, the only way to link an announcement to the right company is by
matching on the name and address text in the announcement — fuzzy, not exact.

This is the project's first genuine entity resolution problem, and the deliberate
reason Step 2 starts with BODACC rather than DECP (Step 3): BODACC is the "easier"
version of this problem, worth proving the approach on before DECP's genuinely messy
multi-buyer-format data.

## Decisions made

- **Linkage is at SIREN (company) granularity, matched against the établissement
  *siège*** (`etablissement_siege = true`) in the Step 1 gold table — not at SIRET.
  BODACC announcements are inherently company-level; forcing a SIRET-level link
  would invent a precision the source data doesn't have. The siège establishment is
  the natural "representative" SIRET for a company when only a SIREN is known.
- **Entity resolution tool: Splink**, not a hand-rolled deterministic+fuzzy
  cascade. Splink (UK Ministry of Justice, open source) implements the
  Fellegi-Sunter probabilistic record linkage model and is used in production by
  national statistical institutes — it gives a calibrated match probability, not an
  ad hoc string-distance score, and is a far stronger "why this tool" answer in
  interview than a bespoke Levenshtein cascade.
- **Matching is a two-stage cascade**: (1) deterministic — if the announcement
  carries a SIREN that parses as 9 digits and exists in the SIRENE gold table,
  that's the match, confidence `1.0`, method `exact_siren`; (2) probabilistic — for
  everything else, Splink matches on company name and address text against the
  siège establishments, method `splink_fuzzy`, confidence = Splink's match
  probability. Every linkage row records which method produced it — this
  transparency is itself part of what makes the result defensible.
- **Splink backend: DuckDB for development/testing, Spark for the real pipeline.**
  Same dev/prod duality already used for dbt (Plan 4): DuckDB gives fast,
  deterministic, no-network test runs against small fixture data; Spark runs the
  same matching rules against the real Iceberg-backed gold table. Splink supports
  both backends behind the same API, so the matching rules themselves don't change
  between the two.
- **Match quality is measured with a blind holdout, not asserted.** A meaningful
  fraction of BODACC announcements carry a trustworthy, already-structured SIREN.
  Evaluation takes those records, hides the SIREN, re-runs the fuzzy (Splink) path
  on name/address alone, and checks whether it recovers the correct SIREN — giving
  real, measured precision/recall for the fuzzy path instead of an unverified claim.
  This is the concrete expression of this project's "measure everything, report
  honestly" standard for the one component where it matters most.
- **BODACC ingestion bootstraps a recent window (last 12 months), then daily
  diffs** — not the full available history. The BODACC Opendatasoft API supports
  date filtering. A full-history bootstrap (potentially years, many millions of
  rows) would add volume and processing time without making the matching problem
  any more interesting to demonstrate; 12 months is enough real data to have
  meaningful match cases and a real history to build on.
- **Sequencing mirrors Step 1's incremental plan structure**: ingestion first
  (lowest risk — it reuses the bootstrap/diff pattern from Step 1's Plans 1 and 3
  almost directly), then entity resolution (the real new work), then orchestration
  and serving last. Each gets its own implementation plan, written and executed one
  at a time, not planned exhaustively upfront.

## Architecture

```
BODACC API (Opendatasoft, 12-month bootstrap window + daily diffs)
        │
        ▼
bronze/bodacc/... (Parquet — same medallion pattern as SIRENE)
        │
        ▼
silver.bodacc_annonces (Iceberg — cleaned/typed: siren_declared, denomination,
                         adresse, type_avis, date_parution, ...)
        │
        ▼
Entity resolution (two-stage cascade)
  1. exact_siren:   siren_declared is a valid 9-digit SIREN present in
                     gold.sirene_etablissements_historized (current siège row)
  2. splink_fuzzy:  Splink match on (denomination, adresse) against current
                     siège establishments, for everything stage 1 didn't resolve
        │
        ▼
gold.bodacc_sirene_links (Iceberg, historized like the SIRENE gold table):
  siren_bodacc, siret_siege, match_method, match_confidence,
  bodacc_announcement_id, valid_from, valid_to, is_current
        │
        ▼
sync to Postgres (same pattern as Step 1) ──► FastAPI:
  GET /etablissements/{siret}/annonces-legales
```

## Data flow

**Bootstrap (once)**: fetch BODACC announcements from the Opendatasoft API for the
last 12 months, land in bronze, clean into `silver.bodacc_annonces`.

**Entity resolution (initial run)**: run the two-stage cascade against the full
silver table, producing the initial `gold.bodacc_sirene_links` rows, each tagged
with its match method and confidence.

**Daily**: fetch new/updated BODACC announcements (diff) → bronze → silver → run
the matching cascade on the new records only → merge new linkage rows into gold
(same SCD2 pattern as the SIRENE gold table: a re-matched announcement that now
resolves differently gets a new version, not an overwrite).

## Evaluation methodology

1. From `silver.bodacc_annonces`, select announcements whose `siren_declared` is a
   valid 9-digit SIREN that exists in the gold SIRENE table — these have a trusted
   label.
2. Hold out this SIREN, re-run **only** the Splink fuzzy stage on
   (`denomination`, `adresse`) for these records, ignoring the declared SIREN
   entirely.
3. Compare Splink's top match against the held-out true SIREN: compute precision
   (of the matches Splink made, how many were correct) and recall (of the
   holdout set, how many Splink found a match for at all, correct or not) at the
   confidence threshold chosen for production matching.
4. Report both numbers plainly, including if they're unimpressive — this is the
   project's main "honest measurement" artifact for Step 2, and a disappointing
   number with a clear explanation is more credible than a suspiciously perfect one.

## Data quality / error handling

- Malformed `siren_declared` values (wrong length, non-numeric) are treated as
  absent, not as a parse error that blocks the row — they fall through to the
  fuzzy stage.
- Announcements whose fuzzy match score falls below the chosen confidence
  threshold get `match_method = unresolved`, `siret_siege = NULL` — an explicit,
  queryable "we don't know" rather than a forced low-confidence guess.
- The gold linkage table's SCD2 historization follows the exact same invariants as
  Plan 2's SIRENE gold table (one current row per `bodacc_announcement_id`, no
  duplicate `(bodacc_announcement_id, valid_from)` pairs) — dbt tests for these
  are written the same way as Plan 4's.

## Testing

- Unit tests for the deterministic stage (SIREN parsing/validation) and the
  cleaning logic, following Step 1's established pattern (pytest, no real
  infrastructure needed).
- Splink matching rules tested against small, hand-built fixture data on the
  DuckDB backend — deterministic, fast, no Spark session required for this part.
- The blind holdout evaluation (above) is itself a test, run against real BODACC
  data pulled during the bootstrap — not synthetic fixtures, since the whole point
  is measuring real-world matching quality.
- dbt tests on `gold.bodacc_sirene_links` mirroring Plan 4's SCD2 invariant tests.

## Deliverable for Step 2

A queryable link between BODACC legal announcements and the SIRENE establishment
registry, with an honestly measured match quality, surfaced through the existing
API as a new endpoint. This is the project's first real entity resolution
component — the foundation Step 3 (DECP) will extend against much messier data.

## Out of scope for this document

- Exact implementation plan breakdown (how many plans, task-level detail) — decided
  via `writing-plans` once this spec is approved, the way Step 1's 5 plans were.
- DECP ingestion and matching (Step 3).
- Any new API use cases beyond the "announcements for this establishment" endpoint
  (e.g. the earlier portfolio pitch's "procurement winners who later went bankrupt"
  cross-source query) — that needs DECP too and belongs to Step 3.
- Whether/how the Airflow DAG from Step 1 gets extended vs. getting a second DAG —
  decided when the orchestration plan is written, after ingestion and matching
  exist to orchestrate.
