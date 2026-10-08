# Design: DECP Public Procurement Linkage (Step 3)

## Context

`french-business-registry` is the Data Engineering / Big Data project of a
three-project portfolio built to support a 2027 end-of-studies internship search in
Data / AI Engineering (France). Step 1 delivered a historized SIRENE establishment
registry (specced in `docs/superpowers/specs/2026-10-01-sirene-lakehouse-design.md`).
Step 2 delivered BODACC entity resolution — a two-stage cascade (exact SIREN, then
Splink fuzzy matching) linking legal announcements to SIRENE establishments,
specced in `docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md`
and built across five plans (ingestion, matching, gold historization + evaluation,
orchestration, dbt tests, serving).

### The problem Step 3 solves

DECP (Données Essentielles de la Commande Publique) is the French government's
open data on public procurement: every public buyer must publish each contract's
essentials — who won it, for how much, what it covers. This is the project's
third and final real-world use case: linking public contract awards to the same
historized company registry BODACC already feeds, so a due-diligence query can
answer "has this company won public contracts, with whom, for how much?"

The original Step 1 spec deferred this explicitly: Step 2's spec called DECP "the
genuinely messy multi-buyer-format data," assuming it would need the same kind of
fuzzy entity resolution BODACC did. Before designing Step 3, that assumption was
checked against the real data rather than carried over unexamined — and it turned
out to be wrong, in an informative way (see "Decisions made").

### What was verified before designing this

The current DECP consolidated file (`decp.parquet`, downloaded and inspected
directly from data.gouv.fr before writing this spec) is **already substantially
cleaner than BODACC**, because a public pipeline
(`github.com/139bercy/decp-arr2022`) aggregates and types it upstream:

- 3,288,586 rows nationally, 2018-2026. Scoped to department 08 (buyer) + the last
  12 months + `donneesActuelles = true` (the source's own "current version of this
  market" flag): **1,288 rows, 1,076 distinct markets (`uid`)**.
- Of those 1,288 rows, **1,281 already carry `titulaire_typeIdentifiant = SIRET`**
  — a real, structured 14-digit SIRET straight from the source, not a SIREN
  buried in free text the way BODACC's `registre` field was.
- The remaining 7 rows (`TVA`, or no identifier type at all) were inspected
  individually: **all 7 have a null `titulaire_nom` and null address fields.**
  There is nothing to fuzzy-match on — not "a weak signal," literally no text.
- Checked separately: only **~50%** of titulaires on a department-08 buyer's
  contracts are themselves located in department 08 — the other half are
  dispersed nationally. A department-scoped SIRENE candidate pool (the approach
  Step 2 used for BODACC) would silently fail to find the correct establishment
  for about half of all titulaires, for a reason that has nothing to do with
  matching quality.

This reframes Step 3 from "BODACC's problem again, on different data" to a
different, genuinely simpler problem: most of the identity work is already done
upstream; what's left is validating it, linking it into the historized registry,
and being honest about the small residual that can't be resolved at all.

## Decisions made

- **No Splink, no fuzzy matching stage.** Built on the two findings above: the
  SIRET is already known for 99.5% of the scoped rows, and the remaining 0.5% has
  no name or address to compare against anything. A fuzzy stage here would either
  do nothing (no candidates match real cases) or manufacture false positives from
  blocking on empty strings. This is a deliberate simplification relative to
  BODACC, justified by measurement, not by avoiding work — see the "genuinely
  messy" assumption this spec starts by overturning.
- **Two-branch resolution, not a two-stage cascade:**
  1. `titulaire_typeIdentifiant = SIRET` → trust the source's `titulaire_id`
     directly as `siret_titulaire`, method `source_siret`, confidence `1.0`.
  2. Anything else → `method = unresolved`, `siret_titulaire = NULL`.
  A separate, non-confidence-affecting enrichment step validates `source_siret`
  rows against `gold.sirene_etablissements_historized` (does this SIRET exist,
  is it active?) — this is a data-quality signal about the *establishment*, not
  a claim about how certain the identity resolution is. The identity itself came
  from the source's own typed field, not from matching.
- **Validation joins against Step 1's national SIRENE gold table, not a fresh
  department-scoped candidate fetch.** BODACC needed to *discover* an unknown
  SIRET from a SIREN or a name, so Step 2 fetched department-scoped candidates
  fresh (Step 1's gold table held only 2 fixture rows and no denomination column
  at the time). DECP already has a specific SIRET for the overwhelming majority of
  rows and titulaires are nationally dispersed (measured above) — a department
  fetch would be the wrong tool twice over. `gold.sirene_etablissements_historized`
  is designed to hold a full national bootstrap; this is the first place in the
  project that actually needs it at that scope, so Step 3 joins against it
  directly rather than re-fetching anything.
- **Same scope as BODACC: department 08 (buyer) + last 12 months.** Keeps the
  project's "universe of entities" consistent across BODACC, SIRENE and DECP —
  the same companies are reachable from multiple angles in a demo — and avoids
  inventing a new scoping axis when an existing, already-justified one fits.
  `donneesActuelles = true` is applied as a pre-filter (see "Data flow") to pick
  one row per market per run, mirroring how BODACC's cascade only ever sees one
  input row per announcement.
- **Ingestion re-downloads the full national file every run; no bootstrap/diff
  split.** Unlike BODACC's Opendatasoft API (which supports `since`/`until`
  filtering server-side), DECP is published as a single full-replacement file —
  there is no incremental endpoint to diff against. At ~236 MB nationally, a full
  download is cheap enough to just do every run, filtering down to the department
  + date scope locally. This removes an entire axis of complexity (bootstrap vs.
  diff ingestion modes) that BODACC needed and DECP doesn't.
- **`silver.decp_marches` legitimately uses `createOrReplace`, not the
  accumulate-via-MERGE fix Step 2 needed for BODACC.** That fix existed because
  BODACC's silver table was fed by true incremental diffs (each run only had
  *new* announcements, so silver had to accumulate across runs to hold history).
  DECP's ingestion re-filters the *entire* current national file to the scope
  every run, so each run's output already *is* the complete current scoped
  universe — overwriting it is correct, not a bug waiting to happen. A market
  that rolls out of the trailing 12-month window simply stops appearing in
  `incoming` on later runs; per the existing `apply_*_scd2_merge` pattern, a row
  absent from `incoming` is left untouched in gold (not closed or deleted) — its
  last-known state remains the current version, which is the right behavior.
- **The data-quality report replaces the blind-holdout evaluation.** BODACC's
  evaluation measured Splink's precision/recall because there was a real fuzzy
  prediction to score. There isn't one here. The honest measurement artifact for
  Step 3 is a coverage report: what fraction of scoped markets resolve to a
  SIRET, what fraction of *those* validate against SIRENE gold, and an explicit,
  inspected accounting of the unresolved rows (not just a count — the actual
  reason, as established above: no name/address to go on).

## Architecture

```
data.gouv.fr decp.parquet (national, full-replacement file, ~3.3M rows, 2018-2026)
        │
        ▼  download full file, filter locally:
           acheteur_departement_code = '08', datePublicationDonnees >= now - 12mo,
           donneesActuelles = true
bronze/decp/... (Parquet — same medallion pattern as SIRENE/BODACC)
        │
        ▼
silver.decp_marches (Iceberg — cleaned/typed: uid, acheteur_id, acheteur_nom,
                      titulaire_id, titulaire_typeIdentifiant, titulaire_nom,
                      montant, objet, codeCPV, dateNotification, ...)
        │
        ▼
Resolution (two branches, no fuzzy stage — see "Decisions made")
  1. source_siret:  titulaire_typeIdentifiant = 'SIRET' → siret_titulaire =
                     titulaire_id, confidence 1.0
  2. unresolved:     everything else → siret_titulaire = NULL
        │
        ▼  enrichment (not a resolution branch): join siret_titulaire against
           gold.sirene_etablissements_historized (current rows) to validate
           existence/status — adds siret_validated_in_sirene, does not change
           match_method or confidence
        │
        ▼
gold.decp_marches_links (Iceberg, historized like the BODACC links table):
  uid, siret_titulaire, match_method, siret_validated_in_sirene,
  acheteur_id, acheteur_nom, montant, objet, codeCPV, dateNotification,
  valid_from, valid_to, is_current
        │
        ▼
sync to Postgres (same pattern as Steps 1-2) ──► FastAPI:
  GET /etablissements/{siret}/marches-publics
```

## Data flow

**Every run** (no separate bootstrap — see "Decisions made"):

1. Download the current national `decp.parquet`.
2. Filter locally to `acheteur_departement_code = '08'`,
   `datePublicationDonnees` within the trailing 12 months, and
   `donneesActuelles = true`.
3. Land the filtered rows in bronze, clean into `silver.decp_marches`
   (`createOrReplace`).
4. Resolve each row to `source_siret` or `unresolved` (no cascade stage needed —
   it's a single column check, not a join or a model).
5. Validate `source_siret` rows' SIRETs against
   `gold.sirene_etablissements_historized` (enrichment only).
6. Historize into `gold.decp_marches_links` via the same two-statement
   MERGE+INSERT SCD2 pattern as `gold_links.py`, keyed on `uid`.

## Data quality measurement

Replaces BODACC's blind-holdout precision/recall (see "Decisions made" for why):

1. From `gold.decp_marches_links`, compute: total markets in scope, count and
   share resolved (`source_siret`) vs. `unresolved`.
2. Of the resolved rows, compute the share where `siret_validated_in_sirene` is
   true — this measures the upstream data's own quality (does its typed SIRET
   correspond to a real, known establishment?), not this project's matching
   quality, and that distinction is reported explicitly rather than blurred.
3. For `unresolved` rows, report the actual composition (identifier type
   breakdown, null-field breakdown) rather than just a count — this is what
   turned up the "no name or address at all" finding above, and the same check
   should be re-run against real department-08 data once ingested, not assumed
   to still hold.
4. Report all of this plainly, including if the resolved/validated shares are
   lower than expected — same standard as BODACC's evaluation.

## Data quality / error handling

- A `titulaire_typeIdentifiant` value is treated case-sensitively as published
  (`SIRET` vs. the rarer `Siret` seen in the national data) — normalized to
  uppercase before the branch check, so a capitalization inconsistency in the
  source doesn't silently misclassify a resolvable row as unresolved.
- A market whose `uid` repeats within the same ingestion run (observed in the
  real data — some markets have more than one `donneesActuelles = true` row,
  likely multi-lot or multi-titulaire awards) is not deduplicated away: each row
  is a distinct link (potentially a different titulaire for the same market),
  and `gold.decp_marches_links`'s invariant is "one current row per `uid` +
  `siret_titulaire` pair," not "one row per `uid`" — this needs confirming
  against real data in the matching plan, not assumed from the one sample
  inspected here.
- `gold.sirene_etablissements_historized` may itself be incomplete at the time
  Step 3 is actually run (its own real bootstrap is still a deferred manual
  verification task from Step 1) — a `source_siret` row whose SIRET isn't found
  there is `siret_validated_in_sirene = false`, not treated as an error or
  downgraded to `unresolved`; the identity still came from the source's own
  typed field.

## Testing

- Unit tests for the resolution branch logic (`source_siret` vs. `unresolved`,
  including the `Siret`/`SIRET` case-normalization) and the cleaning logic,
  following the established pattern (pytest, no real infrastructure needed).
- The SIRENE-gold validation join and the gold SCD2 historization are pure
  DataFrame logic — both unit-tested the same way `gold_links.py` was for
  BODACC, since (unlike Splink) there's no third-party inference engine involved
  anywhere in this step.
- The data-quality report (above) is itself a kind of test, run against real
  ingested DECP data once available — not synthetic fixtures, same reasoning as
  BODACC's blind holdout.
- dbt tests on `gold.decp_marches_links` mirroring the BODACC links table's SCD2
  invariant tests.

## Out of scope

- Breaking down this spec into individual implementation plans (ingestion,
  resolution + gold historization, orchestration, dbt tests, serving) — each
  gets its own plan, written and executed one at a time, mirroring Step 2's
  structure.
- Any entity resolution involving `acheteur_id` (the buyer) — buyers are public
  bodies whose SIRET is always reliable; there is no resolution problem on that
  side, only on `titulaire_id` (the contract winner).
- Re-fetching or re-scoping SIRENE candidates — this step deliberately reuses
  Step 1's national gold table as-is, not a fresh fetch (see "Decisions made").
- Modeling contract *modifications* as their own entity (the `modification_id`
  sequence) — only the `donneesActuelles = true` row per market feeds this
  pipeline; the full modification history stays queryable in bronze/the raw
  source if ever needed, but isn't part of the historized registry.
