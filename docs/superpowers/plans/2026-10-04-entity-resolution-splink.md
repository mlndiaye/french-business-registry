# Entity Resolution (Splink) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Link BODACC announcements (Plan 1 of Step 2) to SIRENE siège
establishments by SIREN — deterministically where the announcement carries a
trustworthy SIREN, and via Splink's probabilistic record linkage (name + address)
where it doesn't.

**Architecture:** A deterministic stage joins on `siren_declared` directly. A
Splink stage (DuckDB backend for dev/test, Spark backend for the real pipeline)
fuzzy-matches whatever the deterministic stage couldn't resolve, on cleaned
denomination and address text. Both stages produce the same shape
(`bodacc_announcement_id`, `siret_siege`, `match_method`, `match_confidence`),
unioned into one result set. A new, small SIRENE "matching candidates" dataset
(siège establishments for one department, fetched fresh via the Sirene API,
*including* the legal denomination) feeds both stages — Step 1's gold SIRENE table
turns out not to carry company names at all (see "Decisions made").

**Tech Stack:** `splink==4.0.17` (version-pinned; its API was verified by hand in
this environment — see "Decisions made" — rather than assumed from memory),
reusing `requests`/`pyarrow`/`boto3` for the new ingestion piece and PySpark for
everything else, consistent with the rest of the project.

This is Plan 2 of Step 2 (BODACC entity resolution) of the
`french-business-registry` project. See
`docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md` for the
full design and Plan 1
(`docs/superpowers/plans/2026-10-03-bodacc-ingestion.md`) for the BODACC silver
table this matches against. Historizing results into a `gold.bodacc_sirene_links`
SCD2 table and the blind-holdout quality evaluation are **out of scope** — a
follow-up plan, once matching itself is proven to work (same incremental
sequencing Step 1 used for transform/orchestrate/test/serve).

## Decisions made

- **Discovered before writing any code: Step 1's gold SIRENE table has no company
  name at all.** It was built from INSEE's *StockEtablissement* file
  (establishment-level) only; the legal denomination lives in a separate
  *StockUniteLegale* file Step 1 never touched. Matching on "name + address" is
  impossible against a table with no name column. Asked the user how to close this
  gap; chose to enrich via the Sirene API already integrated in Step 1 (Plan 3),
  scoped to the one department this project uses (see Plan 1 of Step 2) rather
  than a second bulk national file.
- **Also discovered: Step 1's gold table currently holds only the 2 Paris fixture
  establishments used for Plan 1/2's manual verification** — no real data for
  department 08 (or anywhere else) was ever bootstrapped, since Step 1's `Task 10`s
  needing the real SIRENE stock file / API key were deferred. Enriching
  establishments that don't exist in gold yet doesn't make sense, so this plan
  fetches department-08 siège establishments **fresh, directly from the Sirene API**
  (Tasks 2-4) rather than reading Step 1's gold table at all. This is a parallel,
  purpose-built "matching candidates" dataset — not a retrofit of Step 1's shipped
  gold schema or SCD2 merge logic, which stay exactly as Step 1 left them.
- **The Sirene API's `/siret` search endpoint (already used for diffs in Step 1,
  Plan 3) returns each establishment's legal-unit data nested under
  `uniteLegale`**, including `denominationUniteLegale` for companies and
  `nomUniteLegale`/`prenomUsuelUniteLegale` for individual entrepreneurs — this
  plan's `normalize_candidate` reads both, falling back to nom/prenom when there's
  no company denomination. The query adds `etablissementSiege:true`,
  `etatAdministratifEtablissement:A` (currently active), and a
  `codePostalEtablissement:{department}*` prefix filter — reusing exactly the
  cursor-pagination mechanism Plan 3 already proved against the live API, just
  with a different `q` filter.
- **Splink's API was verified empirically in this environment before this plan was
  written**, not reconstructed from memory the way the Sirene/BODACC API shapes
  were (and then found subtly wrong during manual verification, each time). A
  throwaway venv confirmed: `splink==4.0.17` resolves cleanly alongside
  `pyspark==3.5.3`; `Linker`, `SettingsCreator`, `block_on` are top-level exports;
  `DuckDBAPI` lives at `splink.backends.duckdb`, `SparkAPI` at
  `splink.backends.spark`; a `Linker` instance exposes `.training` (with
  `estimate_probability_two_random_records_match`, `estimate_u_using_random_sampling`,
  `estimate_parameters_using_expectation_maximisation`) and `.inference.predict`;
  and a full tiny train+predict pipeline ran end to end without error. Task 1
  re-runs this same smoke test as this plan's first step, so executing the plan
  doesn't have to trust a research session that already happened.
- **No automated pytest coverage for the Splink training/prediction wrapper**
  (Task 7) — same reasoning as Step 1 Plan 3's Airflow DAG: it's thin orchestration
  around an already-tested external library, and EM parameter estimation is
  statistically meaningless on the tiny, deterministic fixtures pytest needs to
  stay fast. What Splink *returns* is still fully testable, though: Task 8's
  functions (extracting/ranking/combining match candidates) take a
  plausibly-shaped DataFrame as input and are tested the normal way, without
  needing Splink itself in the test.
- **Splink backend duality (DuckDB dev / Spark prod) is supported by making
  `build_linker` accept any `db_api`**, not by writing two separate linker-building
  functions — the matching *rules* (comparisons, blocking) don't change between
  backends, only which engine executes them.
- **Task 10 (the only task touching the real Sirene API) is deferred**, for the
  same reason Step 1 Plan 3's Task 10 was: this environment has no
  `SIRENE_API_KEY` yet. Tasks 1-9 are fully executable and verifiable now without
  one.
- **Gold historization and the blind-holdout quality evaluation are out of scope**
  for this plan (see spec's "Out of scope"). This plan's deliverable is: matching
  produces correct, unified `(bodacc_announcement_id, siret_siege, match_method,
  match_confidence)` rows in memory/tested form — not yet a persisted, historized
  table, and not yet a measured precision/recall number.

## Architecture

```
Sirene API (codePostalEtablissement:08* AND etablissementSiege:true AND etatAdministratifEtablissement:A)
        │  fetch_sirene_candidates (cursor pagination, reuses Plan 3's pattern)
        ▼
bronze/sirene_candidates/department=08/ingestion_date=.../candidates.parquet
        │
        ├──────────────────────────────────────────────┐
        ▼                                               ▼
lakehouse.silver.bodacc_annonces (Plan 1)      sirene candidates (Spark DataFrame)
        │                                               │
        └──────────────┬────────────────────────────────┘
                        ▼
              exact_siren_matches  ──────────────► (confidence=1.0, method=exact_siren)
                        │
              unmatched_announcements
                        ▼
      prepare_bodacc_for_matching / prepare_sirene_for_matching
                        ▼
         Splink Linker (DuckDB dev / Spark prod, same rules)
                        ▼
         train_linker (EM) → predict_fuzzy_matches ──► (confidence=match_probability,
                                                          method=splink_fuzzy)
                        │
              best_fuzzy_match_per_announcement
                        ▼
                combine_match_results  (exact ∪ best fuzzy, per announcement)
```

---

### Task 1: Splink dependency + end-to-end smoke test

**Files:**
- Modify: `pyproject.toml`
- Create: `scripts/splink_smoke_test.py`

- [ ] **Step 1: Add `splink` to dependencies**

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
    "splink>=4.0,<5.0",
]
```

Run: `uv sync --all-groups`
Expected: resolves and installs `splink==4.0.17` (or a compatible 4.x patch
release) alongside the existing dependencies, exits 0.

- [ ] **Step 2: Create the smoke test script**

```python
"""One-off smoke test proving the Splink API this plan relies on actually works in
this environment, before any of the real matching modules are written on top of
it. Not part of the pytest suite — see Task 7's "Decisions made" note on why
Splink's training isn't unit-tested the normal way."""

from __future__ import annotations

import pandas as pd
import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.backends.duckdb import DuckDBAPI

bodacc_df = pd.DataFrame(
    [
        {
            "uid": "b1",
            "denomination_clean": "dupont batiment",
            "adresse_clean": "8 rue de la paix 75002",
        },
        {
            "uid": "b2",
            "denomination_clean": "martin travaux",
            "adresse_clean": "12 rue du test 69001",
        },
    ]
)
sirene_df = pd.DataFrame(
    [
        {
            "uid": "s1",
            "denomination_clean": "dupont batiment sarl",
            "adresse_clean": "8 rue de la paix 75002",
        },
        {
            "uid": "s2",
            "denomination_clean": "autre entreprise",
            "adresse_clean": "1 rue autre 75003",
        },
    ]
)

blocking_rule = block_on("substr(denomination_clean, 1, 3)")

settings = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="uid",
    comparisons=[
        cl.JaroWinklerAtThresholds("denomination_clean"),
        cl.LevenshteinAtThresholds("adresse_clean"),
    ],
    blocking_rules_to_generate_predictions=[blocking_rule],
    retain_intermediate_calculation_columns=False,
)

linker = Linker([bodacc_df, sirene_df], settings, db_api=DuckDBAPI())
linker.training.estimate_probability_two_random_records_match([blocking_rule], recall=0.7)
linker.training.estimate_u_using_random_sampling(max_pairs=1e4)
linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)
results = linker.inference.predict(threshold_match_probability=0.0)
print(results.as_pandas_dataframe()[["uid_l", "uid_r", "match_probability"]])
print("Smoke test completed without error.")
```

- [ ] **Step 3: Run it**

```bash
uv run python scripts/splink_smoke_test.py
```

Expected: prints a (possibly empty or low-confidence, given only 4 toy records —
EM needs real volume to converge meaningfully) comparison table, then
`Smoke test completed without error.`, exit code 0. If any of the API calls in
this script don't exist or have a different signature in the installed Splink
version, **stop here** — the rest of this plan builds directly on this exact API
shape, and every subsequent task would need the same fix.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock scripts/splink_smoke_test.py
git commit -m "feat: verify Splink API with an end-to-end smoke test"
```

---

### Task 2: `normalize_candidate`

**Files:**
- Create: `src/registry/matching/__init__.py`
- Create: `src/registry/matching/sirene_candidates.py`
- Test: `tests/matching/__init__.py`
- Test: `tests/matching/test_sirene_candidates.py`

- [ ] **Step 1: Create the packages**

`src/registry/matching/__init__.py`:
```python
```

`tests/matching/__init__.py`:
```python
```

- [ ] **Step 2: Write the failing tests**

```python
from registry.matching.sirene_candidates import normalize_candidate


def test_normalize_candidate_uses_denomination_when_present():
    raw = {
        "siren": "123456789",
        "siret": "12345678900019",
        "adresseEtablissement": {
            "numeroVoieEtablissement": "5",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA REPUBLIQUE",
            "codePostalEtablissement": "08000",
            "libelleCommuneEtablissement": "CHARLEVILLE-MEZIERES",
        },
        "uniteLegale": {
            "denominationUniteLegale": "DUPONT BATIMENT",
            "nomUniteLegale": None,
            "prenomUsuelUniteLegale": None,
        },
    }

    result = normalize_candidate(raw)

    assert result["siret"] == "12345678900019"
    assert result["denomination"] == "DUPONT BATIMENT"
    assert result["code_postal"] == "08000"
    assert result["libelle_commune"] == "CHARLEVILLE-MEZIERES"


def test_normalize_candidate_falls_back_to_nom_prenom_for_individuals():
    raw = {
        "siren": "987654321",
        "siret": "98765432100019",
        "adresseEtablissement": {
            "numeroVoieEtablissement": "2",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DU TEST",
            "codePostalEtablissement": "08200",
            "libelleCommuneEtablissement": "SEDAN",
        },
        "uniteLegale": {
            "denominationUniteLegale": None,
            "nomUniteLegale": "MARTIN",
            "prenomUsuelUniteLegale": "Julien",
        },
    }

    result = normalize_candidate(raw)

    assert result["denomination"] == "MARTIN Julien"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_sirene_candidates.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.sirene_candidates'`

- [ ] **Step 4: Implement `normalize_candidate`**

```python
"""Fetch current siège establishments with their legal denomination, scoped to a
department, as SIRENE-side matching candidates for entity resolution against
BODACC (see docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file

SIRENE_API_URL = "https://api.insee.fr/api-sirene/3.11/siret"

CANDIDATE_COLUMNS = [
    "siren",
    "siret",
    "denomination",
    "numero_voie",
    "type_voie",
    "libelle_voie",
    "code_postal",
    "libelle_commune",
]


def normalize_candidate(raw: dict) -> dict:
    adresse = raw.get("adresseEtablissement") or {}
    unite_legale = raw.get("uniteLegale") or {}
    denomination = unite_legale.get("denominationUniteLegale")
    if not denomination:
        nom = unite_legale.get("nomUniteLegale")
        prenom = unite_legale.get("prenomUsuelUniteLegale")
        if nom:
            denomination = f"{nom} {prenom}".strip() if prenom else nom
    return {
        "siren": raw.get("siren"),
        "siret": raw.get("siret"),
        "denomination": denomination,
        "numero_voie": adresse.get("numeroVoieEtablissement"),
        "type_voie": adresse.get("typeVoieEtablissement"),
        "libelle_voie": adresse.get("libelleVoieEtablissement"),
        "code_postal": adresse.get("codePostalEtablissement"),
        "libelle_commune": adresse.get("libelleCommuneEtablissement"),
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_sirene_candidates.py -v`
Expected: `2 passed`

- [ ] **Step 6: Commit**

```bash
git add src/registry/matching/__init__.py src/registry/matching/sirene_candidates.py tests/matching/__init__.py tests/matching/test_sirene_candidates.py
git commit -m "feat: normalize Sirene API establishments into matching candidates"
```

---

### Task 3: `fetch_sirene_candidates`

**Files:**
- Modify: `src/registry/matching/sirene_candidates.py`
- Test: `tests/matching/test_sirene_candidates.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/matching/test_sirene_candidates.py`:

```python
import responses

from registry.matching.sirene_candidates import SIRENE_API_URL, fetch_sirene_candidates


def _raw_candidate(siret: str, denomination: str) -> dict:
    return {
        "siren": siret[:9],
        "siret": siret,
        "adresseEtablissement": {
            "numeroVoieEtablissement": "5",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA REPUBLIQUE",
            "codePostalEtablissement": "08000",
            "libelleCommuneEtablissement": "CHARLEVILLE-MEZIERES",
        },
        "uniteLegale": {
            "denominationUniteLegale": denomination,
            "nomUniteLegale": None,
            "prenomUsuelUniteLegale": None,
        },
    }


@responses.activate
def test_fetch_sirene_candidates_queries_department_and_siege_filter():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_raw_candidate("12345678900019", "DUPONT BATIMENT")],
        },
        status=200,
    )

    records = fetch_sirene_candidates("test-api-key", "08")

    assert len(records) == 1
    request_url = responses.calls[0].request.url
    assert "codePostalEtablissement%3A08%2A" in request_url
    assert "etablissementSiege%3Atrue" in request_url


@responses.activate
def test_fetch_sirene_candidates_follows_pagination_cursor():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "*", "curseurSuivant": "PAGE2"},
            "etablissements": [_raw_candidate("12345678900019", "DUPONT BATIMENT")],
        },
        status=200,
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "PAGE2", "curseurSuivant": "PAGE2"},
            "etablissements": [_raw_candidate("98765432100019", "MARTIN SARL")],
        },
        status=200,
    )

    records = fetch_sirene_candidates("test-api-key", "08")

    assert [r["siret"] for r in records] == ["12345678900019", "98765432100019"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_sirene_candidates.py -v`
Expected: FAIL with `ImportError: cannot import name 'SIRENE_API_URL'`

- [ ] **Step 3: Implement `fetch_sirene_candidates`**

Add `import requests` (already present) and append to
`src/registry/matching/sirene_candidates.py`:

```python
def fetch_sirene_candidates(api_key: str, department: str, page_size: int = 100) -> list[dict]:
    records: list[dict] = []
    curseur = "*"
    headers = {"X-INSEE-Api-Key-Integration": api_key}
    query = (
        f"codePostalEtablissement:{department}* "
        f"AND etablissementSiege:true "
        f"AND etatAdministratifEtablissement:A"
    )

    while True:
        response = requests.get(
            SIRENE_API_URL,
            headers=headers,
            params={"q": query, "nombre": page_size, "curseur": curseur},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        records.extend(normalize_candidate(e) for e in payload["etablissements"])

        next_curseur = payload["header"]["curseurSuivant"]
        if next_curseur == curseur:
            break
        curseur = next_curseur

    return records
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_sirene_candidates.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/sirene_candidates.py tests/matching/test_sirene_candidates.py
git commit -m "feat: fetch SIRENE matching candidates for one department"
```

---

### Task 4: Bronze write + `run_candidate_ingestion`

**Files:**
- Modify: `src/registry/matching/sirene_candidates.py`
- Test: `tests/matching/test_sirene_candidates.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/matching/test_sirene_candidates.py`:

```python
import datetime as dt
import io

import boto3
import pyarrow.parquet as pq
from moto import mock_aws

from registry.matching.sirene_candidates import (
    bronze_object_key,
    run_candidate_ingestion,
    write_candidates_to_bronze,
)


def test_bronze_object_key_formats_department_and_date():
    key = bronze_object_key(dt.date(2026, 10, 4), "08")

    assert (
        key == "bronze/sirene_candidates/department=08/ingestion_date=2026-10-04/candidates.parquet"
    )


@mock_aws
def test_write_candidates_to_bronze_uploads_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.matching.sirene_candidates.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    records = [normalize_candidate(_raw_candidate("12345678900019", "DUPONT BATIMENT"))]

    key = write_candidates_to_bronze(
        records, bucket="lakehouse", work_dir=tmp_path, department="08"
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
    assert table.column("siret").to_pylist() == ["12345678900019"]


@responses.activate
@mock_aws
def test_run_candidate_ingestion_fetches_and_uploads_to_bronze(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.matching.sirene_candidates.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_raw_candidate("12345678900019", "DUPONT BATIMENT")],
        },
        status=200,
    )

    key = run_candidate_ingestion(
        api_key="test-api-key", bucket="lakehouse", department="08", work_dir=tmp_path
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
```

`normalize_candidate` and `_raw_candidate` are already imported/defined from
Tasks 2 and 3 earlier in this same test file — no new imports needed beyond the
ones shown above.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_sirene_candidates.py -v`
Expected: FAIL with `ImportError: cannot import name 'bronze_object_key'`

- [ ] **Step 3: Implement bronze write + orchestration**

Append to `src/registry/matching/sirene_candidates.py`:

```python
def bronze_object_key(ingestion_date: dt.date, department: str) -> str:
    return (
        f"bronze/sirene_candidates/department={department}/"
        f"ingestion_date={ingestion_date.isoformat()}/candidates.parquet"
    )


def write_candidates_to_bronze(
    records: list[dict], bucket: str, work_dir: Path, department: str
) -> str:
    table = pa.Table.from_pylist(
        records, schema=pa.schema([(name, pa.string()) for name in CANDIDATE_COLUMNS])
    )
    parquet_path = work_dir / "candidates.parquet"
    pq.write_table(table, parquet_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today(), department)
    upload_file(client, parquet_path, bucket, key)
    return key


def run_candidate_ingestion(api_key: str, bucket: str, department: str, work_dir: Path) -> str:
    records = fetch_sirene_candidates(api_key, department)
    return write_candidates_to_bronze(records, bucket, work_dir, department)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_sirene_candidates.py -v`
Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/sirene_candidates.py tests/matching/test_sirene_candidates.py
git commit -m "feat: land SIRENE matching candidates in bronze as Parquet"
```

---

### Task 5: `exact_siren_matches` and `unmatched_announcements`

**Files:**
- Create: `src/registry/matching/exact_siren.py`
- Test: `tests/matching/test_exact_siren.py`

- [ ] **Step 1: Write the failing tests**

```python
from pyspark.sql.types import StringType, StructField, StructType

from registry.matching.exact_siren import exact_siren_matches, unmatched_announcements

BODACC_SCHEMA = ["id", "siren_declared"]
CANDIDATES_SCHEMA = ["siren", "siret"]
BODACC_SCHEMA_TYPED = StructType(
    [StructField("id", StringType()), StructField("siren_declared", StringType())]
)


def test_exact_siren_matches_joins_on_declared_siren(spark_session):
    bodacc_df = spark_session.createDataFrame(
        [("A1", "552032534"), ("A2", "999999999")], schema=BODACC_SCHEMA
    )
    candidates_df = spark_session.createDataFrame(
        [("552032534", "55203253400019")], schema=CANDIDATES_SCHEMA
    )

    result = exact_siren_matches(bodacc_df, candidates_df).collect()

    assert len(result) == 1
    assert result[0].bodacc_announcement_id == "A1"
    assert result[0].siret_siege == "55203253400019"
    assert result[0].match_method == "exact_siren"
    assert result[0].match_confidence == 1.0


def test_exact_siren_matches_ignores_null_siren_declared(spark_session):
    bodacc_df = spark_session.createDataFrame([("A1", None)], schema=BODACC_SCHEMA_TYPED)
    candidates_df = spark_session.createDataFrame(
        [("552032534", "55203253400019")], schema=CANDIDATES_SCHEMA
    )

    result = exact_siren_matches(bodacc_df, candidates_df).collect()

    assert result == []


def test_unmatched_announcements_excludes_matched_ids(spark_session):
    bodacc_df = spark_session.createDataFrame(
        [("A1", "552032534"), ("A2", "999999999")], schema=BODACC_SCHEMA
    )
    exact_matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)],
        schema=[
            "bodacc_announcement_id",
            "siren_bodacc",
            "siret_siege",
            "match_method",
            "match_confidence",
        ],
    )

    result = unmatched_announcements(bodacc_df, exact_matches_df).collect()

    assert [row.id for row in result] == ["A2"]
```

**Note (post-implementation):** a plain column-name schema fails for the
single-row, all-`None`-in-one-column case (`PySparkValueError:
[CANNOT_DETERMINE_TYPE]`) — the same issue hit repeatedly since Plan 4 of Step 1.
`BODACC_SCHEMA_TYPED` (an explicit `StructType`) is only needed for that one test;
the others infer fine from non-null sample data.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_exact_siren.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.exact_siren'`

- [ ] **Step 3: Implement `exact_siren.py`**

```python
"""Deterministic SIREN-based matching between BODACC announcements and SIRENE
candidate siège establishments (see sirene_candidates.py)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def exact_siren_matches(bodacc_df: DataFrame, sirene_candidates_df: DataFrame) -> DataFrame:
    return (
        bodacc_df.filter(F.col("siren_declared").isNotNull())
        .join(
            sirene_candidates_df,
            bodacc_df["siren_declared"] == sirene_candidates_df["siren"],
            "inner",
        )
        .select(
            bodacc_df["id"].alias("bodacc_announcement_id"),
            bodacc_df["siren_declared"].alias("siren_bodacc"),
            sirene_candidates_df["siret"].alias("siret_siege"),
            F.lit("exact_siren").alias("match_method"),
            F.lit(1.0).alias("match_confidence"),
        )
    )


def unmatched_announcements(bodacc_df: DataFrame, exact_matches_df: DataFrame) -> DataFrame:
    matched_ids = exact_matches_df.select(F.col("bodacc_announcement_id").alias("matched_id"))
    return bodacc_df.join(matched_ids, bodacc_df["id"] == matched_ids["matched_id"], "left_anti")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_exact_siren.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/exact_siren.py tests/matching/test_exact_siren.py
git commit -m "feat: add deterministic exact-SIREN matching stage"
```

---

### Task 6: Data prep for Splink

**Files:**
- Create: `src/registry/matching/prepare.py`
- Test: `tests/matching/test_prepare.py`

- [ ] **Step 1: Write the failing tests**

```python
from pyspark.sql.types import StringType, StructField, StructType

from registry.matching.prepare import prepare_bodacc_for_matching, prepare_sirene_for_matching

BODACC_NULL_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("commercant", StringType()),
        StructField("code_postal", StringType()),
        StructField("ville", StringType()),
    ]
)


def test_prepare_bodacc_for_matching_cleans_and_renames(spark_session):
    df = spark_session.createDataFrame(
        [("A1", "  DUPONT Batiment  ", "08000", "Charleville-Mezieres")],
        schema=["id", "commercant", "code_postal", "ville"],
    )

    row = prepare_bodacc_for_matching(df).collect()[0]

    assert row.uid == "A1"
    assert row.denomination_clean == "dupont batiment"
    assert row.adresse_clean == "08000 charleville-mezieres"


def test_prepare_bodacc_for_matching_handles_nulls(spark_session):
    df = spark_session.createDataFrame(
        [("A1", None, None, None)],
        schema=BODACC_NULL_SCHEMA,
    )

    row = prepare_bodacc_for_matching(df).collect()[0]

    assert row.denomination_clean == ""
    assert row.adresse_clean == ""


def test_prepare_sirene_for_matching_cleans_and_renames(spark_session):
    df = spark_session.createDataFrame(
        [("55203253400019", "DUPONT Batiment SARL", "08000", "Charleville-Mezieres")],
        schema=["siret", "denomination", "code_postal", "libelle_commune"],
    )

    row = prepare_sirene_for_matching(df).collect()[0]

    assert row.uid == "55203253400019"
    assert row.denomination_clean == "dupont batiment sarl"
    assert row.adresse_clean == "08000 charleville-mezieres"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_prepare.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.prepare'`

- [ ] **Step 3: Implement `prepare.py`**

```python
"""Prepare BODACC and SIRENE candidate data for Splink fuzzy matching: both sides
reduce to the same two comparison fields, (uid, denomination_clean, adresse_clean),
at the same granularity (postal code + commune name — the only address detail
both sides have in common)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_bodacc_for_matching(bodacc_df: DataFrame) -> DataFrame:
    return bodacc_df.select(
        F.col("id").alias("uid"),
        F.lower(F.trim(F.coalesce(F.col("commercant"), F.lit("")))).alias("denomination_clean"),
        F.lower(
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("code_postal"), F.lit("")),
                    F.coalesce(F.col("ville"), F.lit("")),
                )
            )
        ).alias("adresse_clean"),
    )


def prepare_sirene_for_matching(sirene_candidates_df: DataFrame) -> DataFrame:
    return sirene_candidates_df.select(
        F.col("siret").alias("uid"),
        F.lower(F.trim(F.coalesce(F.col("denomination"), F.lit("")))).alias("denomination_clean"),
        F.lower(
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("code_postal"), F.lit("")),
                    F.coalesce(F.col("libelle_commune"), F.lit("")),
                )
            )
        ).alias("adresse_clean"),
    )
```

**Note (post-implementation):** the first version omitted the outer `F.trim(...)`
around `concat_ws`. `concat_ws` only skips true `NULL` arguments — it still joins
two empty-string arguments *with* the separator, so an all-null row produced
`" "` (one space) instead of `""`, caught by
`test_prepare_bodacc_for_matching_handles_nulls`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_prepare.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/prepare.py tests/matching/test_prepare.py
git commit -m "feat: prepare BODACC and SIRENE data for Splink comparison"
```

---

### Task 7: Splink linker wrapper

**Files:**
- Create: `src/registry/matching/splink_matching.py`

**Note:** no automated test for this task — see "Decisions made."
`build_linker`'s correctness (and the whole API surface it uses) was proven in
Task 1's smoke test; what's new here is just the specific comparisons/blocking
rule for this project's fields, exercised against real data in the deferred
Task 10.

- [ ] **Step 1: Implement `splink_matching.py`**

```python
"""Splink-based fuzzy matching between BODACC announcements and SIRENE candidate
siège establishments. Same matching rules run on either backend — pass a
DuckDBAPI() for fast local dev/test, or a SparkAPI(spark_session=...) for the
real pipeline against the lakehouse (see docs/superpowers/specs/
2026-10-03-bodacc-entity-resolution-design.md)."""

from __future__ import annotations

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

BLOCKING_RULE = block_on("substr(denomination_clean, 1, 4)")

SPLINK_SETTINGS = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="uid",
    comparisons=[
        cl.JaroWinklerAtThresholds("denomination_clean"),
        cl.LevenshteinAtThresholds("adresse_clean"),
    ],
    blocking_rules_to_generate_predictions=[BLOCKING_RULE],
    retain_intermediate_calculation_columns=False,
)


def build_linker(bodacc_prepared, sirene_prepared, db_api):
    return Linker([bodacc_prepared, sirene_prepared], SPLINK_SETTINGS, db_api=db_api)


def train_linker(linker) -> None:
    linker.training.estimate_probability_two_random_records_match([BLOCKING_RULE], recall=0.7)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.training.estimate_parameters_using_expectation_maximisation(BLOCKING_RULE)


def predict_fuzzy_matches(linker, threshold: float = 0.5):
    return linker.inference.predict(threshold_match_probability=threshold)
```

- [ ] **Step 2: Commit**

```bash
git add src/registry/matching/splink_matching.py
git commit -m "feat: add Splink linker wrapper for BODACC/SIRENE fuzzy matching"
```

---

### Task 8: Combine exact and fuzzy match results

**Files:**
- Create: `src/registry/matching/combine.py`
- Test: `tests/matching/test_combine.py`

This task's functions take a plausibly-shaped "Splink predict output" DataFrame
built by hand in the tests — no real Splink `Linker` involved — which is exactly
what makes this logic testable despite Task 7 having no pytest coverage.

- [ ] **Step 1: Write the failing tests**

```python
from registry.matching.combine import (
    best_fuzzy_match_per_announcement,
    combine_match_results,
    extract_fuzzy_match_candidates,
)

PREDICTIONS_SCHEMA = ["uid_l", "uid_r", "match_probability"]
MATCH_SCHEMA = [
    "bodacc_announcement_id",
    "siren_bodacc",
    "siret_siege",
    "match_method",
    "match_confidence",
]


def test_extract_fuzzy_match_candidates_renames_columns(spark_session):
    df = spark_session.createDataFrame([("A1", "S1", 0.87)], schema=PREDICTIONS_SCHEMA)

    row = extract_fuzzy_match_candidates(df).collect()[0]

    assert row.bodacc_announcement_id == "A1"
    assert row.siret_siege == "S1"
    assert row.match_method == "splink_fuzzy"
    assert row.match_confidence == 0.87


def test_best_fuzzy_match_per_announcement_keeps_highest_confidence(spark_session):
    df = spark_session.createDataFrame(
        [
            ("A1", "S1", "splink_fuzzy", 0.6),
            ("A1", "S2", "splink_fuzzy", 0.9),
            ("A2", "S3", "splink_fuzzy", 0.7),
        ],
        schema=["bodacc_announcement_id", "siret_siege", "match_method", "match_confidence"],
    )

    result = best_fuzzy_match_per_announcement(df).orderBy("bodacc_announcement_id").collect()

    assert len(result) == 2
    assert result[0].siret_siege == "S2"
    assert result[0].match_confidence == 0.9
    assert result[1].siret_siege == "S3"


def test_combine_match_results_unions_exact_and_fuzzy(spark_session):
    exact_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )
    fuzzy_df = spark_session.createDataFrame(
        [("A2", None, "73282932000014", "splink_fuzzy", 0.8)], schema=MATCH_SCHEMA
    )

    result = combine_match_results(exact_df, fuzzy_df).orderBy("bodacc_announcement_id").collect()

    assert [row.bodacc_announcement_id for row in result] == ["A1", "A2"]
    assert [row.match_method for row in result] == ["exact_siren", "splink_fuzzy"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/matching/test_combine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'registry.matching.combine'`

- [ ] **Step 3: Implement `combine.py`**

```python
"""Combine deterministic (exact SIREN) and probabilistic (Splink) match results
into a single unified match-record shape: (bodacc_announcement_id, siret_siege,
match_method, match_confidence)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extract_fuzzy_match_candidates(predictions_df: DataFrame) -> DataFrame:
    return predictions_df.select(
        F.col("uid_l").alias("bodacc_announcement_id"),
        F.lit(None).cast("string").alias("siren_bodacc"),
        F.col("uid_r").alias("siret_siege"),
        F.lit("splink_fuzzy").alias("match_method"),
        F.col("match_probability").alias("match_confidence"),
    )


def best_fuzzy_match_per_announcement(fuzzy_matches_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("bodacc_announcement_id").orderBy(F.desc("match_confidence"))
    return (
        fuzzy_matches_df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


def combine_match_results(exact_matches_df: DataFrame, fuzzy_matches_df: DataFrame) -> DataFrame:
    best_fuzzy_df = best_fuzzy_match_per_announcement(fuzzy_matches_df)
    return exact_matches_df.unionByName(best_fuzzy_df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/matching/test_combine.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/registry/matching/combine.py tests/matching/test_combine.py
git commit -m "feat: combine exact and fuzzy match results into one shape"
```

---

### Task 9: Full automated verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite and lint**

```bash
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Expected: all 62 tests pass (46 from Step 1 and Step 2 Plan 1, plus 16 new: 2 from
Task 2, 2 from Task 3, 3 from Task 4, 3 from Task 5, 3 from Task 6, 3 from
Task 8), lint and format clean.

---

### Task 10: Manual verification against the real Sirene API and BODACC data (deferred)

**Files:** none (manual verification only)

**This task requires a Sirene API key and is not required to consider this plan
complete** — same situation as Step 1 Plan 3's Task 10. Do this once
`SIRENE_API_KEY` is set in `.env`.

- [ ] **Step 1: Fetch real department-08 SIRENE candidates**

```bash
docker compose up -d garage
set -a && source .env && set +a
uv run python -c "
from registry.matching.sirene_candidates import run_candidate_ingestion
from pathlib import Path
import os

key = run_candidate_ingestion(
    api_key=os.environ['SIRENE_API_KEY'],
    bucket='lakehouse',
    department='08',
    work_dir=Path('/tmp'),
)
print(key)
"
```

Expected: prints a key of the form
`bronze/sirene_candidates/department=08/ingestion_date=<today>/candidates.parquet`.
Sanity-check the row count is plausible for one department's siège establishments
(likely a few thousand to low tens of thousands — much smaller than BODACC's
~9,400 *announcements*, since this counts distinct companies, not events).

- [ ] **Step 2: Run exact + fuzzy matching against real data (DuckDB backend first)**

```bash
uv run python -c "
import datetime as dt
from registry.transform.spark_session import build_lakehouse_session
from registry.matching.exact_siren import exact_siren_matches, unmatched_announcements
from registry.matching.prepare import prepare_bodacc_for_matching, prepare_sirene_for_matching
from registry.matching.splink_matching import build_linker, train_linker, predict_fuzzy_matches
from registry.matching.combine import extract_fuzzy_match_candidates, combine_match_results
from splink.backends.duckdb import DuckDBAPI

spark = build_lakehouse_session()
bodacc_df = spark.table('lakehouse.silver.bodacc_annonces')
candidates_df = spark.read.parquet(f's3a://lakehouse/bronze/sirene_candidates/department=08/ingestion_date={dt.date.today().isoformat()}/candidates.parquet')

exact_df = exact_siren_matches(bodacc_df, candidates_df)
print('Exact matches:', exact_df.count())

unmatched_df = unmatched_announcements(bodacc_df, exact_df)
print('Unmatched (candidates for fuzzy stage):', unmatched_df.count())

bodacc_prepared = prepare_bodacc_for_matching(unmatched_df).toPandas()
sirene_prepared = prepare_sirene_for_matching(candidates_df).toPandas()

linker = build_linker(bodacc_prepared, sirene_prepared, DuckDBAPI())
train_linker(linker)
predictions = predict_fuzzy_matches(linker).as_pandas_dataframe()
print('Fuzzy candidate pairs above threshold:', len(predictions))
print(predictions[['uid_l', 'uid_r', 'match_probability']].sort_values('match_probability', ascending=False).head(10))
"
```

Expected: completes without error; prints a plausible exact-match count (likely
small — BODACC's `siren_declared` is often unreliable, which is the whole reason
this plan exists), a much larger unmatched count, and a handful of fuzzy
candidate pairs with their match probabilities. Spot-check the top few by eye:
does a high `match_probability` pair actually look like the same company?

- [ ] **Step 3: Re-run the fuzzy stage on the Spark backend**

```bash
uv run python -c "
from registry.matching.splink_matching import build_linker, train_linker, predict_fuzzy_matches
from splink.backends.spark import SparkAPI, similarity_jar_location
# ... same setup as Step 2, but:
# linker = build_linker(bodacc_prepared_spark_df, sirene_prepared_spark_df, SparkAPI(spark_session=spark))
"
```

Splink's Spark backend needs an extra similarity-functions JAR
(`splink.backends.spark.similarity_jar_location()`) added to
`spark.jars` for some comparison levels — check this function's return value and
add it to `build_lakehouse_session`'s config if predictions differ from the
DuckDB run or fail outright. This wasn't needed for Task 1-9's DuckDB-only path,
so it's genuinely untested until this step.

Expected: produces the same (or very similar — floating point / implementation
differences are possible) match probabilities as the DuckDB run in Step 2,
confirming the backend-swap design actually works end to end on real data.
