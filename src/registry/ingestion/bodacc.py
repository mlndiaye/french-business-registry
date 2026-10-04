"""Ingestion of BODACC legal announcements (bootstrap and daily diffs)."""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file

SIREN_IN_TEXT_PATTERN = re.compile(r"\b(\d[\d ]{0,11}\d)\b")

BODACC_API_URL = (
    "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "annonces-commerciales/records"
)


def extract_siren_from_registre(registre: str | list[str] | None) -> str | None:
    if not registre:
        return None
    if isinstance(registre, list):
        registre = registre[0]
    match = SIREN_IN_TEXT_PATTERN.search(registre)
    if not match:
        return None
    digits = match.group(1).replace(" ", "")
    if len(digits) != 9:
        return None
    return digits


BODACC_COLUMNS = [
    "id",
    "dateparution",
    "numeroannonce",
    "typeavis_lib",
    "familleavis_lib",
    "tribunal",
    "commercant",
    "siren_declared",
    "ville",
    "cp",
    "denomination",
]


def _to_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def normalize_announcement(raw: dict) -> dict:
    siren = raw.get("siren") or extract_siren_from_registre(raw.get("registre"))
    return {
        "id": _to_str(raw.get("id")),
        "dateparution": _to_str(raw.get("dateparution")),
        "numeroannonce": _to_str(raw.get("numeroannonce")),
        "typeavis_lib": _to_str(raw.get("typeavis_lib")),
        "familleavis_lib": _to_str(raw.get("familleavis_lib")),
        "tribunal": _to_str(raw.get("tribunal")),
        "commercant": _to_str(raw.get("commercant")),
        "siren_declared": _to_str(siren),
        "ville": _to_str(raw.get("ville")),
        "cp": _to_str(raw.get("cp")),
        "denomination": _to_str(raw.get("denomination")),
    }


def fetch_bodacc_announcements(since: dt.date, until: dt.date, page_size: int = 100) -> list[dict]:
    records: list[dict] = []
    offset = 0
    where_clause = (
        f"dateparution >= date'{since.isoformat()}' AND dateparution < date'{until.isoformat()}'"
    )

    while True:
        response = requests.get(
            BODACC_API_URL,
            params={"where": where_clause, "limit": page_size, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        records.extend(normalize_announcement(r) for r in results)

        if len(results) < page_size:
            break
        offset += page_size

    return records


def bronze_object_key(ingestion_date: dt.date, run_type: str) -> str:
    return f"bronze/bodacc/{run_type}/ingestion_date={ingestion_date.isoformat()}/bodacc.parquet"


def write_announcements_to_bronze(
    records: list[dict], bucket: str, work_dir: Path, run_type: str
) -> str:
    table = pa.Table.from_pylist(
        records, schema=pa.schema([(name, pa.string()) for name in BODACC_COLUMNS])
    )
    parquet_path = work_dir / "bodacc.parquet"
    pq.write_table(table, parquet_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today(), run_type)
    upload_file(client, parquet_path, bucket, key)
    return key


def run_ingestion(
    bucket: str, since: dt.date, until: dt.date, work_dir: Path, run_type: str
) -> str:
    records = fetch_bodacc_announcements(since, until)
    return write_announcements_to_bronze(records, bucket, work_dir, run_type)
