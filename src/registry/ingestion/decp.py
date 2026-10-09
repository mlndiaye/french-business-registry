"""Ingestion of DECP (public procurement) consolidated data: a single
full-replacement national Parquet file, filtered locally to this project's
scope (see docs/superpowers/specs/2026-10-08-decp-procurement-design.md)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file


def bronze_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/decp/ingestion_date={ingestion_date.isoformat()}/marches.parquet"


def filter_decp_to_scope(parquet_path: Path, department: str, since: dt.date) -> pa.Table:
    table = pq.read_table(parquet_path)
    mask = pc.and_(
        pc.and_(
            pc.equal(table["acheteur_departement_code"], department),
            pc.greater_equal(table["datePublicationDonnees"], pa.scalar(since, type=pa.date32())),
        ),
        pc.equal(table["donneesActuelles"], True),
    )
    return table.filter(mask)


def download_decp_national_file(url: str, dest_path: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, None)) as response:
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def run_ingestion(url: str, bucket: str, department: str, since: dt.date, work_dir: Path) -> str:
    national_path = work_dir / "decp_national.parquet"
    scoped_path = work_dir / "decp_scoped.parquet"

    download_decp_national_file(url, national_path)
    scoped_table = filter_decp_to_scope(national_path, department, since)
    pq.write_table(scoped_table, scoped_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today())
    upload_file(client, scoped_path, bucket, key)
    return key
