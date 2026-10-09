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
