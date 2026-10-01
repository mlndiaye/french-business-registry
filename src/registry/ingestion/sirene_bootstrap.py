"""Bootstrap ingestion of the full SIRENE establishment stock file into the bronze layer."""

from __future__ import annotations

import datetime as dt


def bronze_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/sirene/stock/ingestion_date={ingestion_date.isoformat()}/stock.parquet"
