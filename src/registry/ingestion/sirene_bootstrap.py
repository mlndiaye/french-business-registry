"""Bootstrap ingestion of the full SIRENE establishment stock file into the bronze layer."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq


def bronze_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/sirene/stock/ingestion_date={ingestion_date.isoformat()}/stock.parquet"


def convert_csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    with open(csv_path, encoding="utf-8") as f:
        header = f.readline().strip().split(",")

    column_types = {name: pa.string() for name in header}
    table = pa_csv.read_csv(
        csv_path,
        convert_options=pa_csv.ConvertOptions(column_types=column_types),
    )
    pq.write_table(table, parquet_path)
