"""Bootstrap ingestion of the full SIRENE establishment stock file into the bronze layer."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file


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


def download_sirene_stock(url: str, dest_path: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, None)) as response:
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def run_bootstrap(stock_url: str, bucket: str, work_dir: Path) -> str:
    csv_path = work_dir / "stock.csv"
    parquet_path = work_dir / "stock.parquet"

    download_sirene_stock(stock_url, csv_path)
    convert_csv_to_parquet(csv_path, parquet_path)

    client = get_s3_client()
    key = bronze_object_key(dt.date.today())
    upload_file(client, parquet_path, bucket, key)
    return key
