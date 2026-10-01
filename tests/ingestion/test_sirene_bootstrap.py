import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import responses

from registry.ingestion.sirene_bootstrap import (
    bronze_object_key,
    convert_csv_to_parquet,
    download_sirene_stock,
)


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 1))

    assert key == "bronze/sirene/stock/ingestion_date=2026-10-01/stock.parquet"


def test_convert_csv_to_parquet_preserves_all_columns_as_strings(tmp_path: Path, fixture_csv_path: Path):
    parquet_path = tmp_path / "out.parquet"

    convert_csv_to_parquet(fixture_csv_path, parquet_path)

    table = pq.read_table(parquet_path)
    assert table.num_rows == 2
    assert table.column("siren").to_pylist() == ["552032534", "732829320"]
    assert all(field.type == pa.string() for field in table.schema)


@responses.activate
def test_download_sirene_stock_writes_response_body(tmp_path: Path):
    url = "https://example.test/stock.csv"
    responses.add(responses.GET, url, body=b"siren,nic\n123,001\n", status=200)
    dest_path = tmp_path / "stock.csv"

    download_sirene_stock(url, dest_path)

    assert dest_path.read_bytes() == b"siren,nic\n123,001\n"
