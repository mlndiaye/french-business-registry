import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from registry.ingestion.sirene_bootstrap import bronze_object_key, convert_csv_to_parquet


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
