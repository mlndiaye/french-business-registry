import datetime as dt

from registry.ingestion.sirene_bootstrap import bronze_object_key


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 1))

    assert key == "bronze/sirene/stock/ingestion_date=2026-10-01/stock.parquet"
