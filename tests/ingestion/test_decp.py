import datetime as dt

from registry.ingestion.decp import bronze_object_key


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 9))

    assert key == "bronze/decp/ingestion_date=2026-10-09/marches.parquet"
