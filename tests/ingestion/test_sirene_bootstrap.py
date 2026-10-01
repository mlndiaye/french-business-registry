import datetime as dt
import io
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import responses
from moto import mock_aws

from registry.ingestion.sirene_bootstrap import (
    bronze_object_key,
    convert_csv_to_parquet,
    download_sirene_stock,
    run_bootstrap,
)


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 1))

    assert key == "bronze/sirene/stock/ingestion_date=2026-10-01/stock.parquet"


def test_convert_csv_to_parquet_preserves_all_columns_as_strings(
    tmp_path: Path, fixture_csv_path: Path
):
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


@responses.activate
@mock_aws
def test_run_bootstrap_uploads_parquet_to_bronze(
    tmp_path: Path, monkeypatch, fixture_csv_path: Path
):
    # moto only intercepts requests to real AWS-style endpoints, not custom ones
    # like MinIO's, so get_s3_client is swapped for a moto-compatible client here.
    monkeypatch.setattr(
        "registry.ingestion.sirene_bootstrap.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )

    url = "https://example.test/stock.csv"
    responses.add(responses.GET, url, body=fixture_csv_path.read_bytes(), status=200)

    key = run_bootstrap(url, bucket="lakehouse", work_dir=tmp_path)

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 2
