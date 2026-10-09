import datetime as dt
import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import responses
from moto import mock_aws

from registry.ingestion.decp import (
    bronze_object_key,
    download_decp_national_file,
    filter_decp_to_scope,
    run_ingestion,
)


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 9))

    assert key == "bronze/decp/ingestion_date=2026-10-09/marches.parquet"


def _make_national_table() -> pa.Table:
    return pa.table(
        {
            "uid": ["M1", "M2", "M3", "M4"],
            "acheteur_departement_code": ["08", "51", "08", "08"],
            "datePublicationDonnees": pa.array(
                [
                    dt.date(2026, 5, 1),
                    dt.date(2026, 5, 1),
                    dt.date(2024, 1, 1),
                    dt.date(2026, 5, 1),
                ],
                type=pa.date32(),
            ),
            "donneesActuelles": [True, True, True, False],
        }
    )


def test_filter_decp_to_scope_keeps_only_matching_rows(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert result.column("uid").to_pylist() == ["M1"]


def test_filter_decp_to_scope_excludes_wrong_department(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert "M2" not in result.column("uid").to_pylist()


def test_filter_decp_to_scope_excludes_rows_before_since(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert "M3" not in result.column("uid").to_pylist()


def test_filter_decp_to_scope_excludes_superseded_modifications(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert "M4" not in result.column("uid").to_pylist()


@responses.activate
def test_download_decp_national_file_writes_response_body(tmp_path):
    url = "https://example.test/decp.parquet"
    responses.add(responses.GET, url, body=b"fake-parquet-bytes", status=200)
    dest_path = tmp_path / "decp_national.parquet"

    download_decp_national_file(url, dest_path)

    assert dest_path.read_bytes() == b"fake-parquet-bytes"


@responses.activate
@mock_aws
def test_run_ingestion_uploads_filtered_parquet_to_bronze(tmp_path, monkeypatch):
    # moto only intercepts requests to real AWS-style endpoints, not custom ones
    # like Garage's, so get_s3_client is swapped for a moto-compatible client here
    # (same workaround used in test_sirene_bootstrap.py).
    monkeypatch.setattr(
        "registry.ingestion.decp.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )

    national_path = tmp_path / "source_national.parquet"
    pq.write_table(_make_national_table(), national_path)

    url = "https://example.test/decp.parquet"
    responses.add(responses.GET, url, body=national_path.read_bytes(), status=200)

    key = run_ingestion(
        url,
        bucket="lakehouse",
        department="08",
        since=dt.date(2025, 10, 9),
        work_dir=tmp_path,
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    result_table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert result_table.column("uid").to_pylist() == ["M1"]
