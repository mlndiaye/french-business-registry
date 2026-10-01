from pathlib import Path

import boto3
from moto import mock_aws

from registry.ingestion.storage import ensure_bucket, get_s3_client, upload_file


def test_get_s3_client_uses_env_configuration(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:3900")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-key")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")

    client = get_s3_client()

    assert client.meta.endpoint_url == "http://localhost:3900"


@mock_aws
def test_ensure_bucket_is_idempotent():
    client = boto3.client("s3", region_name="us-east-1")

    ensure_bucket(client, "lakehouse")
    ensure_bucket(client, "lakehouse")

    response = client.list_buckets()
    bucket_names = [b["Name"] for b in response["Buckets"]]
    assert bucket_names == ["lakehouse"]


@mock_aws
def test_upload_file_creates_object(tmp_path: Path):
    client = boto3.client("s3", region_name="us-east-1")
    local_file = tmp_path / "data.parquet"
    local_file.write_bytes(b"fake-parquet-bytes")

    upload_file(client, local_file, bucket="lakehouse", key="bronze/sirene/data.parquet")

    body = client.get_object(Bucket="lakehouse", Key="bronze/sirene/data.parquet")["Body"].read()
    assert body == b"fake-parquet-bytes"
