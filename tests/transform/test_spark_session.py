from registry.transform.spark_session import lakehouse_spark_configs


def test_lakehouse_spark_configs_sets_s3a_and_iceberg_catalog(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lakehouse")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:3900")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-key")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")
    monkeypatch.setenv("S3_REGION", "garage")

    configs = lakehouse_spark_configs()

    assert configs["spark.sql.catalog.lakehouse.warehouse"] == "s3a://lakehouse/warehouse"
    assert configs["spark.hadoop.fs.s3a.endpoint"] == "http://localhost:3900"
    assert configs["spark.hadoop.fs.s3a.endpoint.region"] == "garage"
    assert configs["spark.hadoop.fs.s3a.access.key"] == "test-key"
    assert configs["spark.hadoop.fs.s3a.secret.key"] == "test-secret"
    assert configs["spark.hadoop.fs.s3a.multiobjectdelete.enable"] == "false"
