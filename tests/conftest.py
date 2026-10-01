import uuid
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"


@pytest.fixture
def fixture_csv_path() -> Path:
    return Path(__file__).parent / "fixtures" / "sirene_stock_sample.csv"


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.appName("registry-tests")
        .master("local[1]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def table_suffix() -> str:
    return uuid.uuid4().hex[:8]
