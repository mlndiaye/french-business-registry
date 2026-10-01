"""Production SparkSession factory wired to the Garage/S3A-backed Iceberg lakehouse."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"


def lakehouse_spark_configs() -> dict[str, str]:
    bucket = os.environ["LAKEHOUSE_BUCKET"]
    return {
        "spark.jars.packages": f"{ICEBERG_PACKAGE},{HADOOP_AWS_PACKAGE},{AWS_SDK_PACKAGE}",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakehouse.type": "hadoop",
        "spark.sql.catalog.lakehouse.warehouse": f"s3a://{bucket}/warehouse",
        "spark.hadoop.fs.s3a.endpoint": os.environ["S3_ENDPOINT_URL"],
        "spark.hadoop.fs.s3a.access.key": os.environ["S3_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["S3_SECRET_KEY"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }


def build_lakehouse_session(app_name: str = "registry") -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in lakehouse_spark_configs().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
