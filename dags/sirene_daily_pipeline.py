"""Daily DAG: Sirene API diff -> bronze -> silver -> gold (SCD2 merge)."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="sirene_daily_pipeline",
    schedule="@daily",
    start_date=dt.datetime(2026, 10, 1),
    catchup=False,
)
def sirene_daily_pipeline():
    @task
    def extract_daily_diff(data_interval_start=None, data_interval_end=None) -> str:
        from registry.ingestion.sirene_diff import run_daily_diff

        return run_daily_diff(
            api_key=os.environ["SIRENE_API_KEY"],
            bucket=os.environ["LAKEHOUSE_BUCKET"],
            since=data_interval_start.date(),
            until=data_interval_end.date(),
            work_dir=Path("/tmp"),
        )

    @task
    def transform_bronze_to_silver(bronze_key: str) -> str:
        from registry.transform.bronze_to_silver import bronze_to_silver
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        silver_table = "lakehouse.silver.sirene_etablissements"
        spark = build_lakehouse_session()
        try:
            bronze_to_silver(spark, f"s3a://{bucket}/{bronze_key}", silver_table)
        finally:
            spark.stop()
        return silver_table

    @task
    def transform_silver_to_gold(silver_table: str, logical_date=None) -> None:
        from registry.transform.silver_to_gold import silver_to_gold
        from registry.transform.spark_session import build_lakehouse_session

        gold_table = "lakehouse.gold.sirene_etablissements_historized"
        spark = build_lakehouse_session()
        try:
            silver_to_gold(spark, silver_table, gold_table, logical_date.date())
        finally:
            spark.stop()

    bronze_key = extract_daily_diff()
    silver_table = transform_bronze_to_silver(bronze_key)
    transform_silver_to_gold(silver_table)


sirene_daily_pipeline()
