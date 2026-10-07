"""Daily DAG: BODACC diff -> bronze -> silver -> matching cascade -> gold (SCD2 merge)."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="bodacc_matching_pipeline",
    schedule="@daily",
    start_date=dt.datetime(2026, 10, 1),
    catchup=False,
)
def bodacc_matching_pipeline():
    @task
    def extract_bodacc_daily_diff(data_interval_start=None, data_interval_end=None) -> str:
        from registry.ingestion.bodacc import run_ingestion

        return run_ingestion(
            bucket=os.environ["LAKEHOUSE_BUCKET"],
            since=data_interval_start.date(),
            until=data_interval_end.date(),
            work_dir=Path("/tmp"),
            run_type="diff",
        )

    @task
    def transform_bronze_to_silver(bronze_key: str) -> str:
        from registry.transform.bodacc_to_silver import bronze_to_silver
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        silver_table = "lakehouse.silver.bodacc_annonces"
        spark = build_lakehouse_session()
        try:
            bronze_to_silver(spark, f"s3a://{bucket}/{bronze_key}", silver_table)
        finally:
            spark.stop()
        return silver_table

    @task
    def run_matching_and_historize(silver_table: str, logical_date=None) -> None:
        from splink import SparkAPI

        from registry.matching.cascade import run_matching_cascade
        from registry.matching.gold_links import (
            ensure_gold_links_table,
            find_unlinked_announcements,
            historize_match_results,
        )
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        gold_table = "lakehouse.gold.bodacc_sirene_links"
        silver_links_table = "lakehouse.silver.bodacc_sirene_links"
        candidates_key = os.environ["SIRENE_CANDIDATES_BRONZE_KEY"]

        spark = build_lakehouse_session()
        spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
        try:
            ensure_gold_links_table(spark, gold_table)

            bodacc_df = spark.table(silver_table)
            gold_links_df = spark.table(gold_table)
            backlog = find_unlinked_announcements(bodacc_df, gold_links_df)

            sirene_candidates_df = spark.read.parquet(f"s3a://{bucket}/{candidates_key}")

            db_api = SparkAPI(spark_session=spark)
            matches_df = run_matching_cascade(backlog, sirene_candidates_df, db_api)

            historize_match_results(
                spark, matches_df, silver_links_table, gold_table, logical_date.date()
            )
        finally:
            spark.stop()

    bronze_key = extract_bodacc_daily_diff()
    silver_table = transform_bronze_to_silver(bronze_key)
    run_matching_and_historize(silver_table)


bodacc_matching_pipeline()
