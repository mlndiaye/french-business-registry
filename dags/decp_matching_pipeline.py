"""Daily DAG: DECP national file -> filtered bronze -> silver -> resolve/validate
-> gold (SCD2 merge). No backlog step and no Splink — see Plan 4's "Decisions
made" for why this DAG is structurally simpler than bodacc_matching_pipeline."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="decp_matching_pipeline",
    schedule="@daily",
    start_date=dt.datetime(2026, 10, 1),
    catchup=False,
)
def decp_matching_pipeline():
    @task
    def extract_decp(logical_date=None) -> str:
        from registry.ingestion.decp import run_ingestion

        since = logical_date.date() - dt.timedelta(days=365)
        return run_ingestion(
            url=os.environ["DECP_PARQUET_URL"],
            bucket=os.environ["LAKEHOUSE_BUCKET"],
            department="08",
            since=since,
            work_dir=Path("/tmp"),
        )

    @task
    def transform_bronze_to_silver(bronze_key: str) -> str:
        from registry.transform.decp_to_silver import bronze_to_silver
        from registry.transform.spark_session import build_lakehouse_session

        bucket = os.environ["LAKEHOUSE_BUCKET"]
        silver_table = "lakehouse.silver.decp_marches"
        spark = build_lakehouse_session()
        try:
            bronze_to_silver(spark, f"s3a://{bucket}/{bronze_key}", silver_table)
        finally:
            spark.stop()
        return silver_table

    @task
    def resolve_and_historize(silver_table: str, logical_date=None) -> None:
        from registry.matching.decp_gold_links import historize_decp_links
        from registry.matching.decp_resolution import (
            resolve_decp_titulaires,
            validate_against_sirene,
        )
        from registry.transform.spark_session import build_lakehouse_session

        gold_table = "lakehouse.gold.decp_marches_links"
        silver_links_table = "lakehouse.silver.decp_marches_links"

        spark = build_lakehouse_session()
        try:
            decp_df = spark.table(silver_table)
            resolved_df = resolve_decp_titulaires(decp_df)

            sirene_gold_df = spark.table("lakehouse.gold.sirene_etablissements_historized")
            validated_df = validate_against_sirene(resolved_df, sirene_gold_df)

            historize_decp_links(
                spark, validated_df, silver_links_table, gold_table, logical_date.date()
            )
        finally:
            spark.stop()

    bronze_key = extract_decp()
    silver_table = transform_bronze_to_silver(bronze_key)
    resolve_and_historize(silver_table)


decp_matching_pipeline()
