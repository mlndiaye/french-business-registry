"""SCD2 historization of BODACC/SIRENE match results into `gold.bodacc_sirene_links`,
following the exact same two-statement MERGE+INSERT pattern as Step 1's
`silver_to_gold.py`."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession


def write_matches_to_silver(matches_df: DataFrame, silver_table: str) -> None:
    matches_df.writeTo(silver_table).createOrReplace()


def ensure_gold_links_table(spark: SparkSession, gold_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            bodacc_announcement_id STRING,
            siren_bodacc STRING,
            siret_siege STRING,
            match_method STRING,
            match_confidence DOUBLE,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        ) USING iceberg
    """)
