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


TRACKED_COLUMNS = ["siret_siege", "match_method"]


def apply_links_scd2_merge(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    spark.table(silver_table).createOrReplaceTempView("incoming_links")

    comparison = " AND ".join(f"incoming_links.{c} <=> gold.{c}" for c in TRACKED_COLUMNS)

    changed_links = spark.sql(f"""
        SELECT incoming_links.bodacc_announcement_id
        FROM incoming_links
        JOIN {gold_table} AS gold
          ON incoming_links.bodacc_announcement_id = gold.bodacc_announcement_id
         AND gold.is_current = true
        WHERE NOT ({comparison})
    """)
    changed_links.createOrReplaceTempView("changed_links")

    spark.sql(f"""
        MERGE INTO {gold_table} AS gold
        USING changed_links AS c
        ON gold.bodacc_announcement_id = c.bodacc_announcement_id AND gold.is_current = true
        WHEN MATCHED THEN UPDATE SET
            gold.valid_to = DATE('{run_date.isoformat()}'),
            gold.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT
            incoming_links.bodacc_announcement_id,
            incoming_links.siren_bodacc,
            incoming_links.siret_siege,
            incoming_links.match_method,
            incoming_links.match_confidence,
            DATE('{run_date.isoformat()}') AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            true AS is_current
        FROM incoming_links
        LEFT JOIN {gold_table} AS gold
          ON incoming_links.bodacc_announcement_id = gold.bodacc_announcement_id
         AND gold.is_current = true
        WHERE gold.bodacc_announcement_id IS NULL
    """)
