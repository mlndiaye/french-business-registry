"""SCD2 historization of resolved/validated DECP markets into
`gold.decp_marches_links`, following the exact same two-statement MERGE+INSERT
pattern as `gold_links.py` (BODACC) and `silver_to_gold.py` (SIRENE)."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession


def write_matches_to_silver(matches_df: DataFrame, silver_table: str) -> None:
    matches_df.writeTo(silver_table).createOrReplace()


def ensure_gold_decp_links_table(spark: SparkSession, gold_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            uid STRING,
            siret_titulaire STRING,
            match_method STRING,
            siret_validated_in_sirene BOOLEAN,
            acheteur_id STRING,
            acheteur_nom STRING,
            montant DOUBLE,
            objet STRING,
            code_cpv STRING,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        ) USING iceberg
    """)


TRACKED_COLUMNS = ["siret_titulaire", "match_method", "montant"]


def apply_decp_links_scd2_merge(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    spark.table(silver_table).createOrReplaceTempView("incoming_links")

    comparison = " AND ".join(f"incoming_links.{c} <=> gold.{c}" for c in TRACKED_COLUMNS)

    changed_links = spark.sql(f"""
        SELECT incoming_links.uid
        FROM incoming_links
        JOIN {gold_table} AS gold
          ON incoming_links.uid = gold.uid
         AND gold.is_current = true
        WHERE NOT ({comparison})
    """)
    changed_links.createOrReplaceTempView("changed_links")

    spark.sql(f"""
        MERGE INTO {gold_table} AS gold
        USING changed_links AS c
        ON gold.uid = c.uid AND gold.is_current = true
        WHEN MATCHED THEN UPDATE SET
            gold.valid_to = DATE('{run_date.isoformat()}'),
            gold.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT
            incoming_links.uid,
            incoming_links.siret_titulaire,
            incoming_links.match_method,
            incoming_links.siret_validated_in_sirene,
            incoming_links.acheteur_id,
            incoming_links.acheteur_nom,
            incoming_links.montant,
            incoming_links.objet,
            incoming_links.code_cpv,
            DATE('{run_date.isoformat()}') AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            true AS is_current
        FROM incoming_links
        LEFT JOIN {gold_table} AS gold
          ON incoming_links.uid = gold.uid
         AND gold.is_current = true
        WHERE gold.uid IS NULL
    """)


def historize_decp_links(
    spark: SparkSession,
    matches_df: DataFrame,
    silver_table: str,
    gold_table: str,
    run_date: dt.date,
) -> None:
    write_matches_to_silver(matches_df, silver_table)
    ensure_gold_decp_links_table(spark, gold_table)
    apply_decp_links_scd2_merge(spark, silver_table, gold_table, run_date)
