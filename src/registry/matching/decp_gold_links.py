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
