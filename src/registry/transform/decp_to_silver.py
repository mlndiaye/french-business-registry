"""Bronze-to-silver cleaning and typing of DECP public procurement markets."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def clean_decp_bronze(df: DataFrame) -> DataFrame:
    return df.filter((F.col("uid").isNotNull()) & (F.col("uid") != "")).select(
        F.col("uid").alias("uid"),
        F.col("acheteur_id").alias("acheteur_id"),
        F.col("acheteur_nom").alias("acheteur_nom"),
        F.col("titulaire_id").alias("titulaire_id"),
        F.upper(F.col("titulaire_typeIdentifiant")).alias("titulaire_type_identifiant"),
        F.col("titulaire_nom").alias("titulaire_nom"),
        F.col("objet").alias("objet"),
        F.col("montant").alias("montant"),
        F.col("codeCPV").alias("code_cpv"),
        F.col("dateNotification").alias("date_notification"),
    )


def bronze_to_silver(spark: SparkSession, bronze_path: str, silver_table: str) -> None:
    raw_df = spark.read.parquet(bronze_path)
    clean_df = clean_decp_bronze(raw_df)
    clean_df.writeTo(silver_table).createOrReplace()
