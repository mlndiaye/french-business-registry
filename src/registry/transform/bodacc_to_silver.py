"""Bronze-to-silver cleaning and typing of BODACC legal announcements."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_bodacc_bronze(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("id").isNotNull()) & (F.col("id") != ""))
        .dropDuplicates(["id"])
        .select(
            F.col("id").alias("id"),
            F.to_date("dateparution", "yyyy-MM-dd").alias("date_parution"),
            F.col("numeroannonce").alias("numero_annonce"),
            F.col("typeavis_lib").alias("type_avis"),
            F.col("familleavis_lib").alias("famille_avis"),
            F.col("tribunal").alias("tribunal"),
            F.col("commercant").alias("commercant"),
            F.col("denomination").alias("denomination"),
            F.col("siren_declared").alias("siren_declared"),
            F.col("ville").alias("ville"),
            F.col("cp").alias("code_postal"),
        )
    )
