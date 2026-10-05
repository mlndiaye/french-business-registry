"""Prepare BODACC and SIRENE candidate data for Splink fuzzy matching: both sides
reduce to the same two comparison fields, (uid, denomination_clean, adresse_clean),
at the same granularity (postal code + commune name — the only address detail
both sides have in common)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_bodacc_for_matching(bodacc_df: DataFrame) -> DataFrame:
    return bodacc_df.select(
        F.col("id").alias("uid"),
        F.lower(F.trim(F.coalesce(F.col("commercant"), F.lit("")))).alias("denomination_clean"),
        F.lower(
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("code_postal"), F.lit("")),
                    F.coalesce(F.col("ville"), F.lit("")),
                )
            )
        ).alias("adresse_clean"),
    )


def prepare_sirene_for_matching(sirene_candidates_df: DataFrame) -> DataFrame:
    return sirene_candidates_df.select(
        F.col("siret").alias("uid"),
        F.lower(F.trim(F.coalesce(F.col("denomination"), F.lit("")))).alias("denomination_clean"),
        F.lower(
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("code_postal"), F.lit("")),
                    F.coalesce(F.col("libelle_commune"), F.lit("")),
                )
            )
        ).alias("adresse_clean"),
    )
