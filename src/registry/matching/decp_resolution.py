"""Two-branch resolution of DECP titulaires: trust the source's typed SIRET
directly when present, otherwise unresolved. No fuzzy matching stage — see
docs/superpowers/specs/2026-10-08-decp-procurement-design.md for why."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolve_decp_titulaires(decp_df: DataFrame) -> DataFrame:
    is_siret = F.col("titulaire_type_identifiant") == "SIRET"
    return decp_df.select(
        F.col("uid"),
        F.when(is_siret, F.col("titulaire_id"))
        .otherwise(F.lit(None).cast("string"))
        .alias("siret_titulaire"),
        F.when(is_siret, F.lit("source_siret"))
        .otherwise(F.lit("unresolved"))
        .alias("match_method"),
        F.col("acheteur_id"),
        F.col("acheteur_nom"),
        F.col("montant"),
        F.col("objet"),
        F.col("code_cpv"),
    )


def validate_against_sirene(resolved_df: DataFrame, sirene_gold_df: DataFrame) -> DataFrame:
    known_sirets = sirene_gold_df.filter(F.col("is_current")).select(
        F.col("siret").alias("known_siret")
    )
    joined = resolved_df.join(
        known_sirets,
        resolved_df["siret_titulaire"] == known_sirets["known_siret"],
        "left",
    )
    return joined.withColumn(
        "siret_validated_in_sirene",
        F.when(F.col("siret_titulaire").isNotNull(), F.col("known_siret").isNotNull()),
    ).drop("known_siret")
