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
