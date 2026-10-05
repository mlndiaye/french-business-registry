"""Deterministic SIREN-based matching between BODACC announcements and SIRENE
candidate siège establishments (see sirene_candidates.py)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def exact_siren_matches(bodacc_df: DataFrame, sirene_candidates_df: DataFrame) -> DataFrame:
    return (
        bodacc_df.filter(F.col("siren_declared").isNotNull())
        .join(
            sirene_candidates_df,
            bodacc_df["siren_declared"] == sirene_candidates_df["siren"],
            "inner",
        )
        .select(
            bodacc_df["id"].alias("bodacc_announcement_id"),
            bodacc_df["siren_declared"].alias("siren_bodacc"),
            sirene_candidates_df["siret"].alias("siret_siege"),
            F.lit("exact_siren").alias("match_method"),
            F.lit(1.0).alias("match_confidence"),
        )
    )


def unmatched_announcements(bodacc_df: DataFrame, exact_matches_df: DataFrame) -> DataFrame:
    matched_ids = exact_matches_df.select(F.col("bodacc_announcement_id").alias("matched_id"))
    return bodacc_df.join(matched_ids, bodacc_df["id"] == matched_ids["matched_id"], "left_anti")
