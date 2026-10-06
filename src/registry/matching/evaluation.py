"""Blind-holdout evaluation of the Splink fuzzy stage: reuse exact-SIREN matches as
trusted ground truth, since `prepare_bodacc_for_matching` never reads the declared
SIREN in the first place (see docs/superpowers/specs/
2026-10-03-bodacc-entity-resolution-design.md, 'Evaluation methodology')."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_evaluation_set(bodacc_df: DataFrame, exact_matches_df: DataFrame) -> DataFrame:
    ground_truth = exact_matches_df.select(
        F.col("bodacc_announcement_id").alias("id"),
        F.col("siret_siege").alias("true_siret_siege"),
    )
    return bodacc_df.join(ground_truth, on="id", how="inner")
