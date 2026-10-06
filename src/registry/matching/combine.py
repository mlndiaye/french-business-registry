"""Combine deterministic (exact SIREN) and probabilistic (Splink) match results
into a single unified match-record shape: (bodacc_announcement_id, siret_siege,
match_method, match_confidence)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extract_fuzzy_match_candidates(predictions_df: DataFrame) -> DataFrame:
    return predictions_df.select(
        F.col("uid_l").alias("bodacc_announcement_id"),
        F.lit(None).cast("string").alias("siren_bodacc"),
        F.col("uid_r").alias("siret_siege"),
        F.lit("splink_fuzzy").alias("match_method"),
        F.col("match_probability").alias("match_confidence"),
    )


def best_fuzzy_match_per_announcement(fuzzy_matches_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("bodacc_announcement_id").orderBy(F.desc("match_confidence"))
    return (
        fuzzy_matches_df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


def combine_match_results(exact_matches_df: DataFrame, fuzzy_matches_df: DataFrame) -> DataFrame:
    best_fuzzy_df = best_fuzzy_match_per_announcement(fuzzy_matches_df)
    return exact_matches_df.unionByName(best_fuzzy_df)


def resolve_unresolved_matches(bodacc_df: DataFrame, combined_matches_df: DataFrame) -> DataFrame:
    unresolved_ids = bodacc_df.select(F.col("id").alias("bodacc_announcement_id")).join(
        combined_matches_df.select("bodacc_announcement_id"),
        on="bodacc_announcement_id",
        how="left_anti",
    )
    unresolved_df = unresolved_ids.select(
        F.col("bodacc_announcement_id"),
        F.lit(None).cast("string").alias("siren_bodacc"),
        F.lit(None).cast("string").alias("siret_siege"),
        F.lit("unresolved").alias("match_method"),
        F.lit(None).cast("double").alias("match_confidence"),
    )
    return combined_matches_df.unionByName(unresolved_df)
