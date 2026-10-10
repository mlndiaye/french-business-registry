"""Data-quality coverage report for DECP entity resolution — the equivalent of
BODACC's blind-holdout evaluation, adapted to the fact that there's no
probabilistic prediction to score here (see
docs/superpowers/specs/2026-10-08-decp-procurement-design.md)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_data_quality_report(validated_df: DataFrame) -> dict:
    total = validated_df.count()
    resolved = validated_df.filter(F.col("match_method") == "source_siret").count()
    unresolved = total - resolved
    validated_in_sirene = validated_df.filter(
        F.col("siret_validated_in_sirene") == True  # noqa: E712
    ).count()

    return {
        "total": total,
        "resolved": resolved,
        "unresolved": unresolved,
        "resolved_share": resolved / total if total else 0.0,
        "validated_in_sirene": validated_in_sirene,
        "validated_share": validated_in_sirene / resolved if resolved else 0.0,
    }


def compute_unresolved_composition(validated_df: DataFrame, decp_df: DataFrame) -> DataFrame:
    # Excluding resolved (uid, titulaire_id) pairs, rather than including rows
    # whose uid merely appears somewhere in the unresolved set, matters because a
    # single uid can carry multiple titulaires (a joint "groupement" award) —
    # some resolved, some not. Matching on uid alone would let a resolved
    # sibling leak into this unresolved breakdown.
    resolved_pairs = validated_df.filter(F.col("match_method") == "source_siret").select(
        F.col("uid"), F.col("siret_titulaire").alias("titulaire_id")
    )
    unresolved_rows = decp_df.join(resolved_pairs, on=["uid", "titulaire_id"], how="left_anti")
    return unresolved_rows.groupBy("titulaire_type_identifiant").agg(
        F.count("*").alias("count"),
        F.sum(F.when(F.col("titulaire_nom").isNull(), 1).otherwise(0)).alias("null_nom_count"),
    )
