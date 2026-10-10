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
