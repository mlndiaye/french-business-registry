"""Splink-based fuzzy matching between BODACC announcements and SIRENE candidate
siège establishments. Same matching rules run on either backend — pass a
DuckDBAPI() for fast local dev/test, or a SparkAPI(spark_session=...) for the
real pipeline against the lakehouse (see docs/superpowers/specs/
2026-10-03-bodacc-entity-resolution-design.md)."""

from __future__ import annotations

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

BLOCKING_RULE = block_on("substr(denomination_clean, 1, 4)")

SPLINK_SETTINGS = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="uid",
    comparisons=[
        cl.JaroWinklerAtThresholds("denomination_clean"),
        cl.LevenshteinAtThresholds("adresse_clean"),
    ],
    blocking_rules_to_generate_predictions=[BLOCKING_RULE],
    retain_intermediate_calculation_columns=False,
)


def build_linker(bodacc_prepared, sirene_prepared, db_api):
    return Linker([bodacc_prepared, sirene_prepared], SPLINK_SETTINGS, db_api=db_api)


def train_linker(linker) -> None:
    linker.training.estimate_probability_two_random_records_match([BLOCKING_RULE], recall=0.7)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.training.estimate_parameters_using_expectation_maximisation(BLOCKING_RULE)


def predict_fuzzy_matches(linker, threshold: float = 0.5):
    return linker.inference.predict(threshold_match_probability=threshold)
