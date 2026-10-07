"""Wire the full BODACC/SIRENE matching cascade (exact -> fuzzy -> unresolved) into
a single callable, for use by the daily orchestration DAG. No pytest: it drives a
real Splink Linker end-to-end, same reasoning as splink_matching.py — verified
manually (scripts/cascade_smoke_test.py), not mocked."""

from __future__ import annotations

from pyspark.sql import DataFrame

from registry.matching.combine import (
    best_fuzzy_match_per_announcement,
    combine_match_results,
    extract_fuzzy_match_candidates,
    resolve_unresolved_matches,
)
from registry.matching.exact_siren import exact_siren_matches, unmatched_announcements
from registry.matching.prepare import prepare_bodacc_for_matching, prepare_sirene_for_matching
from registry.matching.splink_matching import build_linker, predict_fuzzy_matches, train_linker


def run_matching_cascade(
    bodacc_df: DataFrame, sirene_candidates_df: DataFrame, db_api
) -> DataFrame:
    exact_matches = exact_siren_matches(bodacc_df, sirene_candidates_df)
    remaining = unmatched_announcements(bodacc_df, exact_matches)

    bodacc_prepared = prepare_bodacc_for_matching(remaining)
    sirene_prepared = prepare_sirene_for_matching(sirene_candidates_df)

    linker = build_linker(bodacc_prepared, sirene_prepared, db_api)
    train_linker(linker)
    predictions_df = predict_fuzzy_matches(linker).as_spark_dataframe()

    fuzzy_candidates = extract_fuzzy_match_candidates(predictions_df)
    best_fuzzy = best_fuzzy_match_per_announcement(fuzzy_candidates)

    combined = combine_match_results(exact_matches, best_fuzzy)
    return resolve_unresolved_matches(bodacc_df, combined)
