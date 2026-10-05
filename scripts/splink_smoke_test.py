"""One-off smoke test proving the Splink API this plan relies on actually works in
this environment, before any of the real matching modules are written on top of
it. Not part of the pytest suite — see Task 7's "Decisions made" note on why
Splink's training isn't unit-tested the normal way."""

from __future__ import annotations

import pandas as pd
import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.backends.duckdb import DuckDBAPI

bodacc_df = pd.DataFrame(
    [
        {
            "uid": "b1",
            "denomination_clean": "dupont batiment",
            "adresse_clean": "8 rue de la paix 75002",
        },
        {
            "uid": "b2",
            "denomination_clean": "martin travaux",
            "adresse_clean": "12 rue du test 69001",
        },
    ]
)
sirene_df = pd.DataFrame(
    [
        {
            "uid": "s1",
            "denomination_clean": "dupont batiment sarl",
            "adresse_clean": "8 rue de la paix 75002",
        },
        {
            "uid": "s2",
            "denomination_clean": "autre entreprise",
            "adresse_clean": "1 rue autre 75003",
        },
    ]
)

blocking_rule = block_on("substr(denomination_clean, 1, 3)")

settings = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="uid",
    comparisons=[
        cl.JaroWinklerAtThresholds("denomination_clean"),
        cl.LevenshteinAtThresholds("adresse_clean"),
    ],
    blocking_rules_to_generate_predictions=[blocking_rule],
    retain_intermediate_calculation_columns=False,
)

linker = Linker([bodacc_df, sirene_df], settings, db_api=DuckDBAPI())
linker.training.estimate_probability_two_random_records_match([blocking_rule], recall=0.7)
linker.training.estimate_u_using_random_sampling(max_pairs=1e4)
linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)
results = linker.inference.predict(threshold_match_probability=0.0)
print(results.as_pandas_dataframe()[["uid_l", "uid_r", "match_probability"]])
print("Smoke test completed without error.")
