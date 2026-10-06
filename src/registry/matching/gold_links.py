"""SCD2 historization of BODACC/SIRENE match results into `gold.bodacc_sirene_links`,
following the exact same two-statement MERGE+INSERT pattern as Step 1's
`silver_to_gold.py`."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession


def write_matches_to_silver(matches_df: DataFrame, silver_table: str) -> None:
    matches_df.writeTo(silver_table).createOrReplace()
