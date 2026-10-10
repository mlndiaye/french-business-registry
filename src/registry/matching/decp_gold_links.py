"""SCD2 historization of resolved/validated DECP markets into
`gold.decp_marches_links`, following the exact same two-statement MERGE+INSERT
pattern as `gold_links.py` (BODACC) and `silver_to_gold.py` (SIRENE)."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession


def write_matches_to_silver(matches_df: DataFrame, silver_table: str) -> None:
    matches_df.writeTo(silver_table).createOrReplace()
