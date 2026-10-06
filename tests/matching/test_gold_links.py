from registry.matching.gold_links import write_matches_to_silver

MATCH_SCHEMA = [
    "bodacc_announcement_id",
    "siren_bodacc",
    "siret_siege",
    "match_method",
    "match_confidence",
]


def test_write_matches_to_silver_creates_table(spark_session, table_suffix):
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    write_matches_to_silver(matches_df, silver_table)

    rows = spark_session.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0].siret_siege == "55203253400019"
