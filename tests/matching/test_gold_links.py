from registry.matching.gold_links import ensure_gold_links_table, write_matches_to_silver

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


def test_ensure_gold_links_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"

    ensure_gold_links_table(spark_session, gold_table)
    ensure_gold_links_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "bodacc_announcement_id" in columns
    assert "match_confidence" in columns
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns
