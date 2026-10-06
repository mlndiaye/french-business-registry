import datetime as dt

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from registry.matching.gold_links import (
    apply_links_scd2_merge,
    ensure_gold_links_table,
    write_matches_to_silver,
)

MATCH_SCHEMA = [
    "bodacc_announcement_id",
    "siren_bodacc",
    "siret_siege",
    "match_method",
    "match_confidence",
]
MATCH_SCHEMA_TYPED = StructType(
    [
        StructField("bodacc_announcement_id", StringType()),
        StructField("siren_bodacc", StringType()),
        StructField("siret_siege", StringType()),
        StructField("match_method", StringType()),
        StructField("match_confidence", DoubleType()),
    ]
)


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


def test_apply_links_scd2_merge_inserts_new_links(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    ensure_gold_links_table(spark_session, gold_table)

    matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )
    write_matches_to_silver(matches_df, silver_table)

    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 6))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].bodacc_announcement_id == "A1"
    assert rows[0].siret_siege == "55203253400019"
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 6)
    assert rows[0].valid_to is None


def test_apply_links_scd2_merge_versions_changed_match(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    ensure_gold_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", None, "11111111100019", "splink_fuzzy", 0.75)], schema=MATCH_SCHEMA_TYPED
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 6))

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 7))

    rows = spark_session.table(gold_table).orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].match_method == "splink_fuzzy"
    assert rows[0].is_current is False
    assert rows[0].valid_to == dt.date(2026, 10, 7)
    assert rows[1].match_method == "exact_siren"
    assert rows[1].siret_siege == "55203253400019"
    assert rows[1].is_current is True
    assert rows[1].valid_to is None


def test_apply_links_scd2_merge_ignores_confidence_only_drift(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.bodacc_sirene_links_{table_suffix}"
    silver_table = f"lakehouse.silver.bodacc_sirene_links_{table_suffix}"
    ensure_gold_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", None, "11111111100019", "splink_fuzzy", 0.650001)], schema=MATCH_SCHEMA_TYPED
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 6))

    write_matches_to_silver(
        spark_session.createDataFrame(
            [("A1", None, "11111111100019", "splink_fuzzy", 0.650002)], schema=MATCH_SCHEMA_TYPED
        ),
        silver_table,
    )
    apply_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 7))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 6)
    assert rows[0].match_confidence == 0.650001
