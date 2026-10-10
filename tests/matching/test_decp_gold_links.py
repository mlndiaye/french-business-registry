import datetime as dt

from registry.matching.decp_gold_links import (
    apply_decp_links_scd2_merge,
    ensure_gold_decp_links_table,
    historize_decp_links,
    write_matches_to_silver,
)

MATCH_SCHEMA = [
    "uid",
    "siret_titulaire",
    "match_method",
    "siret_validated_in_sirene",
    "acheteur_id",
    "acheteur_nom",
    "montant",
    "objet",
    "code_cpv",
]


def test_write_matches_to_silver_creates_table(spark_session, table_suffix):
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "98236972000015",
                "source_siret",
                False,
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                510400.0,
                "LOT No1",
                "15300000",
            )
        ],
        schema=MATCH_SCHEMA,
    )

    write_matches_to_silver(matches_df, silver_table)

    rows = spark_session.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0].siret_titulaire == "98236972000015"


def test_ensure_gold_decp_links_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"

    ensure_gold_decp_links_table(spark_session, gold_table)
    ensure_gold_decp_links_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "uid" in columns
    assert "siret_validated_in_sirene" in columns
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns


def test_apply_decp_links_scd2_merge_inserts_new_links(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    ensure_gold_decp_links_table(spark_session, gold_table)

    matches_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "98236972000015",
                "source_siret",
                False,
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                510400.0,
                "LOT No1",
                "15300000",
            )
        ],
        schema=MATCH_SCHEMA,
    )
    write_matches_to_silver(matches_df, silver_table)

    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 10))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].uid == "M1"
    assert rows[0].siret_titulaire == "98236972000015"
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 10)
    assert rows[0].valid_to is None


def test_apply_decp_links_scd2_merge_versions_changed_amount(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    ensure_gold_decp_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    False,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    510400.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 10))

    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    False,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    600000.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 11))

    rows = spark_session.table(gold_table).orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].montant == 510400.0
    assert rows[0].is_current is False
    assert rows[0].valid_to == dt.date(2026, 10, 11)
    assert rows[1].montant == 600000.0
    assert rows[1].is_current is True
    assert rows[1].valid_to is None


def test_apply_decp_links_scd2_merge_ignores_validation_flag_only_drift(
    spark_session, table_suffix
):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    ensure_gold_decp_links_table(spark_session, gold_table)

    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    False,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    510400.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 10))

    # Same market, same siret/method/amount — only siret_validated_in_sirene flips
    # (e.g. a later real SIRENE bootstrap now finds this SIRET). Must NOT version.
    write_matches_to_silver(
        spark_session.createDataFrame(
            [
                (
                    "M1",
                    "98236972000015",
                    "source_siret",
                    True,
                    "21080096700015",
                    "COMMUNE DE CHARLEVILLE-MEZIERES",
                    510400.0,
                    "LOT No1",
                    "15300000",
                )
            ],
            schema=MATCH_SCHEMA,
        ),
        silver_table,
    )
    apply_decp_links_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 11))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 10)
    assert rows[0].siret_validated_in_sirene is False


def test_historize_decp_links_creates_table_and_merges(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.decp_marches_links_{table_suffix}"
    silver_table = f"lakehouse.silver.decp_marches_links_{table_suffix}"
    matches_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "98236972000015",
                "source_siret",
                False,
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                510400.0,
                "LOT No1",
                "15300000",
            )
        ],
        schema=MATCH_SCHEMA,
    )

    historize_decp_links(spark_session, matches_df, silver_table, gold_table, dt.date(2026, 10, 10))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
