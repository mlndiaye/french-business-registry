from registry.matching.decp_gold_links import ensure_gold_decp_links_table, write_matches_to_silver

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
