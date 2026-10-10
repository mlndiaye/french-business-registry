from registry.matching.decp_gold_links import write_matches_to_silver

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
