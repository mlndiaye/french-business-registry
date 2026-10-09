import datetime as dt

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from registry.transform.decp_to_silver import bronze_to_silver, clean_decp_bronze

BRONZE_SCHEMA = StructType(
    [
        StructField("uid", StringType()),
        StructField("acheteur_id", StringType()),
        StructField("acheteur_nom", StringType()),
        StructField("titulaire_id", StringType()),
        StructField("titulaire_typeIdentifiant", StringType()),
        StructField("titulaire_nom", StringType()),
        StructField("objet", StringType()),
        StructField("montant", DoubleType()),
        StructField("codeCPV", StringType()),
        StructField("dateNotification", DateType()),
        StructField("acheteur_departement_code", StringType()),
        StructField("datePublicationDonnees", DateType()),
        StructField("donneesActuelles", BooleanType()),
    ]
)


def test_clean_decp_bronze_types_and_renames_columns(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1 : FRUITS ET LEGUMES",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_decp_bronze(raw_df).collect()[0]

    assert row.uid == "M1"
    assert row.titulaire_id == "98236972000015"
    assert row.titulaire_type_identifiant == "SIRET"
    assert row.montant == 510400.0
    assert row.code_cpv == "15300000"
    assert row.date_notification == dt.date(2026, 5, 1)


def test_clean_decp_bronze_drops_rows_with_missing_uid(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            ),
            (
                "",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                None,
                None,
                None,
                "LOT No2",
                1000.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            ),
        ],
        schema=BRONZE_SCHEMA,
    )

    result = clean_decp_bronze(raw_df).collect()

    assert [row.uid for row in result] == ["M1"]


def test_clean_decp_bronze_normalizes_identifiant_case(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "Siret",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_decp_bronze(raw_df).collect()[0]

    assert row.titulaire_type_identifiant == "SIRET"


def test_bronze_to_silver_writes_cleaned_iceberg_table(spark_session, tmp_path, table_suffix):
    bronze_df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES (MAIRIE)",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
                dt.date(2026, 5, 1),
                "08",
                dt.date(2026, 5, 2),
                True,
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    bronze_path = str(tmp_path / "bronze_decp.parquet")
    bronze_df.write.parquet(bronze_path)
    silver_table = f"lakehouse.silver.decp_{table_suffix}"

    bronze_to_silver(spark_session, bronze_path, silver_table)

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
    assert result[0].uid == "M1"
