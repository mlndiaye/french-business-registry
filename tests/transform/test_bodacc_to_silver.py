from pyspark.sql.types import StringType, StructField, StructType

from registry.transform.bodacc_to_silver import clean_bodacc_bronze

BRONZE_COLUMN_NAMES = [
    "id",
    "dateparution",
    "numeroannonce",
    "typeavis_lib",
    "familleavis_lib",
    "tribunal",
    "commercant",
    "siren_declared",
    "ville",
    "cp",
    "denomination",
]

BRONZE_SCHEMA = StructType([StructField(name, StringType()) for name in BRONZE_COLUMN_NAMES])


def test_clean_bodacc_bronze_types_and_renames_columns(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "BX202500012345",
                "2025-10-15",
                "12345",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Paris",
                "DUPONT BATIMENT SARL",
                "334393806",
                "PARIS",
                "75002",
                None,
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_bodacc_bronze(raw_df).collect()[0]

    assert row.id == "BX202500012345"
    assert str(row.date_parution) == "2025-10-15"
    assert row.siren_declared == "334393806"
    assert row.famille_avis == "Procedures collectives"


def test_clean_bodacc_bronze_drops_rows_with_missing_id(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "BX202500012345",
                "2025-10-15",
                "12345",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Paris",
                "DUPONT BATIMENT SARL",
                "334393806",
                "PARIS",
                "75002",
                None,
            ),
            (
                "",
                "2025-10-16",
                "12346",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Lyon",
                "MARTIN TRAVAUX SARL",
                None,
                "LYON",
                "69001",
                None,
            ),
        ],
        schema=BRONZE_SCHEMA,
    )

    result = clean_bodacc_bronze(raw_df).collect()

    assert [row.id for row in result] == ["BX202500012345"]
