from pyspark.sql.types import StringType, StructField, StructType

from registry.transform.bodacc_to_silver import bronze_to_silver, clean_bodacc_bronze

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


def test_bronze_to_silver_writes_cleaned_iceberg_table(spark_session, tmp_path, table_suffix):
    bronze_df = spark_session.createDataFrame(
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
    bronze_path = str(tmp_path / "bronze_bodacc.parquet")
    bronze_df.write.parquet(bronze_path)
    silver_table = f"lakehouse.silver.bodacc_{table_suffix}"

    bronze_to_silver(spark_session, bronze_path, silver_table)

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
    assert result[0].id == "BX202500012345"


def test_bronze_to_silver_accumulates_across_runs(spark_session, tmp_path, table_suffix):
    first_bronze_df = spark_session.createDataFrame(
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
    first_bronze_path = str(tmp_path / "bronze_bodacc_1.parquet")
    first_bronze_df.write.parquet(first_bronze_path)
    silver_table = f"lakehouse.silver.bodacc_{table_suffix}"
    bronze_to_silver(spark_session, first_bronze_path, silver_table)

    second_bronze_df = spark_session.createDataFrame(
        [
            (
                "BX202500099999",
                "2025-10-16",
                "99999",
                "Jugement",
                "Procedures collectives",
                "Tribunal de commerce de Lyon",
                "MARTIN TRAVAUX SARL",
                "445566778",
                "LYON",
                "69001",
                None,
            )
        ],
        schema=BRONZE_SCHEMA,
    )
    second_bronze_path = str(tmp_path / "bronze_bodacc_2.parquet")
    second_bronze_df.write.parquet(second_bronze_path)
    bronze_to_silver(spark_session, second_bronze_path, silver_table)

    result = spark_session.table(silver_table).orderBy("id").collect()
    assert [row.id for row in result] == ["BX202500012345", "BX202500099999"]


def test_bronze_to_silver_does_not_duplicate_already_seen_announcements(
    spark_session, tmp_path, table_suffix
):
    bronze_df = spark_session.createDataFrame(
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
    bronze_path = str(tmp_path / "bronze_bodacc.parquet")
    bronze_df.write.parquet(bronze_path)
    silver_table = f"lakehouse.silver.bodacc_{table_suffix}"

    bronze_to_silver(spark_session, bronze_path, silver_table)
    bronze_to_silver(spark_session, bronze_path, silver_table)

    result = spark_session.table(silver_table).collect()
    assert len(result) == 1
