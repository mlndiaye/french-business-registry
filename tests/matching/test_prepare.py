from pyspark.sql.types import StringType, StructField, StructType

from registry.matching.prepare import prepare_bodacc_for_matching, prepare_sirene_for_matching

BODACC_NULL_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("commercant", StringType()),
        StructField("code_postal", StringType()),
        StructField("ville", StringType()),
    ]
)


def test_prepare_bodacc_for_matching_cleans_and_renames(spark_session):
    df = spark_session.createDataFrame(
        [("A1", "  DUPONT Batiment  ", "08000", "Charleville-Mezieres")],
        schema=["id", "commercant", "code_postal", "ville"],
    )

    row = prepare_bodacc_for_matching(df).collect()[0]

    assert row.uid == "A1"
    assert row.denomination_clean == "dupont batiment"
    assert row.adresse_clean == "08000 charleville-mezieres"


def test_prepare_bodacc_for_matching_handles_nulls(spark_session):
    df = spark_session.createDataFrame(
        [("A1", None, None, None)],
        schema=BODACC_NULL_SCHEMA,
    )

    row = prepare_bodacc_for_matching(df).collect()[0]

    assert row.denomination_clean == ""
    assert row.adresse_clean == ""


def test_prepare_sirene_for_matching_cleans_and_renames(spark_session):
    df = spark_session.createDataFrame(
        [("55203253400019", "DUPONT Batiment SARL", "08000", "Charleville-Mezieres")],
        schema=["siret", "denomination", "code_postal", "libelle_commune"],
    )

    row = prepare_sirene_for_matching(df).collect()[0]

    assert row.uid == "55203253400019"
    assert row.denomination_clean == "dupont batiment sarl"
    assert row.adresse_clean == "08000 charleville-mezieres"
