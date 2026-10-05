from pyspark.sql.types import StringType, StructField, StructType

from registry.matching.exact_siren import exact_siren_matches, unmatched_announcements

BODACC_SCHEMA = ["id", "siren_declared"]
CANDIDATES_SCHEMA = ["siren", "siret"]
BODACC_SCHEMA_TYPED = StructType(
    [StructField("id", StringType()), StructField("siren_declared", StringType())]
)


def test_exact_siren_matches_joins_on_declared_siren(spark_session):
    bodacc_df = spark_session.createDataFrame(
        [("A1", "552032534"), ("A2", "999999999")], schema=BODACC_SCHEMA
    )
    candidates_df = spark_session.createDataFrame(
        [("552032534", "55203253400019")], schema=CANDIDATES_SCHEMA
    )

    result = exact_siren_matches(bodacc_df, candidates_df).collect()

    assert len(result) == 1
    assert result[0].bodacc_announcement_id == "A1"
    assert result[0].siret_siege == "55203253400019"
    assert result[0].match_method == "exact_siren"
    assert result[0].match_confidence == 1.0


def test_exact_siren_matches_ignores_null_siren_declared(spark_session):
    bodacc_df = spark_session.createDataFrame([("A1", None)], schema=BODACC_SCHEMA_TYPED)
    candidates_df = spark_session.createDataFrame(
        [("552032534", "55203253400019")], schema=CANDIDATES_SCHEMA
    )

    result = exact_siren_matches(bodacc_df, candidates_df).collect()

    assert result == []


def test_unmatched_announcements_excludes_matched_ids(spark_session):
    bodacc_df = spark_session.createDataFrame(
        [("A1", "552032534"), ("A2", "999999999")], schema=BODACC_SCHEMA
    )
    exact_matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)],
        schema=[
            "bodacc_announcement_id",
            "siren_bodacc",
            "siret_siege",
            "match_method",
            "match_confidence",
        ],
    )

    result = unmatched_announcements(bodacc_df, exact_matches_df).collect()

    assert [row.id for row in result] == ["A2"]
