from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from registry.matching.combine import (
    best_fuzzy_match_per_announcement,
    combine_match_results,
    extract_fuzzy_match_candidates,
    resolve_unresolved_matches,
)

PREDICTIONS_SCHEMA = ["uid_l", "uid_r", "match_probability"]
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


def test_extract_fuzzy_match_candidates_renames_columns(spark_session):
    df = spark_session.createDataFrame([("A1", "S1", 0.87)], schema=PREDICTIONS_SCHEMA)

    row = extract_fuzzy_match_candidates(df).collect()[0]

    assert row.bodacc_announcement_id == "A1"
    assert row.siret_siege == "S1"
    assert row.match_method == "splink_fuzzy"
    assert row.match_confidence == 0.87


def test_best_fuzzy_match_per_announcement_keeps_highest_confidence(spark_session):
    df = spark_session.createDataFrame(
        [
            ("A1", "S1", "splink_fuzzy", 0.6),
            ("A1", "S2", "splink_fuzzy", 0.9),
            ("A2", "S3", "splink_fuzzy", 0.7),
        ],
        schema=["bodacc_announcement_id", "siret_siege", "match_method", "match_confidence"],
    )

    result = best_fuzzy_match_per_announcement(df).orderBy("bodacc_announcement_id").collect()

    assert len(result) == 2
    assert result[0].siret_siege == "S2"
    assert result[0].match_confidence == 0.9
    assert result[1].siret_siege == "S3"


def test_combine_match_results_unions_exact_and_fuzzy(spark_session):
    exact_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )
    fuzzy_df = spark_session.createDataFrame(
        [("A2", None, "73282932000014", "splink_fuzzy", 0.8)], schema=MATCH_SCHEMA_TYPED
    )

    result = combine_match_results(exact_df, fuzzy_df).orderBy("bodacc_announcement_id").collect()

    assert [row.bodacc_announcement_id for row in result] == ["A1", "A2"]
    assert [row.match_method for row in result] == ["exact_siren", "splink_fuzzy"]


def test_resolve_unresolved_matches_adds_rows_for_unmatched_announcements(spark_session):
    bodacc_df = spark_session.createDataFrame([("A1",), ("A2",), ("A3",)], schema=["id"])
    combined_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    result = (
        resolve_unresolved_matches(bodacc_df, combined_df)
        .orderBy("bodacc_announcement_id")
        .collect()
    )

    assert [row.bodacc_announcement_id for row in result] == ["A1", "A2", "A3"]
    assert result[0].match_method == "exact_siren"
    assert result[1].match_method == "unresolved"
    assert result[1].siret_siege is None
    assert result[1].match_confidence is None
    assert result[2].match_method == "unresolved"
