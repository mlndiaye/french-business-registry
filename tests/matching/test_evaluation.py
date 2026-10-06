from pyspark.sql.types import StringType, StructField, StructType

from registry.matching.evaluation import build_evaluation_set, compute_precision_recall

FUZZY_PREDICTED_SCHEMA = StructType(
    [
        StructField("bodacc_announcement_id", StringType()),
        StructField("siret_siege", StringType()),
    ]
)

MATCH_SCHEMA = [
    "bodacc_announcement_id",
    "siren_bodacc",
    "siret_siege",
    "match_method",
    "match_confidence",
]


def test_build_evaluation_set_keeps_only_exact_matched_announcements(spark_session):
    bodacc_df = spark_session.createDataFrame(
        [("A1", "ACME SARL"), ("A2", "OTHER SARL")], schema=["id", "commercant"]
    )
    exact_matches_df = spark_session.createDataFrame(
        [("A1", "552032534", "55203253400019", "exact_siren", 1.0)], schema=MATCH_SCHEMA
    )

    result = build_evaluation_set(bodacc_df, exact_matches_df).collect()

    assert len(result) == 1
    assert result[0].id == "A1"
    assert result[0].true_siret_siege == "55203253400019"
    assert result[0].commercant == "ACME SARL"


def test_compute_precision_recall_counts_correct_and_incorrect_matches(spark_session):
    evaluation_df = spark_session.createDataFrame(
        [("A1", "SIRET1"), ("A2", "SIRET2"), ("A3", "SIRET3")],
        schema=["id", "true_siret_siege"],
    )
    fuzzy_predicted_df = spark_session.createDataFrame(
        [("A1", "SIRET1"), ("A2", "WRONG_SIRET")],
        schema=["bodacc_announcement_id", "siret_siege"],
    )

    result = compute_precision_recall(evaluation_df, fuzzy_predicted_df)

    assert result["total"] == 3
    assert result["predicted"] == 2
    assert result["correct"] == 1
    assert result["precision"] == 0.5
    assert result["recall"] == 1 / 3


def test_compute_precision_recall_handles_no_predictions(spark_session):
    evaluation_df = spark_session.createDataFrame(
        [("A1", "SIRET1")], schema=["id", "true_siret_siege"]
    )
    fuzzy_predicted_df = spark_session.createDataFrame([], schema=FUZZY_PREDICTED_SCHEMA)

    result = compute_precision_recall(evaluation_df, fuzzy_predicted_df)

    assert result["total"] == 1
    assert result["predicted"] == 0
    assert result["correct"] == 0
    assert result["precision"] == 0.0
    assert result["recall"] == 0.0
