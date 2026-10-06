from registry.matching.evaluation import build_evaluation_set

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
