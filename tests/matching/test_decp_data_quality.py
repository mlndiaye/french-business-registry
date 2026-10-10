from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from registry.matching.decp_data_quality import (
    compute_data_quality_report,
    compute_unresolved_composition,
)

VALIDATED_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("siret_titulaire", StringType()),
        StructField("match_method", StringType()),
        StructField("siret_validated_in_sirene", BooleanType()),
        StructField("montant", DoubleType()),
    ]
)
DECP_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("titulaire_id", StringType()),
        StructField("titulaire_type_identifiant", StringType()),
        StructField("titulaire_nom", StringType()),
    ]
)


def test_compute_data_quality_report_counts_resolved_and_validated(spark_session):
    validated_df = spark_session.createDataFrame(
        [
            ("M1", "98236972000015", "source_siret", True, 510400.0),
            ("M2", "11111111100011", "source_siret", False, 2000.0),
            ("M3", None, "unresolved", None, 3000.0),
        ],
        schema=VALIDATED_SCHEMA_TYPED,
    )

    report = compute_data_quality_report(validated_df)

    assert report["total"] == 3
    assert report["resolved"] == 2
    assert report["unresolved"] == 1
    assert report["resolved_share"] == 2 / 3
    assert report["validated_in_sirene"] == 1
    assert report["validated_share"] == 1 / 2


def test_compute_data_quality_report_handles_empty_input(spark_session):
    validated_df = spark_session.createDataFrame([], schema=VALIDATED_SCHEMA_TYPED)

    report = compute_data_quality_report(validated_df)

    assert report["total"] == 0
    assert report["resolved"] == 0
    assert report["resolved_share"] == 0.0
    assert report["validated_share"] == 0.0


def test_compute_unresolved_composition_breaks_down_by_identifiant_type(spark_session):
    validated_df = spark_session.createDataFrame(
        [
            ("M1", "98236972000015", "source_siret", True, 510400.0),
            ("M2", None, "unresolved", None, 2000.0),
            ("M3", None, "unresolved", None, 3000.0),
        ],
        schema=VALIDATED_SCHEMA_TYPED,
    )
    decp_df = spark_session.createDataFrame(
        [
            ("M1", "98236972000015", "SIRET", "PRIMEURS CHAMPARDENNAIS"),
            ("M2", "DE119375450", "TVA", None),
            ("M3", None, None, None),
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    result = (
        compute_unresolved_composition(validated_df, decp_df)
        .orderBy("titulaire_type_identifiant")
        .collect()
    )

    assert len(result) == 2
    assert result[0].titulaire_type_identifiant is None
    assert result[0]["count"] == 1
    assert result[0].null_nom_count == 1
    assert result[1].titulaire_type_identifiant == "TVA"
    assert result[1]["count"] == 1
    assert result[1].null_nom_count == 1


def test_compute_unresolved_composition_excludes_resolved_sibling_on_shared_uid(spark_session):
    # A real case found in department-08 data: a joint award (groupement) splits
    # one market uid across two titulaires — one resolved, one not. The resolved
    # sibling must not leak into the unresolved composition just because it
    # shares the same uid.
    validated_df = spark_session.createDataFrame(
        [
            ("M1", "68628001700035", "source_siret", False, 510400.0),
            ("M1", None, "unresolved", None, 510400.0),
        ],
        schema=VALIDATED_SCHEMA_TYPED,
    )
    decp_df = spark_session.createDataFrame(
        [
            ("M1", "68628001700035", "SIRET", "GABELLA S.A."),
            ("M1", "999999999", "TVA", None),
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    result = compute_unresolved_composition(validated_df, decp_df).collect()

    assert len(result) == 1
    assert result[0].titulaire_type_identifiant == "TVA"
    assert result[0]["count"] == 1
