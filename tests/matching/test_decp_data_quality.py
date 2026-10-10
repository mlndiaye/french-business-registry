from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from registry.matching.decp_data_quality import compute_data_quality_report

VALIDATED_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("siret_titulaire", StringType()),
        StructField("match_method", StringType()),
        StructField("siret_validated_in_sirene", BooleanType()),
        StructField("montant", DoubleType()),
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
