from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from registry.matching.decp_resolution import resolve_decp_titulaires, validate_against_sirene

DECP_SCHEMA = [
    "uid",
    "acheteur_id",
    "acheteur_nom",
    "titulaire_id",
    "titulaire_type_identifiant",
    "titulaire_nom",
    "objet",
    "montant",
    "code_cpv",
]
DECP_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("acheteur_id", StringType()),
        StructField("acheteur_nom", StringType()),
        StructField("titulaire_id", StringType()),
        StructField("titulaire_type_identifiant", StringType()),
        StructField("titulaire_nom", StringType()),
        StructField("objet", StringType()),
        StructField("montant", DoubleType()),
        StructField("code_cpv", StringType()),
    ]
)


def test_resolve_decp_titulaires_trusts_source_siret(spark_session):
    df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                "98236972000015",
                "SIRET",
                "PRIMEURS CHAMPARDENNAIS",
                "LOT No1",
                510400.0,
                "15300000",
            )
        ],
        schema=DECP_SCHEMA,
    )

    row = resolve_decp_titulaires(df).collect()[0]

    assert row.titulaire_id == "98236972000015"
    assert row.siret_titulaire == "98236972000015"
    assert row.match_method == "source_siret"


def test_resolve_decp_titulaires_keeps_both_rows_of_a_joint_award(spark_session):
    # A real department-08 case: one market uid awarded jointly to two
    # titulaires, one resolved and one not. uid alone can't distinguish them —
    # titulaire_id must be carried through so downstream code has a real key.
    df = spark_session.createDataFrame(
        [
            (
                "M1",
                "21080372200417",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                "68628001700035",
                "SIRET",
                "GABELLA S.A.",
                "LOT No1",
                510400.0,
                "45110000",
            ),
            (
                "M1",
                "21080372200417",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                "999999999",
                "TVA",
                None,
                "LOT No1",
                510400.0,
                "45110000",
            ),
        ],
        schema=DECP_SCHEMA,
    )

    rows = resolve_decp_titulaires(df).orderBy("titulaire_id").collect()

    assert len(rows) == 2
    assert rows[0].titulaire_id == "68628001700035"
    assert rows[0].siret_titulaire == "68628001700035"
    assert rows[0].match_method == "source_siret"
    assert rows[1].titulaire_id == "999999999"
    assert rows[1].siret_titulaire is None
    assert rows[1].match_method == "unresolved"


def test_resolve_decp_titulaires_marks_non_siret_as_unresolved(spark_session):
    df = spark_session.createDataFrame(
        [
            (
                "M2",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                "DE119375450",
                "TVA",
                None,
                "LOT No2",
                2000.0,
                "15300000",
            )
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    row = resolve_decp_titulaires(df).collect()[0]

    assert row.siret_titulaire is None
    assert row.match_method == "unresolved"


RESOLVED_SCHEMA_TYPED = StructType(
    [
        StructField("uid", StringType()),
        StructField("siret_titulaire", StringType()),
        StructField("match_method", StringType()),
    ]
)
SIRENE_GOLD_SCHEMA = ["siret", "is_current"]


def test_validate_against_sirene_marks_known_siret_true(spark_session):
    resolved_df = spark_session.createDataFrame(
        [("M1", "98236972000015", "source_siret")], schema=RESOLVED_SCHEMA_TYPED
    )
    sirene_gold_df = spark_session.createDataFrame(
        [("98236972000015", True)], schema=SIRENE_GOLD_SCHEMA
    )

    row = validate_against_sirene(resolved_df, sirene_gold_df).collect()[0]

    assert row.siret_validated_in_sirene is True


def test_validate_against_sirene_marks_unknown_siret_false(spark_session):
    resolved_df = spark_session.createDataFrame(
        [("M1", "98236972000015", "source_siret")], schema=RESOLVED_SCHEMA_TYPED
    )
    sirene_gold_df = spark_session.createDataFrame(
        [("11111111100011", True)], schema=SIRENE_GOLD_SCHEMA
    )

    row = validate_against_sirene(resolved_df, sirene_gold_df).collect()[0]

    assert row.siret_validated_in_sirene is False


def test_validate_against_sirene_leaves_unresolved_rows_null(spark_session):
    resolved_df = spark_session.createDataFrame(
        [("M2", None, "unresolved")], schema=RESOLVED_SCHEMA_TYPED
    )
    sirene_gold_df = spark_session.createDataFrame(
        [("98236972000015", True)], schema=SIRENE_GOLD_SCHEMA
    )

    row = validate_against_sirene(resolved_df, sirene_gold_df).collect()[0]

    assert row.siret_validated_in_sirene is None


def test_resolve_decp_titulaires_marks_missing_identifiant_as_unresolved(spark_session):
    df = spark_session.createDataFrame(
        [
            (
                "M3",
                "21080096700015",
                "COMMUNE DE CHARLEVILLE-MEZIERES",
                None,
                None,
                None,
                "LOT No3",
                3000.0,
                "15300000",
            )
        ],
        schema=DECP_SCHEMA_TYPED,
    )

    row = resolve_decp_titulaires(df).collect()[0]

    assert row.siret_titulaire is None
    assert row.match_method == "unresolved"
