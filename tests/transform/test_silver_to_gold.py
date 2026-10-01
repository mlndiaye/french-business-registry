import datetime as dt

from registry.transform.silver_to_gold import apply_scd2_merge, ensure_gold_table, silver_to_gold

SILVER_SCHEMA = [
    "siren",
    "nic",
    "siret",
    "statut_diffusion",
    "date_creation",
    "etablissement_siege",
    "numero_voie",
    "type_voie",
    "libelle_voie",
    "code_postal",
    "libelle_commune",
    "activite_principale",
    "etat_administratif",
    "date_dernier_traitement",
]


def test_ensure_gold_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"

    ensure_gold_table(spark_session, gold_table)
    ensure_gold_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns


def test_apply_scd2_merge_inserts_new_establishments(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    ensure_gold_table(spark_session, gold_table)

    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2023, 5, 12),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()

    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].siret == "55203253400019"
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 1)
    assert rows[0].valid_to is None


def test_apply_scd2_merge_closes_and_versions_changed_establishment(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    ensure_gold_table(spark_session, gold_table)

    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2023, 5, 12),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "10",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2026, 9, 30),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 2))

    rows = spark_session.table(gold_table).orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].numero_voie == "8"
    assert rows[0].is_current is False
    assert rows[0].valid_to == dt.date(2026, 10, 2)
    assert rows[1].numero_voie == "10"
    assert rows[1].is_current is True
    assert rows[1].valid_to is None


def test_apply_scd2_merge_does_not_version_unchanged_establishment(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    ensure_gold_table(spark_session, gold_table)
    row = (
        "552032534",
        "00019",
        "55203253400019",
        "O",
        dt.date(1966, 1, 1),
        True,
        "8",
        "RUE",
        "DE LA PAIX",
        "75002",
        "PARIS",
        "70.10Z",
        "A",
        dt.date(2023, 5, 12),
    )

    spark_session.createDataFrame([row], schema=SILVER_SCHEMA).writeTo(
        silver_table
    ).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    spark_session.createDataFrame([row], schema=SILVER_SCHEMA).writeTo(
        silver_table
    ).createOrReplace()
    apply_scd2_merge(spark_session, silver_table, gold_table, dt.date(2026, 10, 2))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
    assert rows[0].is_current is True
    assert rows[0].valid_from == dt.date(2026, 10, 1)


def test_silver_to_gold_creates_table_and_merges(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"
    silver_table = f"lakehouse.silver.sirene_{table_suffix}"
    spark_session.createDataFrame(
        [
            (
                "552032534",
                "00019",
                "55203253400019",
                "O",
                dt.date(1966, 1, 1),
                True,
                "8",
                "RUE",
                "DE LA PAIX",
                "75002",
                "PARIS",
                "70.10Z",
                "A",
                dt.date(2023, 5, 12),
            )
        ],
        schema=SILVER_SCHEMA,
    ).writeTo(silver_table).createOrReplace()

    silver_to_gold(spark_session, silver_table, gold_table, dt.date(2026, 10, 1))

    rows = spark_session.table(gold_table).collect()
    assert len(rows) == 1
