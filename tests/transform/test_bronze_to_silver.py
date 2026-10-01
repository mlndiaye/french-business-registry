from registry.transform.bronze_to_silver import clean_sirene_bronze

BRONZE_SCHEMA = [
    "siren",
    "nic",
    "siret",
    "statutDiffusionEtablissement",
    "dateCreationEtablissement",
    "etablissementSiege",
    "numeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "activitePrincipaleEtablissement",
    "etatAdministratifEtablissement",
    "dateDernierTraitementEtablissement",
]


def test_clean_sirene_bronze_types_and_renames_columns(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "552032534", "00019", "55203253400019", "O", "1966-01-01", "true",
                "8", "RUE", "DE LA PAIX", "75002", "PARIS", "70.10Z", "A", "2023-05-12",
            )
        ],
        schema=BRONZE_SCHEMA,
    )

    row = clean_sirene_bronze(raw_df).collect()[0]

    assert row.siret == "55203253400019"
    assert row.etablissement_siege is True
    assert str(row.date_creation) == "1966-01-01"
    assert row.libelle_commune == "PARIS"


def test_clean_sirene_bronze_drops_rows_with_missing_siret(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (
                "552032534", "00019", "55203253400019", "O", "1966-01-01", "true",
                "8", "RUE", "DE LA PAIX", "75002", "PARIS", "70.10Z", "A", "2023-05-12",
            ),
            (
                "732829320", "00014", "", "O", "1994-03-15", "false",
                "12", "AV", "DES CHAMPS ELYSEES", "75008", "PARIS", "46.19B", "A", "2022-11-03",
            ),
        ],
        schema=BRONZE_SCHEMA,
    )

    result = clean_sirene_bronze(raw_df).collect()

    assert [row.siret for row in result] == ["55203253400019"]
