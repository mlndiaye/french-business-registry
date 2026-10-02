from registry.ingestion.sirene_diff import normalize_etablissement


def test_normalize_etablissement_flattens_nested_api_response():
    raw = {
        "siren": "552032534",
        "nic": "00019",
        "siret": "55203253400019",
        "statutDiffusionEtablissement": "O",
        "dateCreationEtablissement": "1966-01-01",
        "etablissementSiege": True,
        "dateDernierTraitementEtablissement": "2026-10-02",
        "adresseEtablissement": {
            "numeroVoieEtablissement": "99",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA PAIX",
            "codePostalEtablissement": "75002",
            "libelleCommuneEtablissement": "PARIS",
        },
        "periodesEtablissement": [
            {
                "dateFin": None,
                "dateDebut": "2026-10-02",
                "etatAdministratifEtablissement": "A",
                "activitePrincipaleEtablissement": "70.10Z",
            }
        ],
    }

    result = normalize_etablissement(raw)

    assert result == {
        "siren": "552032534",
        "nic": "00019",
        "siret": "55203253400019",
        "statutDiffusionEtablissement": "O",
        "dateCreationEtablissement": "1966-01-01",
        "etablissementSiege": "true",
        "numeroVoieEtablissement": "99",
        "typeVoieEtablissement": "RUE",
        "libelleVoieEtablissement": "DE LA PAIX",
        "codePostalEtablissement": "75002",
        "libelleCommuneEtablissement": "PARIS",
        "activitePrincipaleEtablissement": "70.10Z",
        "etatAdministratifEtablissement": "A",
        "dateDernierTraitementEtablissement": "2026-10-02",
    }
