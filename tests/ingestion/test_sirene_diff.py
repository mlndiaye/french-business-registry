import datetime as dt

import responses

from registry.ingestion.sirene_diff import (
    SIRENE_API_URL,
    fetch_sirene_updates,
    normalize_etablissement,
)


def _etablissement(siret: str, numero_voie: str) -> dict:
    return {
        "siren": siret[:9],
        "nic": siret[9:],
        "siret": siret,
        "statutDiffusionEtablissement": "O",
        "dateCreationEtablissement": "1966-01-01",
        "etablissementSiege": True,
        "dateDernierTraitementEtablissement": "2026-10-02",
        "adresseEtablissement": {
            "numeroVoieEtablissement": numero_voie,
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


@responses.activate
def test_fetch_sirene_updates_returns_normalized_records_for_single_page():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_etablissement("55203253400019", "99")],
        },
        status=200,
    )

    records = fetch_sirene_updates("test-api-key", dt.date(2026, 10, 1), dt.date(2026, 10, 2))

    assert len(records) == 1
    assert records[0]["siret"] == "55203253400019"
    assert records[0]["numeroVoieEtablissement"] == "99"
    assert responses.calls[0].request.headers["X-INSEE-Api-Key-Integration"] == "test-api-key"


@responses.activate
def test_fetch_sirene_updates_follows_pagination_cursor():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "*", "curseurSuivant": "PAGE2"},
            "etablissements": [_etablissement("55203253400019", "99")],
        },
        status=200,
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "PAGE2", "curseurSuivant": "PAGE2"},
            "etablissements": [_etablissement("73282932000014", "12")],
        },
        status=200,
    )

    records = fetch_sirene_updates("test-api-key", dt.date(2026, 10, 1), dt.date(2026, 10, 2))

    assert [r["siret"] for r in records] == ["55203253400019", "73282932000014"]
