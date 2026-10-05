import responses

from registry.matching.sirene_candidates import (
    SIRENE_API_URL,
    fetch_sirene_candidates,
    normalize_candidate,
)


def _raw_candidate(siret: str, denomination: str) -> dict:
    return {
        "siren": siret[:9],
        "siret": siret,
        "adresseEtablissement": {
            "numeroVoieEtablissement": "5",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA REPUBLIQUE",
            "codePostalEtablissement": "08000",
            "libelleCommuneEtablissement": "CHARLEVILLE-MEZIERES",
        },
        "uniteLegale": {
            "denominationUniteLegale": denomination,
            "nomUniteLegale": None,
            "prenomUsuelUniteLegale": None,
        },
    }


def test_normalize_candidate_uses_denomination_when_present():
    raw = {
        "siren": "123456789",
        "siret": "12345678900019",
        "adresseEtablissement": {
            "numeroVoieEtablissement": "5",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DE LA REPUBLIQUE",
            "codePostalEtablissement": "08000",
            "libelleCommuneEtablissement": "CHARLEVILLE-MEZIERES",
        },
        "uniteLegale": {
            "denominationUniteLegale": "DUPONT BATIMENT",
            "nomUniteLegale": None,
            "prenomUsuelUniteLegale": None,
        },
    }

    result = normalize_candidate(raw)

    assert result["siret"] == "12345678900019"
    assert result["denomination"] == "DUPONT BATIMENT"
    assert result["code_postal"] == "08000"
    assert result["libelle_commune"] == "CHARLEVILLE-MEZIERES"


def test_normalize_candidate_falls_back_to_nom_prenom_for_individuals():
    raw = {
        "siren": "987654321",
        "siret": "98765432100019",
        "adresseEtablissement": {
            "numeroVoieEtablissement": "2",
            "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DU TEST",
            "codePostalEtablissement": "08200",
            "libelleCommuneEtablissement": "SEDAN",
        },
        "uniteLegale": {
            "denominationUniteLegale": None,
            "nomUniteLegale": "MARTIN",
            "prenomUsuelUniteLegale": "Julien",
        },
    }

    result = normalize_candidate(raw)

    assert result["denomination"] == "MARTIN Julien"


@responses.activate
def test_fetch_sirene_candidates_queries_department_and_siege_filter():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_raw_candidate("12345678900019", "DUPONT BATIMENT")],
        },
        status=200,
    )

    records = fetch_sirene_candidates("test-api-key", "08")

    assert len(records) == 1
    request_url = responses.calls[0].request.url
    assert "codePostalEtablissement%3A08%2A" in request_url
    assert "etablissementSiege%3Atrue" in request_url


@responses.activate
def test_fetch_sirene_candidates_follows_pagination_cursor():
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "*", "curseurSuivant": "PAGE2"},
            "etablissements": [_raw_candidate("12345678900019", "DUPONT BATIMENT")],
        },
        status=200,
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"curseur": "PAGE2", "curseurSuivant": "PAGE2"},
            "etablissements": [_raw_candidate("98765432100019", "MARTIN SARL")],
        },
        status=200,
    )

    records = fetch_sirene_candidates("test-api-key", "08")

    assert [r["siret"] for r in records] == ["12345678900019", "98765432100019"]
