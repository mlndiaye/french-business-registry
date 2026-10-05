from registry.matching.sirene_candidates import normalize_candidate


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
