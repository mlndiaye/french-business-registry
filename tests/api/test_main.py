import datetime as dt

from fastapi.testclient import TestClient

from registry.api.main import app, get_repository

SAMPLE_ROW = {
    "siren": "552032534",
    "nic": "00019",
    "siret": "55203253400019",
    "statut_diffusion": "O",
    "date_creation": dt.date(1966, 1, 1),
    "etablissement_siege": True,
    "numero_voie": "8",
    "type_voie": "RUE",
    "libelle_voie": "DE LA PAIX",
    "code_postal": "75002",
    "libelle_commune": "PARIS",
    "activite_principale": "70.10Z",
    "etat_administratif": "A",
    "date_dernier_traitement": dt.date(2023, 5, 12),
    "valid_from": dt.date(2026, 10, 1),
    "valid_to": None,
    "is_current": True,
}


class FakeRepository:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def search(self, q: str) -> list[dict]:
        return [r for r in self._rows if r["siret"] == q or r["siren"] == q]

    def history(self, siret: str) -> list[dict]:
        return [r for r in self._rows if r["siret"] == siret]


def test_health_returns_ok():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_search_returns_matching_establishment():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/search", params={"q": "55203253400019"})

    assert response.status_code == 200
    assert response.json()[0]["siret"] == "55203253400019"
    app.dependency_overrides.clear()


def test_search_returns_empty_list_for_no_match():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/search", params={"q": "00000000000000"})

    assert response.status_code == 200
    assert response.json() == []
    app.dependency_overrides.clear()


def test_history_returns_404_for_unknown_siret():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/00000000000000/history")

    assert response.status_code == 404
    app.dependency_overrides.clear()


def test_history_returns_versions_for_known_siret():
    app.dependency_overrides[get_repository] = lambda: FakeRepository([SAMPLE_ROW])
    client = TestClient(app)

    response = client.get("/etablissements/55203253400019/history")

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["siret"] == "55203253400019"
    app.dependency_overrides.clear()
