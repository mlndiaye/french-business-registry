import datetime as dt
import io

import boto3
import pyarrow.parquet as pq
import responses
from moto import mock_aws

from registry.ingestion.sirene_diff import (
    SIRENE_API_URL,
    diff_object_key,
    fetch_sirene_updates,
    normalize_etablissement,
    run_daily_diff,
    write_diff_to_bronze,
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


def test_diff_object_key_formats_ingestion_date():
    key = diff_object_key(dt.date(2026, 10, 2))

    assert key == "bronze/sirene/diff/ingestion_date=2026-10-02/diff.parquet"


@mock_aws
def test_write_diff_to_bronze_uploads_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.ingestion.sirene_diff.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    records = [normalize_etablissement(_etablissement("55203253400019", "99"))]

    key = write_diff_to_bronze(records, bucket="lakehouse", work_dir=tmp_path)

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
    assert table.column("siret").to_pylist() == ["55203253400019"]


@responses.activate
@mock_aws
def test_run_daily_diff_fetches_and_uploads_to_bronze(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "registry.ingestion.sirene_diff.get_s3_client",
        lambda: boto3.client("s3", region_name="us-east-1"),
    )
    responses.add(
        responses.GET,
        SIRENE_API_URL,
        json={
            "header": {"statut": 200, "curseur": "*", "curseurSuivant": "*"},
            "etablissements": [_etablissement("55203253400019", "99")],
        },
        status=200,
    )

    key = run_daily_diff(
        "test-api-key", "lakehouse", dt.date(2026, 10, 1), dt.date(2026, 10, 2), tmp_path
    )

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket="lakehouse", Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
