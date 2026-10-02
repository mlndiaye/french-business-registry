"""Daily differential ingestion from the Sirene API into the bronze layer."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file

SIRENE_API_URL = "https://api.insee.fr/api-sirene/3.11/siret"

BRONZE_COLUMNS = [
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


def normalize_etablissement(raw: dict) -> dict:
    adresse = raw.get("adresseEtablissement") or {}
    periodes = raw.get("periodesEtablissement") or [{}]
    periode_courante = periodes[0]
    return {
        "siren": raw.get("siren"),
        "nic": raw.get("nic"),
        "siret": raw.get("siret"),
        "statutDiffusionEtablissement": raw.get("statutDiffusionEtablissement"),
        "dateCreationEtablissement": raw.get("dateCreationEtablissement"),
        "etablissementSiege": "true" if raw.get("etablissementSiege") else "false",
        "numeroVoieEtablissement": adresse.get("numeroVoieEtablissement"),
        "typeVoieEtablissement": adresse.get("typeVoieEtablissement"),
        "libelleVoieEtablissement": adresse.get("libelleVoieEtablissement"),
        "codePostalEtablissement": adresse.get("codePostalEtablissement"),
        "libelleCommuneEtablissement": adresse.get("libelleCommuneEtablissement"),
        "activitePrincipaleEtablissement": periode_courante.get("activitePrincipaleEtablissement"),
        "etatAdministratifEtablissement": periode_courante.get("etatAdministratifEtablissement"),
        "dateDernierTraitementEtablissement": raw.get("dateDernierTraitementEtablissement"),
    }


def fetch_sirene_updates(api_key: str, since: dt.date, until: dt.date) -> list[dict]:
    records: list[dict] = []
    curseur = "*"
    headers = {"X-INSEE-Api-Key-Integration": api_key}
    query = f"dateDernierTraitementEtablissement:[{since.isoformat()} TO {until.isoformat()}]"

    while True:
        response = requests.get(
            SIRENE_API_URL,
            headers=headers,
            params={"q": query, "nombre": 1000, "curseur": curseur},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        records.extend(normalize_etablissement(e) for e in payload["etablissements"])

        next_curseur = payload["header"]["curseurSuivant"]
        if next_curseur == curseur:
            break
        curseur = next_curseur

    return records


def diff_object_key(ingestion_date: dt.date) -> str:
    return f"bronze/sirene/diff/ingestion_date={ingestion_date.isoformat()}/diff.parquet"


def write_diff_to_bronze(records: list[dict], bucket: str, work_dir: Path) -> str:
    table = pa.Table.from_pylist(
        records, schema=pa.schema([(name, pa.string()) for name in BRONZE_COLUMNS])
    )
    parquet_path = work_dir / "diff.parquet"
    pq.write_table(table, parquet_path)

    client = get_s3_client()
    key = diff_object_key(dt.date.today())
    upload_file(client, parquet_path, bucket, key)
    return key


def run_daily_diff(
    api_key: str, bucket: str, since: dt.date, until: dt.date, work_dir: Path
) -> str:
    records = fetch_sirene_updates(api_key, since, until)
    return write_diff_to_bronze(records, bucket, work_dir)
