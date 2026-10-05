"""Fetch current siège establishments with their legal denomination, scoped to a
department, as SIRENE-side matching candidates for entity resolution against
BODACC (see docs/superpowers/specs/2026-10-03-bodacc-entity-resolution-design.md)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from registry.ingestion.storage import get_s3_client, upload_file

SIRENE_API_URL = "https://api.insee.fr/api-sirene/3.11/siret"

CANDIDATE_COLUMNS = [
    "siren",
    "siret",
    "denomination",
    "numero_voie",
    "type_voie",
    "libelle_voie",
    "code_postal",
    "libelle_commune",
]


def normalize_candidate(raw: dict) -> dict:
    adresse = raw.get("adresseEtablissement") or {}
    unite_legale = raw.get("uniteLegale") or {}
    denomination = unite_legale.get("denominationUniteLegale")
    if not denomination:
        nom = unite_legale.get("nomUniteLegale")
        prenom = unite_legale.get("prenomUsuelUniteLegale")
        if nom:
            denomination = f"{nom} {prenom}".strip() if prenom else nom
    return {
        "siren": raw.get("siren"),
        "siret": raw.get("siret"),
        "denomination": denomination,
        "numero_voie": adresse.get("numeroVoieEtablissement"),
        "type_voie": adresse.get("typeVoieEtablissement"),
        "libelle_voie": adresse.get("libelleVoieEtablissement"),
        "code_postal": adresse.get("codePostalEtablissement"),
        "libelle_commune": adresse.get("libelleCommuneEtablissement"),
    }
