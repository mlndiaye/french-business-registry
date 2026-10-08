"""FastAPI service exposing the SIRENE establishment registry."""

from __future__ import annotations

import datetime as dt

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from registry.api.repository import (
    AnnoncesLegalesRepository,
    EtablissementRepository,
    PostgresAnnoncesLegalesRepository,
    PostgresEtablissementRepository,
    build_dsn,
)

app = FastAPI(title="French Business Registry API")


class Etablissement(BaseModel):
    siren: str
    nic: str
    siret: str
    statut_diffusion: str | None = None
    date_creation: dt.date | None = None
    etablissement_siege: bool | None = None
    numero_voie: str | None = None
    type_voie: str | None = None
    libelle_voie: str | None = None
    code_postal: str | None = None
    libelle_commune: str | None = None
    activite_principale: str | None = None
    etat_administratif: str | None = None
    date_dernier_traitement: dt.date | None = None
    valid_from: dt.date
    valid_to: dt.date | None = None
    is_current: bool


class AnnonceLegale(BaseModel):
    siret_siege: str
    bodacc_announcement_id: str
    siren_bodacc: str | None = None
    match_method: str
    match_confidence: float | None = None
    date_parution: dt.date | None = None
    type_avis: str | None = None
    famille_avis: str | None = None
    tribunal: str | None = None
    commercant: str | None = None
    denomination: str | None = None
    ville: str | None = None
    code_postal: str | None = None
    valid_from: dt.date
    valid_to: dt.date | None = None
    is_current: bool


def get_repository() -> EtablissementRepository:
    return PostgresEtablissementRepository(build_dsn())


def get_annonces_legales_repository() -> AnnoncesLegalesRepository:
    return PostgresAnnoncesLegalesRepository(build_dsn())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/etablissements/search", response_model=list[Etablissement])
def search_etablissements(
    q: str, repo: EtablissementRepository = Depends(get_repository)
) -> list[dict]:
    return repo.search(q)


@app.get("/etablissements/{siret}/history", response_model=list[Etablissement])
def get_history(siret: str, repo: EtablissementRepository = Depends(get_repository)) -> list[dict]:
    rows = repo.history(siret)
    if not rows:
        raise HTTPException(status_code=404, detail="Establishment not found")
    return rows


@app.get("/etablissements/{siret}/annonces-legales", response_model=list[AnnonceLegale])
def get_annonces_legales(
    siret: str,
    repo: AnnoncesLegalesRepository = Depends(get_annonces_legales_repository),
) -> list[dict]:
    return repo.get_by_siret(siret)
