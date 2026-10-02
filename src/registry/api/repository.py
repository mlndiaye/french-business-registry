"""Data access for the SIRENE establishment registry serving store."""

from __future__ import annotations

import os
from typing import Protocol

import psycopg2
import psycopg2.extras

POSTGRES_TABLE = "sirene_etablissements_historized"


class EtablissementRepository(Protocol):
    def search(self, q: str) -> list[dict]: ...
    def history(self, siret: str) -> list[dict]: ...


class PostgresEtablissementRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def search(self, q: str) -> list[dict]:
        pattern = f"%{q}%"
        query = f"""
            SELECT * FROM {POSTGRES_TABLE}
            WHERE is_current = true
              AND (siret = %(q)s OR siren = %(q)s OR libelle_commune ILIKE %(pattern)s)
            ORDER BY siret
            LIMIT 50
        """
        return self._fetch(query, {"q": q, "pattern": pattern})

    def history(self, siret: str) -> list[dict]:
        query = f"""
            SELECT * FROM {POSTGRES_TABLE}
            WHERE siret = %(siret)s
            ORDER BY valid_from
        """
        return self._fetch(query, {"siret": siret})

    def _fetch(self, query: str, params: dict) -> list[dict]:
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]


def build_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_SERVING_DB', 'registry')} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )
