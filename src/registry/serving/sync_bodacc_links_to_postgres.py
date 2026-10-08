"""Sync the joined BODACC links + announcement content into Postgres for the
/annonces-legales endpoint. Reuses the generic session/connection helpers from
sync_gold_to_postgres.py — only the query and target table are specific here."""

from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession

from registry.serving.sync_gold_to_postgres import build_jdbc_url, build_psycopg2_dsn

BODACC_LINKS_POSTGRES_TABLE = "bodacc_annonces_legales"


def ensure_bodacc_links_postgres_table(dsn: str) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {BODACC_LINKS_POSTGRES_TABLE} (
                siret_siege TEXT,
                bodacc_announcement_id TEXT,
                siren_bodacc TEXT,
                match_method TEXT,
                match_confidence DOUBLE PRECISION,
                date_parution DATE,
                type_avis TEXT,
                famille_avis TEXT,
                tribunal TEXT,
                commercant TEXT,
                denomination TEXT,
                ville TEXT,
                code_postal TEXT,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{BODACC_LINKS_POSTGRES_TABLE}_siret_siege "
            f"ON {BODACC_LINKS_POSTGRES_TABLE} (siret_siege)"
        )
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS "
            f"uq_{BODACC_LINKS_POSTGRES_TABLE}_announcement_valid_from "
            f"ON {BODACC_LINKS_POSTGRES_TABLE} (bodacc_announcement_id, valid_from)"
        )
        conn.commit()


def build_joined_links_df(spark: SparkSession):
    return spark.sql("""
        SELECT
            links.siret_siege,
            links.bodacc_announcement_id,
            links.siren_bodacc,
            links.match_method,
            links.match_confidence,
            a.date_parution,
            a.type_avis,
            a.famille_avis,
            a.tribunal,
            a.commercant,
            a.denomination,
            a.ville,
            a.code_postal,
            links.valid_from,
            links.valid_to,
            links.is_current
        FROM lakehouse.gold.bodacc_sirene_links AS links
        JOIN lakehouse.silver.bodacc_annonces AS a
          ON links.bodacc_announcement_id = a.id
        WHERE links.siret_siege IS NOT NULL
    """)


def sync_bodacc_links_to_postgres(
    spark: SparkSession, jdbc_url: str, pg_user: str, pg_password: str
) -> int:
    df = build_joined_links_df(spark)
    df.write.option("truncate", "true").jdbc(
        url=jdbc_url,
        table=BODACC_LINKS_POSTGRES_TABLE,
        mode="overwrite",
        properties={
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        },
    )
    return df.count()


def run_sync(spark: SparkSession) -> int:
    ensure_bodacc_links_postgres_table(build_psycopg2_dsn())
    return sync_bodacc_links_to_postgres(
        spark, build_jdbc_url(), os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"]
    )
