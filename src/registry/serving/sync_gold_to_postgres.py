"""Sync the historized gold table into Postgres for low-latency API serving."""

from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession

from registry.transform.spark_session import lakehouse_spark_configs

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

GOLD_TABLE = "lakehouse.gold.sirene_etablissements_historized"
POSTGRES_TABLE = "sirene_etablissements_historized"


def build_sync_session(app_name: str = "registry-sync") -> SparkSession:
    configs = lakehouse_spark_configs()
    configs["spark.jars.packages"] = f"{configs['spark.jars.packages']},{POSTGRES_JDBC_PACKAGE}"
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def build_jdbc_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_SERVING_DB", "registry")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def build_psycopg2_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_SERVING_DB', 'registry')} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )


def ensure_postgres_table(dsn: str) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
                siren TEXT,
                nic TEXT,
                siret TEXT,
                statut_diffusion TEXT,
                date_creation DATE,
                etablissement_siege BOOLEAN,
                numero_voie TEXT,
                type_voie TEXT,
                libelle_voie TEXT,
                code_postal TEXT,
                libelle_commune TEXT,
                activite_principale TEXT,
                etat_administratif TEXT,
                date_dernier_traitement DATE,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{POSTGRES_TABLE}_siret ON {POSTGRES_TABLE} (siret)"
        )
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{POSTGRES_TABLE}_siret_valid_from "
            f"ON {POSTGRES_TABLE} (siret, valid_from)"
        )
        conn.commit()


def sync_gold_to_postgres(
    spark: SparkSession, jdbc_url: str, pg_user: str, pg_password: str
) -> int:
    df = spark.table(GOLD_TABLE)
    df.write.option("truncate", "true").jdbc(
        url=jdbc_url,
        table=POSTGRES_TABLE,
        mode="overwrite",
        properties={
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        },
    )
    return df.count()


def run_sync(spark: SparkSession) -> int:
    ensure_postgres_table(build_psycopg2_dsn())
    return sync_gold_to_postgres(
        spark, build_jdbc_url(), os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"]
    )
