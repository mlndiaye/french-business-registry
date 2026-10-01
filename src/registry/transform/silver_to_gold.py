"""Silver-to-gold SCD2 historization for the SIRENE establishment registry."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession


def ensure_gold_table(spark: SparkSession, gold_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            siren STRING,
            nic STRING,
            siret STRING,
            statut_diffusion STRING,
            date_creation DATE,
            etablissement_siege BOOLEAN,
            numero_voie STRING,
            type_voie STRING,
            libelle_voie STRING,
            code_postal STRING,
            libelle_commune STRING,
            activite_principale STRING,
            etat_administratif STRING,
            date_dernier_traitement DATE,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        ) USING iceberg
    """)


TRACKED_COLUMNS = [
    "nic",
    "statut_diffusion",
    "date_creation",
    "etablissement_siege",
    "numero_voie",
    "type_voie",
    "libelle_voie",
    "code_postal",
    "libelle_commune",
    "activite_principale",
    "etat_administratif",
    "date_dernier_traitement",
]


def apply_scd2_merge(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    spark.table(silver_table).createOrReplaceTempView("incoming")

    comparison = " AND ".join(f"incoming.{c} <=> gold.{c}" for c in TRACKED_COLUMNS)

    changed_sirets = spark.sql(f"""
        SELECT incoming.siret
        FROM incoming
        JOIN {gold_table} AS gold
          ON incoming.siret = gold.siret AND gold.is_current = true
        WHERE NOT ({comparison})
    """)
    changed_sirets.createOrReplaceTempView("changed_sirets")

    spark.sql(f"""
        MERGE INTO {gold_table} AS gold
        USING changed_sirets AS c
        ON gold.siret = c.siret AND gold.is_current = true
        WHEN MATCHED THEN UPDATE SET
            gold.valid_to = DATE('{run_date.isoformat()}'),
            gold.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT
            incoming.siren, incoming.nic, incoming.siret, incoming.statut_diffusion,
            incoming.date_creation, incoming.etablissement_siege, incoming.numero_voie,
            incoming.type_voie, incoming.libelle_voie, incoming.code_postal,
            incoming.libelle_commune, incoming.activite_principale,
            incoming.etat_administratif, incoming.date_dernier_traitement,
            DATE('{run_date.isoformat()}') AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            true AS is_current
        FROM incoming
        LEFT JOIN {gold_table} AS gold
          ON incoming.siret = gold.siret AND gold.is_current = true
        WHERE gold.siret IS NULL
    """)


def silver_to_gold(
    spark: SparkSession, silver_table: str, gold_table: str, run_date: dt.date
) -> None:
    ensure_gold_table(spark, gold_table)
    apply_scd2_merge(spark, silver_table, gold_table, run_date)
