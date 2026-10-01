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
