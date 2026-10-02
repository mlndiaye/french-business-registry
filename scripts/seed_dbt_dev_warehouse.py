"""Seed a local Iceberg warehouse with known SIRENE gold data, for fast dbt test
verification against the `dev` target (see dbt/profiles.yml)."""

from __future__ import annotations

import argparse
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType

from registry.transform.silver_to_gold import ensure_gold_table

DEV_WAREHOUSE_DIR = ".dbt_dev_warehouse"
GOLD_TABLE = "lakehouse.gold.sirene_etablissements_historized"

GOLD_SCHEMA = StructType(
    [
        StructField("siren", StringType()),
        StructField("nic", StringType()),
        StructField("siret", StringType()),
        StructField("statut_diffusion", StringType()),
        StructField("date_creation", DateType()),
        StructField("etablissement_siege", BooleanType()),
        StructField("numero_voie", StringType()),
        StructField("type_voie", StringType()),
        StructField("libelle_voie", StringType()),
        StructField("code_postal", StringType()),
        StructField("libelle_commune", StringType()),
        StructField("activite_principale", StringType()),
        StructField("etat_administratif", StringType()),
        StructField("date_dernier_traitement", DateType()),
        StructField("valid_from", DateType()),
        StructField("valid_to", DateType()),
        StructField("is_current", BooleanType()),
    ]
)

GOOD_ROWS = [
    (
        "552032534",
        "00019",
        "55203253400019",
        "O",
        dt.date(1966, 1, 1),
        True,
        "8",
        "RUE",
        "DE LA PAIX",
        "75002",
        "PARIS",
        "70.10Z",
        "A",
        dt.date(2023, 5, 12),
        dt.date(2026, 10, 1),
        None,
        True,
    ),
    (
        "732829320",
        "00014",
        "73282932000014",
        "O",
        dt.date(1994, 3, 15),
        False,
        "12",
        "AV",
        "DES CHAMPS ELYSEES",
        "75008",
        "PARIS",
        "46.19B",
        "A",
        dt.date(2022, 11, 3),
        dt.date(2026, 10, 1),
        None,
        True,
    ),
]

# Each row below is a deliberate violation of exactly one of the three custom
# singular dbt tests (Tasks 4-6), isolated so each test's failure can be verified
# independently in Task 7.
VIOLATION_ROWS = [
    # Second is_current=true row for an existing siret -> violates
    # assert_exactly_one_current_version_per_siret.
    (
        "552032534",
        "00019",
        "55203253400019",
        "O",
        dt.date(1966, 1, 1),
        True,
        "99",
        "RUE",
        "DE LA PAIX",
        "75002",
        "PARIS",
        "70.10Z",
        "A",
        dt.date(2026, 9, 30),
        dt.date(2026, 10, 2),
        None,
        True,
    ),
    # Duplicate (siret, valid_from) for an existing siret -> violates
    # assert_unique_siret_valid_from.
    (
        "732829320",
        "00014",
        "73282932000014",
        "O",
        dt.date(1994, 3, 15),
        False,
        "12",
        "AV",
        "DES CHAMPS ELYSEES",
        "75008",
        "PARIS",
        "46.19B",
        "A",
        dt.date(2022, 11, 3),
        dt.date(2026, 10, 1),
        dt.date(2026, 10, 2),
        False,
    ),
    # is_current=true but valid_to is also set -> violates
    # assert_valid_to_matches_is_current.
    (
        "999999999",
        "00001",
        "99999999900001",
        "O",
        dt.date(2020, 1, 1),
        True,
        "1",
        "RUE",
        "DU TEST",
        "75001",
        "PARIS",
        "62.01Z",
        "A",
        dt.date(2026, 10, 1),
        dt.date(2026, 10, 1),
        dt.date(2026, 10, 5),
        True,
    ),
]


def seed(with_violations: bool) -> None:
    spark = (
        SparkSession.builder.appName("dbt-dev-seed")
        .master("local[1]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", DEV_WAREHOUSE_DIR)
        .getOrCreate()
    )

    spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")
    ensure_gold_table(spark, GOLD_TABLE)

    rows = list(GOOD_ROWS) + (VIOLATION_ROWS if with_violations else [])
    spark.createDataFrame(rows, schema=GOLD_SCHEMA).writeTo(GOLD_TABLE).append()

    print(f"Seeded {len(rows)} rows into {GOLD_TABLE} (violations={with_violations})")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-violations", action="store_true")
    args = parser.parse_args()
    seed(args.with_violations)
