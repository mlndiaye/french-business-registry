"""Smoke test proving `run_matching_cascade` wires exact + fuzzy (Splink on the
Spark backend) + unresolved correctly end-to-end, without needing BODACC/SIRENE
API credentials — a plain local Spark session and small hand-built DataFrames are
enough. Not part of the pytest suite, same reasoning as splink_matching.py."""

from __future__ import annotations

import tempfile

from pyspark.sql import SparkSession
from splink import SparkAPI
from splink.backends.spark import similarity_jar_location

from registry.matching.cascade import run_matching_cascade

spark = (
    SparkSession.builder.master("local[*]")
    .appName("cascade-smoke-test")
    .config("spark.jars", similarity_jar_location())
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir(tempfile.mkdtemp())

bodacc_df = spark.createDataFrame(
    [
        ("B1", "552032534", "DUPONT BATIMENT", "75002", "PARIS"),
        ("B2", None, "MARTIN TRAVAUX", "69001", "LYON"),
        ("B3", None, "ENTREPRISE INCONNUE", "13001", "MARSEILLE"),
    ],
    schema=["id", "siren_declared", "commercant", "code_postal", "ville"],
)
sirene_candidates_df = spark.createDataFrame(
    [
        ("552032534", "55203253400019", "DUPONT BATIMENT SARL", "75002", "PARIS"),
        ("445566778", "44556677800012", "MARTIN TRAVAUX SARL", "69001", "LYON"),
    ],
    schema=["siren", "siret", "denomination", "code_postal", "libelle_commune"],
)

db_api = SparkAPI(spark_session=spark)
result = run_matching_cascade(bodacc_df, sirene_candidates_df, db_api)
result.orderBy("bodacc_announcement_id").show(truncate=False)
spark.stop()
print("Cascade smoke test completed without error.")
