"""Bronze-to-silver cleaning and typing of the SIRENE establishment stock."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_sirene_bronze(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("siret").isNotNull()) & (F.col("siret") != ""))
        .dropDuplicates(["siret"])
        .select(
            F.col("siren").alias("siren"),
            F.col("nic").alias("nic"),
            F.col("siret").alias("siret"),
            F.col("statutDiffusionEtablissement").alias("statut_diffusion"),
            F.to_date("dateCreationEtablissement", "yyyy-MM-dd").alias("date_creation"),
            (F.col("etablissementSiege") == "true").alias("etablissement_siege"),
            F.col("numeroVoieEtablissement").alias("numero_voie"),
            F.col("typeVoieEtablissement").alias("type_voie"),
            F.col("libelleVoieEtablissement").alias("libelle_voie"),
            F.col("codePostalEtablissement").alias("code_postal"),
            F.col("libelleCommuneEtablissement").alias("libelle_commune"),
            F.col("activitePrincipaleEtablissement").alias("activite_principale"),
            F.col("etatAdministratifEtablissement").alias("etat_administratif"),
            F.to_date("dateDernierTraitementEtablissement", "yyyy-MM-dd").alias(
                "date_dernier_traitement"
            ),
        )
    )
