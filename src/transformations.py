"""TP3 — Module de nettoyage des clients (customers.csv).

Toutes les fonctions sont pures : DataFrame -> DataFrame, sans lecture,
écriture ni affichage. Elles sont conçues pour être chaînées via
``DataFrame.transform`` dans ``nettoyer_clients``.
"""
import unicodedata

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

SCHEMA_CLIENTS = StructType([
    StructField("customer_id", StringType(), False),
    StructField("prenom", StringType(), True),
    StructField("nom", StringType(), True),
    StructField("email", StringType(), True),
    StructField("telephone", StringType(), True),
    StructField("adresse", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("region", StringType(), True),
    StructField("date_naissance", StringType(), True),
    StructField("date_inscription", StringType(), True),
])


def lire_clients(spark, path: str) -> DataFrame:
    """Charge customers.csv avec le schéma explicite ci-dessus."""
    return spark.read.option("header", True).schema(SCHEMA_CLIENTS).csv(path)


def unifier_manquants(df: DataFrame) -> DataFrame:
    """Emails "" ou "N/A" -> null."""
    email_propre = F.when(
        F.trim(F.col("email")).isin("", "N/A"), None
    ).otherwise(F.col("email"))
    return df.withColumn("email", email_propre)


def normaliser_email(df: DataFrame) -> DataFrame:
    """Email en minuscules + trim ; ajoute email_valide (format simple)."""
    motif = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    email_norm = F.trim(F.lower(F.col("email")))
    return (df
            .withColumn("email", email_norm)
            .withColumn("email_valide",
                        email_norm.isNotNull() & email_norm.rlike(motif)))


def _sans_accent(s):
    if s is None:
        return None
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


_sans_accent_udf = F.udf(_sans_accent, StringType())


def normaliser_ville(df: DataFrame) -> DataFrame:
    """trim + initcap sur ville (affichage) ; ville_norm = clé sans accent."""
    ville_affichage = F.initcap(F.trim(F.col("ville")))
    return (df
            .withColumn("ville", ville_affichage)
            .withColumn("ville_norm", _sans_accent_udf(ville_affichage)))


def normaliser_telephone(df: DataFrame) -> DataFrame:
    """Ne garde que les chiffres, retire l'indicatif +221 ; ajoute telephone_valide
    (9 chiffres, préfixe 70/75/76/77/78)."""
    chiffres = F.regexp_replace(F.col("telephone"), r"[^0-9]", "")
    chiffres = F.regexp_replace(chiffres, r"^221", "")
    return (df
            .withColumn("telephone", chiffres)
            .withColumn("telephone_valide",
                        chiffres.rlike(r"^(70|75|76|77|78)\d{7}$")))


def valider_naissance(df: DataFrame) -> DataFrame:
    """Date plausible entre 1920-01-01 et aujourd'hui, sinon null."""
    date_naissance = F.to_date(F.col("date_naissance"), "yyyy-MM-dd")
    bornes_ok = (date_naissance >= F.lit("1920-01-01").cast("date")) & \
                (date_naissance <= F.current_date())
    return df.withColumn(
        "date_naissance",
        F.when(bornes_ok, date_naissance).otherwise(F.lit(None).cast("date")),
    )


def dedupliquer_clients(df: DataFrame) -> DataFrame:
    """Retire les doublons exacts, puis les quasi-doublons partageant le même
    email (déjà normalisé à ce stade du pipeline). Les clients sans email
    retombent sur customer_id pour ne pas être fusionnés entre eux."""
    sans_doublons_exacts = df.dropDuplicates()
    cle = F.coalesce(F.col("email"), F.col("customer_id"))
    return (sans_doublons_exacts
            .withColumn("_cle_dedup", cle)
            .dropDuplicates(["_cle_dedup"])
            .drop("_cle_dedup"))


def nettoyer_clients(df: DataFrame) -> DataFrame:
    """Pipeline complet de nettoyage des clients."""
    return (df
            .transform(unifier_manquants)
            .transform(normaliser_email)
            .transform(normaliser_ville)
            .transform(normaliser_telephone)
            .transform(valider_naissance)
            .transform(dedupliquer_clients)
            )
