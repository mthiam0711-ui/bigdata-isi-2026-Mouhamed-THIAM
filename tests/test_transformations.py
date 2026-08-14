from chispa import assert_df_equality
from pyspark.sql.types import StringType, StructField, StructType

from src.transformations import (
    dedupliquer_clients,
    nettoyer_clients,
    normaliser_email,
    normaliser_telephone,
    normaliser_ville,
    unifier_manquants,
    valider_naissance,
)


def test_unifier_manquants(spark):
    entree = spark.createDataFrame(
        [("C1", "N/A"), ("C2", ""), ("C3", "ok@ucad.edu.sn")],
        ["customer_id", "email"])
    res = unifier_manquants(entree)
    emails = [r["email"] for r in res.select("email").collect()]
    assert emails.count(None) == 2
    assert "ok@ucad.edu.sn" in emails


def test_normaliser_email(spark):
    entree = spark.createDataFrame(
        [("C1", "  Jean.DUPONT@Gmail.com  "), ("C2", "pas-un-email")],
        ["customer_id", "email"])
    res = normaliser_email(entree).orderBy("customer_id").collect()
    assert res[0]["email"] == "jean.dupont@gmail.com"
    assert res[0]["email_valide"] is True
    assert res[1]["email_valide"] is False


def test_normaliser_ville(spark):
    # Cas fourni par le sujet : trim + initcap sur la colonne ville.
    entree = spark.createDataFrame(
        [("C1", " DAKAR "), ("C2", "thies")],
        ["customer_id", "ville"])
    res = normaliser_ville(entree)
    villes = [r["ville"] for r in res.select("ville").collect()]
    assert set(villes) == {"Dakar", "Thies"}


def test_normaliser_ville_meme_cle_sans_accent(spark):
    # Cas piège : "Thiès" accentué et "THIES" doivent donner la même clé.
    entree = spark.createDataFrame(
        [("C1", "Thiès"), ("C2", "THIES")],
        ["customer_id", "ville"])
    res = normaliser_ville(entree)
    cles = [r["ville_norm"] for r in res.select("ville_norm").collect()]
    assert set(cles) == {"Thies"}


def test_normaliser_telephone(spark):
    entree = spark.createDataFrame(
        [("C1", "+221 77 780 83 93"), ("C2", "77-780-83-93"), ("C3", "123456789")],
        ["customer_id", "telephone"])
    res = normaliser_telephone(entree).orderBy("customer_id").collect()
    assert res[0]["telephone"] == "777808393"
    assert res[0]["telephone_valide"] is True
    assert res[1]["telephone_valide"] is True
    assert res[2]["telephone_valide"] is False


def test_valider_naissance(spark):
    entree = spark.createDataFrame(
        [("C1", "1990-05-12"), ("C2", "1900-01-01"), ("C3", "2999-01-01")],
        ["customer_id", "date_naissance"])
    res = valider_naissance(entree).orderBy("customer_id").collect()
    assert str(res[0]["date_naissance"]) == "1990-05-12"
    assert res[1]["date_naissance"] is None
    assert res[2]["date_naissance"] is None


def test_dedupliquer_clients_quasi_doublon_email(spark):
    entree = spark.createDataFrame(
        [("C1", "Jean.Dupont@Gmail.com"), ("C2", "jean.dupont@gmail.com")],
        ["customer_id", "email"])
    res = normaliser_email(entree).transform(dedupliquer_clients)
    assert res.count() == 1


def test_dedupliquer_clients_preserve_emails_nuls(spark):
    # Deux clients sans email ne doivent pas fusionner entre eux.
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
    ])
    entree = spark.createDataFrame([("C1", None), ("C2", None)], schema)
    res = dedupliquer_clients(entree)
    assert res.count() == 2


def test_nettoyer_clients_pipeline(spark):
    entree = spark.createDataFrame(
        [
            ("C1", "Jean.DUPONT@Gmail.com", "+221 77 780 83 93", " DAKAR ", "1990-05-12"),
            ("C2", "N/A", "123456789", "thies", "1900-01-01"),
            ("C2", "N/A", "123456789", "thies", "1900-01-01"),
        ],
        ["customer_id", "email", "telephone", "ville", "date_naissance"])
    res = nettoyer_clients(entree)
    # La 3e ligne est un doublon exact de la 2e : il doit disparaitre.
    assert res.count() == 2
