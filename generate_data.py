#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Générateur de données du fil rouge e-commerce
Cours : Big Data Engineering — Master 1, DMI / FST / UCAD
Enseignant : Prof. Samba NDIAYE
================================================================================

Génère, de manière DÉTERMINISTE (graine fixe), l'ensemble des données utilisées
dans les travaux pratiques des 12 séances :

  - customers.csv        : clients (avec anomalies contrôlées — séance 3)
  - products.csv         : catalogue produits
  - orders.csv           : commandes (skew client/région — séance 8 ;
                           clés orphelines — séance 5 ; types sales — séance 3/4)
  - order_items.csv      : lignes de commandes
  - payments.json        : paiements, JSON Lines (anomalies qualité — séance 9)
  - events.json          : événements de navigation, JSON Lines
                           (désordre temporel et retards — séance 11)
  - pg_init.sql          : dump PostgreSQL (données de référence — séance 9)
  - anomalies_manifest.json : comptes exacts des anomalies injectées
                              (DOCUMENT ENSEIGNANT — ne pas distribuer)

Usage :
  python3 generate_data.py --scale 1.0 --outdir ./data
  python3 generate_data.py --scale 0.1 --outdir ./data_sample
  python3 generate_data.py --scale 1.0 --small-files 400   # démo séance 7

Échelle 1.0 (standard) : 50 000 clients, 2 000 produits, 500 000 commandes,
~1,25 M lignes de commandes, ~480 000 paiements, ~2,9 M événements (~550 Mo).
Toutes les volumétries varient linéairement avec --scale.
"""

import argparse
import json
import os
import sys
import unicodedata

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42

# ------------------------------------------------------------------ référentiels
PRENOMS = [
    "Mamadou", "Fatou", "Ousmane", "Aïssatou", "Cheikh", "Awa", "Ibrahima",
    "Mariama", "Moussa", "Khady", "Abdoulaye", "Ndèye", "Modou", "Astou",
    "Serigne", "Sokhna", "Pape", "Coumba", "Aliou", "Dieynaba", "Babacar",
    "Adama", "Idrissa", "Rokhaya", "Lamine", "Aminata", "Souleymane", "Bineta",
    "Omar", "Yacine", "Malick", "Seynabou", "Boubacar", "Maguette", "Assane",
    "Diarra", "El Hadji", "Fatoumata", "Gora", "Ramatoulaye",
]
NOMS = [
    "Ndiaye", "Diop", "Fall", "Sow", "Sarr", "Ba", "Diallo", "Faye", "Guèye",
    "Sy", "Sène", "Mbaye", "Niang", "Cissé", "Kane", "Diouf", "Thiam",
    "Camara", "Ndour", "Wade", "Mbodj", "Samb", "Lô", "Dieng", "Badji",
    "Sané", "Mendy", "Goudiaby", "Tall", "Kébé",
]

# (ville, région, poids) — poids volontairement déséquilibrés : Dakar ≈ 45 %
# de l'activité une fois Pikine/Guédiawaye/Rufisque agrégées (skew — séance 8)
VILLES = [
    ("Dakar", "Dakar", 0.30), ("Pikine", "Dakar", 0.08),
    ("Guédiawaye", "Dakar", 0.04), ("Rufisque", "Dakar", 0.03),
    ("Thiès", "Thiès", 0.10), ("Mbour", "Thiès", 0.05),
    ("Touba", "Diourbel", 0.09), ("Diourbel", "Diourbel", 0.02),
    ("Saint-Louis", "Saint-Louis", 0.06), ("Kaolack", "Kaolack", 0.05),
    ("Ziguinchor", "Ziguinchor", 0.04), ("Louga", "Louga", 0.03),
    ("Tambacounda", "Tambacounda", 0.02), ("Kolda", "Kolda", 0.02),
    ("Fatick", "Fatick", 0.02), ("Matam", "Matam", 0.02),
    ("Kaffrine", "Kaffrine", 0.01), ("Sédhiou", "Sédhiou", 0.01),
    ("Kédougou", "Kédougou", 0.01),
]

CATEGORIES = {
    "Téléphonie": (15000, 450000), "Informatique": (25000, 900000),
    "Électroménager": (20000, 600000), "Mode Homme": (3000, 60000),
    "Mode Femme": (3000, 80000), "Beauté & Soins": (1500, 40000),
    "Maison & Cuisine": (2500, 120000), "Sport & Loisirs": (4000, 90000),
    "Épicerie": (500, 25000), "Enfants & Jouets": (2000, 50000),
}
MARQUES = ["Tecno", "Samsung", "Infinix", "Itel", "HP", "Lenovo", "LG", "Hisense",
           "Nivea", "Adidas", "Royal", "Dakar Style", "Penc Mi", "Kirène",
           "Sunu Tech", "Teranga Home"]

PAIEMENT_METHODES = ["Orange Money", "Wave", "Carte bancaire",
                     "Paiement à la livraison", "Free Money"]
PAIEMENT_POIDS = [0.35, 0.30, 0.08, 0.22, 0.05]

DOMAINES_EMAIL = ["gmail.com", "yahoo.fr", "orange.sn", "hotmail.com", "ucad.edu.sn"]

EVENT_TYPES_NAV = ["page_view", "search", "view_product", "add_to_cart", "remove_from_cart"]
DEVICES = ["mobile", "desktop", "tablette"]
DEVICE_POIDS = [0.78, 0.20, 0.02]


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


# ============================================================== CUSTOMERS
def gen_customers(n, rng, fake, manifest):
    print(f"  - customers : {n:,} lignes")
    idx = np.arange(1, n + 1)
    cid = np.char.add("C", np.char.zfill(idx.astype(str), 6))
    prenoms = rng.choice(PRENOMS, n)
    noms = rng.choice(NOMS, n)

    v_idx = rng.choice(len(VILLES), n, p=[v[2] for v in VILLES])
    villes = np.array([VILLES[i][0] for i in v_idx])
    regions = np.array([VILLES[i][1] for i in v_idx])

    # emails — base propre
    doms = rng.choice(DOMAINES_EMAIL, n)
    nums = rng.integers(1, 999, n)
    emails = np.array([
        f"{strip_accents(p).lower().replace(' ', '')}."
        f"{strip_accents(no).lower()}{num}@{d}"
        for p, no, num, d in zip(prenoms, noms, nums, doms)
    ], dtype=object)

    # téléphones — formats volontairement hétérogènes (séance 3)
    prefixes = rng.choice(["77", "78", "76", "70", "75"], n, p=[0.45, 0.25, 0.15, 0.10, 0.05])
    suffix = rng.integers(0, 10_000_000, n)
    fmt = rng.choice(4, n, p=[0.40, 0.30, 0.20, 0.10])
    phones = np.empty(n, dtype=object)
    for i in range(n):
        s = f"{suffix[i]:07d}"
        if fmt[i] == 0:
            phones[i] = f"+221{prefixes[i]}{s}"
        elif fmt[i] == 1:
            phones[i] = f"{prefixes[i]}{s}"
        elif fmt[i] == 2:
            phones[i] = f"+221 {prefixes[i]} {s[:3]} {s[3:5]} {s[5:]}"
        else:
            phones[i] = f"{prefixes[i]}-{s[:3]}-{s[3:5]}-{s[5:]}"
    manifest["customers"]["telephones_formats_heterogenes"] = int(n)

    # dates
    signup = pd.Timestamp("2022-01-01") + pd.to_timedelta(
        rng.integers(0, 1600, n), unit="D")
    birth = pd.Timestamp("1958-01-01") + pd.to_timedelta(
        rng.integers(0, 17500, n), unit="D")

    adresses = np.array([fake.street_address() for _ in range(min(n, 500))], dtype=object)
    adresses = adresses[rng.integers(0, len(adresses), n)]

    df = pd.DataFrame({
        "customer_id": cid, "prenom": prenoms, "nom": noms, "email": emails,
        "telephone": phones, "adresse": adresses, "ville": villes,
        "region": regions, "date_naissance": birth.strftime("%Y-%m-%d"),
        "date_inscription": signup.strftime("%Y-%m-%d"),
    })

    df_clean = df.copy()  # version propre pour PostgreSQL (référentiel maître)

    # ---- ANOMALIES (uniquement dans le CSV, le référentiel SQL reste propre)
    # 1) emails manquants ~3 % (mélange de vide et de "N/A")
    k = int(0.03 * n)
    sel = rng.choice(n, k, replace=False)
    half = k // 2
    df.loc[sel[:half], "email"] = ""
    df.loc[sel[half:], "email"] = "N/A"
    manifest["customers"]["emails_manquants_ou_NA"] = int(k)

    # 2) casse incohérente des villes ~4 % + accents retirés ~2 %
    k2 = int(0.04 * n)
    sel2 = rng.choice(n, k2, replace=False)
    df.loc[sel2[: k2 // 2], "ville"] = df.loc[sel2[: k2 // 2], "ville"].str.upper()
    df.loc[sel2[k2 // 2:], "ville"] = df.loc[sel2[k2 // 2:], "ville"].str.lower()
    k3 = int(0.02 * n)
    sel3 = rng.choice(n, k3, replace=False)
    df.loc[sel3, "ville"] = df.loc[sel3, "ville"].map(strip_accents)
    manifest["customers"]["villes_casse_ou_accents_incoherents"] = int(k2 + k3)

    # 3) espaces parasites dans le nom ~2 %
    k4 = int(0.02 * n)
    sel4 = rng.choice(n, k4, replace=False)
    df.loc[sel4, "nom"] = "  " + df.loc[sel4, "nom"] + " "
    manifest["customers"]["noms_avec_espaces_parasites"] = int(k4)

    # 4) dates de naissance aberrantes ~0,5 % (futur ou âge > 110 ans)
    k5 = max(1, int(0.005 * n))
    sel5 = rng.choice(n, k5, replace=False)
    df.loc[sel5[: k5 // 2], "date_naissance"] = "2031-05-14"
    df.loc[sel5[k5 // 2:], "date_naissance"] = "1900-01-01"
    manifest["customers"]["dates_naissance_aberrantes"] = int(k5)

    # 5) doublons : ~0,3 % exacts + ~0,2 % quasi-doublons (email en MAJUSCULES)
    kd = max(1, int(0.003 * n))
    dup_exact = df.sample(kd, random_state=SEED)
    kq = max(1, int(0.002 * n))
    dup_quasi = df.sample(kq, random_state=SEED + 1).copy()
    dup_quasi["email"] = dup_quasi["email"].str.upper()
    df = pd.concat([df, dup_exact, dup_quasi], ignore_index=True)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    manifest["customers"]["doublons_exacts"] = int(kd)
    manifest["customers"]["quasi_doublons_email_majuscules"] = int(kq)

    return df, df_clean


# ============================================================== PRODUCTS
def gen_products(n, rng, manifest):
    print(f"  - products  : {n:,} lignes")
    idx = np.arange(1, n + 1)
    pid = np.char.add("P", np.char.zfill(idx.astype(str), 5))
    cats = list(CATEGORIES.keys())
    cat = rng.choice(cats, n)
    lo = np.array([CATEGORIES[c][0] for c in cat], dtype=float)
    hi = np.array([CATEGORIES[c][1] for c in cat], dtype=float)
    # prix log-uniformes arrondis aux 500 FCFA
    prix = np.exp(rng.uniform(np.log(lo), np.log(hi)))
    prix = (np.round(prix / 500) * 500).astype(int)
    marque = rng.choice(MARQUES, n)
    nom = np.array([f"{m} {c} {i:04d}" for m, c, i in zip(marque, cat, idx)], dtype=object)
    stock = rng.integers(0, 800, n)
    df = pd.DataFrame({
        "product_id": pid, "nom_produit": nom, "categorie": cat,
        "marque": marque, "prix_unitaire_fcfa": prix, "stock": stock,
    })
    manifest["products"]["nb_produits"] = int(n)
    return df


# ============================================================== ORDERS + ITEMS
def gen_orders_items(n_orders, customers_clean, products, rng, manifest):
    print(f"  - orders    : {n_orders:,} lignes (+ lignes de commandes)")
    n_cust = len(customers_clean)
    cust_ids = customers_clean["customer_id"].to_numpy()

    # ---- skew client (séance 8) : un client "grossiste" concentre ~5 % des
    # commandes ; le reste suit une loi de type Zipf.
    whale = cust_ids[0]  # C000001
    n_whale = int(0.05 * n_orders)
    ranks = np.arange(1, n_cust + 1, dtype=float)
    w = 1.0 / ranks ** 0.6
    w /= w.sum()
    others = cust_ids[rng.choice(n_cust, n_orders - n_whale, p=w)]
    order_cust = np.concatenate([np.full(n_whale, whale), others])
    rng.shuffle(order_cust)
    manifest["orders"]["commandes_client_grossiste_C000001"] = int(n_whale)

    # ---- clés orphelines ~0,2 % (séance 5 : anti-jointures)
    k_orph = max(1, int(0.002 * n_orders))
    sel = rng.choice(n_orders, k_orph, replace=False)
    order_cust[sel] = np.array(
        [f"C9{rng.integers(10000, 99999)}" for _ in range(k_orph)])
    manifest["orders"]["commandes_client_inexistant"] = int(k_orph)

    # ---- dates : 24 mois (2024-07-01 → 2026-06-30), tendance + pic décembre
    days = np.arange(730)
    wd = 1.0 + days / 730 * 0.6
    base = pd.Timestamp("2024-07-01")
    months = (base + pd.to_timedelta(days, unit="D")).month
    wd[months == 12] *= 1.5
    wd /= wd.sum()
    d = rng.choice(days, n_orders, p=wd)
    secs = rng.integers(6 * 3600, 23 * 3600, n_orders)
    order_ts = base + pd.to_timedelta(d, unit="D") + pd.to_timedelta(secs, unit="s")

    oid = np.char.add("O", np.char.zfill(np.arange(1, n_orders + 1).astype(str), 7))
    statut = rng.choice(["livrée", "en_cours", "annulée", "retournée"],
                        n_orders, p=[0.78, 0.08, 0.09, 0.05])
    canal = rng.choice(["web", "mobile_app"], n_orders, p=[0.35, 0.65])

    # ---- lignes de commandes
    n_items_per = rng.choice([1, 2, 3, 4, 5, 6], n_orders,
                             p=[0.38, 0.27, 0.17, 0.10, 0.05, 0.03])
    total_items = int(n_items_per.sum())
    row_order = np.repeat(np.arange(n_orders), n_items_per)

    n_prod = len(products)
    pw = 1.0 / np.arange(1, n_prod + 1, dtype=float) ** 0.8
    pw /= pw.sum()
    item_prod_idx = rng.choice(n_prod, total_items, p=pw)
    qty = rng.choice([1, 2, 3], total_items, p=[0.72, 0.21, 0.07])
    base_price = products["prix_unitaire_fcfa"].to_numpy()[item_prod_idx].astype(float)
    promo = np.where(rng.random(total_items) < 0.12,
                     1.0 - rng.uniform(0.05, 0.30, total_items), 1.0)
    unit_price = (np.round(base_price * promo / 100) * 100).astype(int)

    items = pd.DataFrame({
        "order_id": oid[row_order],
        "product_id": products["product_id"].to_numpy()[item_prod_idx],
        "quantite": qty,
        "prix_unitaire_fcfa": unit_price,
    })
    items.insert(0, "item_id",
                 np.char.add("I", np.char.zfill(
                     np.arange(1, total_items + 1).astype(str), 8)))

    sous_total = items.assign(t=items["quantite"] * items["prix_unitaire_fcfa"]) \
                      .groupby("order_id")["t"].sum()
    livraison = rng.choice([0, 1000, 1500, 2000, 3000], n_orders,
                           p=[0.10, 0.30, 0.25, 0.20, 0.15])
    montant = sous_total.reindex(oid).to_numpy() + livraison

    orders = pd.DataFrame({
        "order_id": oid, "customer_id": order_cust,
        "date_commande": pd.Series(order_ts).dt.strftime("%Y-%m-%d %H:%M:%S"),
        "statut": statut, "canal": canal,
        "frais_livraison_fcfa": livraison,
        "montant_total_fcfa": montant.astype(int).astype(str),
    })

    # ---- pollution de type ~1 % : suffixe " FCFA" (séances 3-4 : conversion)
    k_fcfa = int(0.01 * n_orders)
    sel = rng.choice(n_orders, k_fcfa, replace=False)
    orders.loc[sel, "montant_total_fcfa"] = \
        orders.loc[sel, "montant_total_fcfa"] + " FCFA"
    manifest["orders"]["montants_avec_suffixe_FCFA"] = int(k_fcfa)
    manifest["orders"]["nb_commandes"] = int(n_orders)
    manifest["order_items"]["nb_lignes"] = int(total_items)

    return orders, items, order_ts


# ============================================================== PAYMENTS
def gen_payments(orders, order_ts, rng, manifest, outpath):
    paid_mask = (orders["statut"] != "annulée") & (rng.random(len(orders)) < 0.97)
    base = orders.loc[paid_mask, ["order_id", "customer_id"]].reset_index(drop=True)
    n = len(base)
    print(f"  - payments  : ~{n:,} lignes (JSON Lines)")

    montant = (orders.loc[paid_mask, "montant_total_fcfa"]
               .str.replace(" FCFA", "", regex=False).astype(int)
               .reset_index(drop=True))
    ts = pd.Series(order_ts)[paid_mask.to_numpy()].reset_index(drop=True)
    paid_at = ts + pd.to_timedelta(rng.integers(30, 7200, n), unit="s")

    methode = rng.choice(PAIEMENT_METHODES, n, p=PAIEMENT_POIDS)
    # statuts à casse incohérente (séance 9 : normalisation + Great Expectations)
    statut = rng.choice(["REUSSI", "reussi", "Réussi", "OK", "succes", "ECHEC"],
                        n, p=[0.40, 0.20, 0.15, 0.12, 0.08, 0.05])
    manifest["payments"]["statuts_casse_incoherente"] = int(n)

    df = pd.DataFrame({
        "payment_id": np.char.add("PAY", np.char.zfill(
            np.arange(1, n + 1).astype(str), 8)),
        "order_id": base["order_id"], "customer_id": base["customer_id"],
        "montant_fcfa": montant.to_numpy(), "methode": methode, "statut": statut,
        "paid_at": paid_at.dt.strftime("%Y-%m-%d %H:%M:%S"),
        "operateur_ref": [f"TX{rng.integers(10**9, 10**10 - 1)}" for _ in range(n)],
    })

    # montants négatifs ~0,1 %
    k_neg = max(1, int(0.001 * n))
    sel = rng.choice(n, k_neg, replace=False)
    df.loc[sel, "montant_fcfa"] = -df.loc[sel, "montant_fcfa"]
    manifest["payments"]["montants_negatifs"] = int(k_neg)

    # écarts montant payé / montant commandé ~0,5 % (paiements partiels)
    k_mis = max(1, int(0.005 * n))
    sel = rng.choice(n, k_mis, replace=False)
    df.loc[sel, "montant_fcfa"] = (df.loc[sel, "montant_fcfa"] * 0.5).astype(int)
    manifest["payments"]["paiements_partiels_ecart_montant"] = int(k_mis)

    # paiements orphelins ~0,3 % (order_id inexistant)
    k_orph = max(1, int(0.003 * n))
    orph = df.sample(k_orph, random_state=SEED).copy()
    orph["payment_id"] = [f"PAY9{rng.integers(10**6, 10**7 - 1)}" for _ in range(k_orph)]
    orph["order_id"] = [f"O9{rng.integers(10**5, 10**6 - 1)}" for _ in range(k_orph)]
    manifest["payments"]["paiements_orphelins_order_id_inexistant"] = int(k_orph)

    # doublons ~0,8 % (effet de livraison at-least-once — séances 9 et 10)
    k_dup = max(1, int(0.008 * n))
    dup = df.sample(k_dup, random_state=SEED + 2)
    manifest["payments"]["doublons_at_least_once"] = int(k_dup)

    df = pd.concat([df, orph, dup], ignore_index=True) \
           .sample(frac=1, random_state=SEED).reset_index(drop=True)
    df.to_json(outpath, orient="records", lines=True, force_ascii=False)
    manifest["payments"]["nb_paiements_total"] = int(len(df))


# ============================================================== EVENTS
def gen_events(orders, order_ts, customers_clean, products, rng, manifest,
               outpath, small_files=0):
    n_orders = len(orders)
    cust_ids = customers_clean["customer_id"].to_numpy()
    prod_ids = products["product_id"].to_numpy()
    villes = [v[0] for v in VILLES]
    vp = np.array([v[2] for v in VILLES]); vp /= vp.sum()

    # sessions d'achat (1 par commande, 3-6 événements) +
    # sessions de navigation pure (0,6 × commandes, 2-5 événements)
    n_nav = int(0.6 * n_orders)
    est_total = int(n_orders * 4.5 + n_nav * 3.5)
    print(f"  - events    : ~{est_total:,} lignes (JSON Lines, écrit par lots)")

    if os.path.exists(outpath):
        os.remove(outpath)
    small_dir = None
    if small_files > 0:
        small_dir = os.path.join(os.path.dirname(outpath), "events_small_files")
        os.makedirs(small_dir, exist_ok=True)

    eid = 0
    n_late_total = 0
    chunk = 100_000
    file_idx = 0

    def write(df):
        nonlocal file_idx
        df.to_json(outpath, orient="records", lines=True,
                   force_ascii=False, mode="a")
        if small_dir is not None:
            # éclatement volontaire en petits fichiers (séance 7)
            part_size = max(1, est_total // max(1, small_files))
            for s0 in range(0, len(df), part_size):
                part = df.iloc[s0:s0 + part_size]
                part.to_json(os.path.join(small_dir, f"events_{file_idx:06d}.json"),
                             orient="records", lines=True, force_ascii=False)
                file_idx += 1

    def build_sessions(n_sess, ts_base, user_ids, purchase, order_ids=None):
        nonlocal eid, n_late_total
        n_ev = rng.integers(3 if purchase else 2, 7 if purchase else 6, n_sess)
        total = int(n_ev.sum())
        row = np.repeat(np.arange(n_sess), n_ev)
        # position de l'événement dans sa session
        pos = np.concatenate([np.arange(k) for k in n_ev])
        offs = rng.integers(5, 240, total).cumsum()
        offs -= np.repeat(np.concatenate([[0], offs[np.cumsum(n_ev)[:-1] - 1]]), n_ev)
        ev_time = ts_base.to_numpy()[row] + offs.astype("timedelta64[s]")

        etype = rng.choice(EVENT_TYPES_NAV, total,
                           p=[0.30, 0.15, 0.35, 0.15, 0.05]).astype(object)
        if purchase:
            last = np.cumsum(n_ev) - 1
            etype[last] = "purchase"
        prod = prod_ids[rng.integers(0, len(prod_ids), total)].astype(object)
        prod[np.isin(etype, ["search", "page_view"])] = None
        oid_col = np.full(total, None, dtype=object)
        if purchase and order_ids is not None:
            oid_col[np.cumsum(n_ev) - 1] = order_ids

        sess = np.char.add("S", np.char.zfill(
            (np.arange(n_sess) + eid).astype(str), 9))
        df = pd.DataFrame({
            "event_id": np.char.add("E", np.char.zfill(
                np.arange(eid * 8, eid * 8 + total).astype(str), 10)),
            "event_time": pd.Series(ev_time).dt.strftime("%Y-%m-%d %H:%M:%S"),
            "session_id": sess[row],
            "user_id": user_ids[row],
            "event_type": etype,
            "product_id": prod,
            "order_id": oid_col,
            "device": rng.choice(DEVICES, total, p=DEVICE_POIDS),
            "ville": rng.choice(villes, total, p=vp),
        })
        eid += n_sess

        # ---- retards (séance 11) : 1 % des événements voient leur event_time
        # reculé de 10 à 60 min (ils arriveront "en retard" au rejeu)
        k_late = max(1, int(0.01 * total))
        sel = rng.choice(total, k_late, replace=False)
        late = (pd.to_datetime(df.loc[sel, "event_time"])
                - pd.to_timedelta(rng.integers(600, 3600, k_late), unit="s"))
        df.loc[sel, "event_time"] = late.dt.strftime("%Y-%m-%d %H:%M:%S")
        n_late_total += k_late

        # ---- désordre temporel léger : tri par event_time + bruit d'arrivée
        arrival = pd.to_datetime(df["event_time"]) + pd.to_timedelta(
            rng.normal(0, 90, total).astype(int), unit="s")
        df = df.iloc[np.argsort(arrival.to_numpy())].reset_index(drop=True)
        return df

    # sessions d'achat, par lots
    ots = pd.Series(order_ts)
    ocust = orders["customer_id"].to_numpy()
    ooid = orders["order_id"].to_numpy()
    for s in range(0, n_orders, chunk):
        e = min(s + chunk, n_orders)
        start = ots.iloc[s:e] - pd.to_timedelta(
            rng.integers(300, 1800, e - s), unit="s")
        write(build_sessions(e - s, start, ocust[s:e], True, ooid[s:e]))

    # sessions de navigation pure (30 % anonymes)
    for s in range(0, n_nav, chunk):
        e = min(s + chunk, n_nav)
        m = e - s
        users = cust_ids[rng.integers(0, len(cust_ids), m)].astype(object)
        anon = rng.random(m) < 0.30
        users[anon] = [f"ANON-{rng.integers(10**6, 10**7 - 1)}" for _ in range(int(anon.sum()))]
        base = pd.Timestamp("2024-07-01") + pd.to_timedelta(
            rng.integers(0, 730 * 86400, m), unit="s")
        write(build_sessions(m, pd.Series(base), users, False))

    manifest["events"]["evenements_en_retard_injectes"] = int(n_late_total)
    manifest["events"]["desordre_temporel"] = "tri par heure d'arrivée (bruit gaussien 90 s) — event_time non monotone"
    if small_dir:
        manifest["events"]["petits_fichiers_seance7"] = file_idx


# ============================================================== POSTGRES DUMP
def gen_pg_dump(customers_clean, products, outpath):
    print("  - pg_init.sql : référentiel propre (customers, products)")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write("-- Référentiel maître e-commerce — séance 9 (ingestion JDBC)\n")
        f.write("-- Chargement : psql -U postgres -d ecommerce -f pg_init.sql\n")
        f.write("-- Ou via docker-entrypoint-initdb.d (voir README)\n\n")
        f.write("DROP TABLE IF EXISTS customers;\n"
                "CREATE TABLE customers (\n"
                "  customer_id      VARCHAR(10) PRIMARY KEY,\n"
                "  prenom           VARCHAR(50),\n  nom VARCHAR(50),\n"
                "  email            VARCHAR(120),\n  telephone VARCHAR(30),\n"
                "  adresse          VARCHAR(200),\n  ville VARCHAR(50),\n"
                "  region           VARCHAR(50),\n  date_naissance DATE,\n"
                "  date_inscription DATE\n);\n\n")
        f.write("COPY customers FROM stdin;\n")
        for r in customers_clean.itertuples(index=False):
            vals = [str(v).replace("\t", " ") if v not in ("", None) else r"\N" for v in r]
            f.write("\t".join(vals) + "\n")
        f.write("\\.\n\n")
        f.write("DROP TABLE IF EXISTS products;\n"
                "CREATE TABLE products (\n"
                "  product_id         VARCHAR(10) PRIMARY KEY,\n"
                "  nom_produit        VARCHAR(120),\n  categorie VARCHAR(50),\n"
                "  marque             VARCHAR(50),\n"
                "  prix_unitaire_fcfa INTEGER,\n  stock INTEGER\n);\n\n")
        f.write("COPY products FROM stdin;\n")
        for r in products.itertuples(index=False):
            f.write("\t".join(str(v) for v in r) + "\n")
        f.write("\\.\n\n")
        f.write("CREATE INDEX idx_customers_region ON customers(region);\n")
        f.write("CREATE INDEX idx_products_categorie ON products(categorie);\n")


# ============================================================== MAIN
def main():
    ap = argparse.ArgumentParser(description="Générateur fil rouge e-commerce — UCAD")
    ap.add_argument("--scale", type=float, default=1.0,
                    help="facteur d'échelle (1.0 = 50 000 clients / 500 000 commandes)")
    ap.add_argument("--outdir", default="./data", help="répertoire de sortie")
    ap.add_argument("--small-files", type=int, default=0,
                    help="si > 0, écrit AUSSI les événements éclatés en ~N petits fichiers (séance 7)")
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    Faker.seed(args.seed)
    fake = Faker("fr_FR")
    os.makedirs(args.outdir, exist_ok=True)

    n_cust = max(100, int(50_000 * args.scale))
    n_prod = max(50, int(2_000 * args.scale ** 0.5))   # le catalogue croît moins vite
    n_ord = max(500, int(500_000 * args.scale))

    manifest = {k: {} for k in
                ["customers", "products", "orders", "order_items", "payments", "events"]}
    manifest["_meta"] = {
        "seed": args.seed, "scale": args.scale,
        "description": "Comptes exacts des anomalies injectées — document ENSEIGNANT, ne pas distribuer aux étudiants.",
    }

    print(f"Génération (graine={args.seed}, échelle={args.scale}) vers {args.outdir}/")
    customers, customers_clean = gen_customers(n_cust, rng, fake, manifest)
    products = gen_products(n_prod, rng, manifest)
    orders, items, order_ts = gen_orders_items(n_ord, customers_clean, products, rng, manifest)

    customers.to_csv(os.path.join(args.outdir, "customers.csv"), index=False)
    products.to_csv(os.path.join(args.outdir, "products.csv"), index=False)
    orders.to_csv(os.path.join(args.outdir, "orders.csv"), index=False)
    items.to_csv(os.path.join(args.outdir, "order_items.csv"), index=False)

    gen_payments(orders, order_ts, rng, manifest,
                 os.path.join(args.outdir, "payments.json"))
    gen_events(orders, order_ts, customers_clean, products, rng, manifest,
               os.path.join(args.outdir, "events.json"), args.small_files)
    gen_pg_dump(customers_clean, products, os.path.join(args.outdir, "pg_init.sql"))

    with open(os.path.join(args.outdir, "anomalies_manifest.json"), "w",
              encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("\nTerminé. Fichiers générés :")
    for fn in sorted(os.listdir(args.outdir)):
        p = os.path.join(args.outdir, fn)
        if os.path.isfile(p):
            print(f"  {fn:28s} {os.path.getsize(p) / 1e6:10.1f} Mo")


if __name__ == "__main__":
    main()
