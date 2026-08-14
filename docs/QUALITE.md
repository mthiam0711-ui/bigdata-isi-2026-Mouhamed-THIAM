# TP3 — Qualité des données : `customers.csv`

Mesures réalisées sur les données réelles (échelle 0.1, `data/customers.csv`,
5025 lignes brutes). Voir `notebooks/TP3_nettoyage.ipynb` pour le détail des
cellules de mesure.

## Tableau de diagnostic

| Défaut                                          | Mesure | Colonne          |
|--------------------------------------------------|-------:|------------------|
| Emails manquants (null + `""` + `"N/A"`)          |    150 | `email`          |
| Villes distinctes avant normalisation             |     56 | `ville`          |
| Noms avec espaces parasites (`nom != trim(nom)`)  |    100 | `nom`            |
| Dates de naissance hors bornes (1920 → aujourd'hui, ou illisibles) | 25 | `date_naissance` |
| Doublons exacts (`count - distinct`)              |     15 | (toutes)         |
| Téléphones non conformes (hors préfixe 70/75/76/77/78, 9 chiffres) | 0 | `telephone` |

## Après nettoyage (`nettoyer_clients`)

| Mesure                          | Avant | Après |
|----------------------------------|------:|------:|
| Lignes                           |  5025 |  4998 |
| Villes distinctes                |    56 |    19 |

27 lignes ont disparu au total (15 doublons exacts + quasi-doublons d'email
détectés après normalisation, cf. `dedupliquer_clients`).

## Décisions prises

- **Emails manquants** : mis à `null` (pas de suppression de ligne) via
  `unifier_manquants`, puis un drapeau `email_valide` est ajouté par
  `normaliser_email`. Un email absent ou mal formé n'invalide pas le reste
  des informations exploitables du client (téléphone, ville, date de
  naissance).
- **Ville** : deux colonnes en sortie — `ville` (affichage, trim + initcap,
  accents conservés) et `ville_norm` (clé sans accent, via une UDF Python,
  utilisée pour le regroupement et la déduplication).
- **Téléphone** : conservé sous forme de 9 chiffres sans indicatif ni
  séparateurs, avec un drapeau `telephone_valide` (préfixe 70/75/76/77/78).
  Aucun numéro non conforme trouvé sur ce jeu de données (mesure : 0).
- **Date de naissance** : parsée en `date` ; toute valeur hors bornes
  (avant 1920 ou dans le futur) ou illisible devient `null` plutôt que
  d'être conservée telle quelle.
- **Déduplication** : exécutée en dernier dans le pipeline, après toutes les
  normalisations — sinon les quasi-doublons (même personne, email en
  casse différente) survivraient. Doublons exacts retirés en premier
  (`dropDuplicates()` toutes colonnes), puis quasi-doublons par email
  normalisé (repli sur `customer_id` pour les clients sans email, afin de
  ne pas les fusionner entre eux à tort).

## Preuve des tests

`pytest -q` : 9/9 tests au vert — voir `docs/pytest_output.txt`.
