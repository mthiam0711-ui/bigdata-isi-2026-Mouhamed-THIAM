# Big Data Engineering — ISI 2026 — Dépôt de Pape hamady Fall

> Master 1 — Isi — Prof. Samba Ndiaye
> Dépôt individuel du cours : tous les livrables du semestre y sont versionnés.

## À compléter (TP 1)

- **Étudiant·e** : <Pape Hamady Fall> — <phfalldk@groupeisi.com> — <n° carte étudiant>
- **Identifiant GitHub** : <phf-dev221>

## Structure du dépôt

```
.
├── README.md          # ce fichier
├── .gitignore         # exclut les données générées (ne pas modifier sans raison)
├── requirements.txt   # dépendances Python du cours
├── check_env.py       # vérification automatique de l'environnement
├── data/              # données générées localement — JAMAIS commitées
├── notebooks/         # notebooks de TP (avec leurs sorties)
├── src/               # code Python modulaire (à partir de la séance 2)
└── docs/
    └── DIAGNOSTIC.md  # livrable de la séance 1
```

## Reproduire les données

Les données ne sont pas versionnées : elles se régénèrent à l'identique
(graine fixe = 42).

```bash
python3 -m venv venv-bigdata && source venv-bigdata/bin/activate
pip install -r requirements.txt
python3 generate_data.py --scale 0.1 --outdir ./data
```

## Journal des livrables

| Séance | Livrable | Statut |
|---|---|---|
| 1 | Dépôt initialisé + `docs/DIAGNOSTIC.md` + notebook TP1 | ☐ |
| 2 | … | ☐ |
