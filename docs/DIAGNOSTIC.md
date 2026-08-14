# Diagnostic technique — Les limites du traitement local

> **Livrable de la séance 1** — Big Data Engineering — ISI 
> Auteur : <Pape hamady Fall> 
> Longueur attendue : 1 à 2 pages. Toute affirmation doit s'appuyer sur une
> **mesure** issue de votre notebook (`notebooks/TP1_exploration.ipynb`).

## 1. Constats — où le traitement local atteint-il ses limites ?

Mesures (échelle 0.1 en local, échelle 1.0 sur Colab, comme demandé par la
consigne) :

| Opération | Échelle / environnement | Lignes | Temps (s) | Mémoire (Mo) | Observation |
|---|---|---|---|---|---|
| Chargement `orders.csv` | 0.1, local | 50 000 | 0,32 | 17,7 | ratio mémoire/disque ×5,7 (disque 3,1 Mo) |
| Jointure `orders × items` | 0.1, local | 112 750 | 0,37 | 54 (résultat) | mémoire avant jointure 38 Mo → 54 Mo après (+42 %), alors que le nombre de lignes croît de +125 % |
| Chargement `events.json` | 0.1, local | 329 976 | 4,31 | 137,7 | ratio mémoire/disque ×2,1 (disque 65,3 Mo) |
| Chargement `orders.csv` | 1.0, Colab | 500 000 | 1,68 | 176,4 | disque 31,0 Mo |
| Chargement `events.json` | 1.0, Colab | 3 301 501 | 42,34 | 1 346,5 | chargé sans crash ni avertissement — ratio mémoire/disque ×2,06 |

**Ratio mémoire/disque.** Il varie de ×2,1 (`events.json`, JSON riche en clés
répétées) à ×5,7 (`orders.csv`, colonnes texte type `object`), mais reste
**stable à volume constant** entre l'échelle 0.1 et l'échelle 1.0 (×2,10 puis
×2,06 pour `events.json`) : c'est une propriété de la représentation en
mémoire de pandas, indépendante du volume.

**Ce qui s'est passé au chargement de `events.json` à l'échelle 1.0.** Sur
Colab, le chargement a réussi sans erreur ni avertissement. Le temps a été
multiplié par ×9,8 pour un volume ×10 (4,31 s → 42,34 s), c'est-à-dire une
croissance quasi linéaire ; la mémoire a suivi le même mouvement de façon
linéaire (ratio mémoire/disque quasi identique aux deux échelles).

**Extrapolation à 50 Go puis 1 To.** Le ratio mémoire/disque étant stable, la
mémoire nécessaire s'extrapole **linéairement et de façon fiable** :
≈ 105 Go pour 50 Go de données, ≈ 2,1 To pour 1 To. Le temps s'extrapole
linéairement **tant que la machine cible garde une marge de RAM largement
supérieure au volume à charger** — or à 50 Go, cela demanderait ≈ 105 Go de
RAM disponibles, ce qu'aucune machine courante n'offre. L'hypothèse linéaire
cesse donc d'être fiable bien avant les 50 Go.

## 2. Analyse — pourquoi ça casse ?

**Pourquoi un DataFrame pandas occupe plus de mémoire que le fichier
source.** Sur disque, une valeur est du texte compact (ex. un entier tient en
quelques octets ASCII). En mémoire, pandas doit soit la caster dans un type
fixe (int64/float64, 8 octets par valeur, quel que soit le nombre de
chiffres), soit, pour du texte, créer un objet Python complet (`str`) avec
son en-tête, sa longueur et son pointeur — nettement plus lourd qu'une suite
de caractères ASCII sur disque. C'est exactement ce qu'on observe : ×5,7 pour
`orders.csv` (beaucoup de colonnes `object`) contre ×2,1 pour `events.json`
(structure JSON avec clés répétées, mais valeurs plus homogènes).

**Pourquoi la jointure aggrave le problème.** Sur `orders × items`, la
mémoire déjà occupée par les deux tables sources (38 Mo) et le résultat de la
jointure (54 Mo) coexistent en mémoire **au même moment** pendant l'opération
— soit un pic d'environ 92 Mo, pas seulement les 54 Mo du résultat final. Une
jointure duplique aussi les colonnes de la table de gauche pour chaque ligne
correspondante à droite : le nombre de lignes a crû de +125 % (50 000 →
112 750) pour un volume mémoire final en hausse de seulement +42 %, ce qui
montre que le **pic transitoire pendant le calcul** peut dépasser largement
le volume du résultat final — un danger pour la mémoire disponible que la
seule taille du résultat ne laisse pas deviner.

**Pourquoi le scale-up (plus de RAM) n'est pas une stratégie durable.** Pour
garder le même confort à 50 Go de données, il faudrait ≈ 105 Go de RAM
disponibles rien que pour ce fichier ; à 1 To, ≈ 2,1 To. Au-delà d'un certain
point, le coût d'achat de RAM supplémentaire cesse d'être linéaire (le haut
de gamme serveur coûte disproportionnellement plus cher), un plafond
physique existe (nombre de barrettes, bus mémoire), la machine reste un
**point de panne unique** (aucune tolérance si elle tombe), et elle ne
traite qu'**un job à la fois** — aucune concurrence d'accès entre plusieurs
analystes ou pipelines sur le même jeu de données.

**Parades locales et leurs limites.** Lecture par blocs (`chunksize`),
`dtype` optimisés (`category`, entiers plus étroits que `object`/`int64`),
formats binaires colonnes (Parquet, qui évite le re-parsing et compresse),
échantillonnage pour l'exploration. Elles repoussent le mur d'un facteur
limité (2 à 10× environ) — mais ne changent pas l'ordre de grandeur : aucune
de ces techniques n'aurait permis de charger 1 To en RAM sur une seule
machine, aussi optimisée soit-elle.

## 3. Besoins — ce qu'une architecture distribuée doit apporter

1. Le système doit **traiter des données plus grandes que la RAM** d'une
   seule machine, sans jamais avoir besoin de tout charger en mémoire d'un
   coup.
2. Le système doit **paralléliser le calcul sur plusieurs machines**, pour
   que le temps de traitement ne dépende plus de la marge RAM/volume d'un
   seul nœud.
3. Le système doit **tolérer la panne d'un nœud** sans perdre le job en
   cours ni les données déjà traitées.
4. Le système doit **lire nativement des formats variés** (CSV, JSON,
   Parquet…) sans réécriture manuelle par format.
5. Le système doit **rester pilotable depuis Python**, avec une API proche de
   pandas, pour capitaliser sur les compétences déjà acquises.
6. Le système doit **passer à l'échelle sans réécrire le code** entre le
   prototype (petit volume, une machine) et la production (gros volume,
   cluster).

Pandas **reste** le bon outil pour l'exploration rapide et l'analyse fine sur
des volumes qui tiennent confortablement en RAM avec une large marge (le cas
de notre point de mesure à l'échelle 1.0 sur Colab, ~1,35 Go) — la
distribution n'a de sens qu'une fois cette marge trop réduite pour être
fiable.

## 4. (Optionnel) Questions ouvertes

Ce que vous n'avez pas compris ou aimeriez approfondir — discuté en séance 2.
