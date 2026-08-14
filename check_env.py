#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_env.py — Vérification de l'environnement du cours Big Data Engineering
Master 1 — DMI/FST/UCAD — TP 1

Usage : python3 check_env.py
Sortie : un bilan ligne par ligne ; code retour 0 si tout est OK.
"""
import importlib
import shutil
import subprocess
import sys

OK, KO, WARN = "[ OK ]", "[ÉCHEC]", "[AVERT]"
PY_MIN = (3, 9)
MODULES_REQUIS = ["pandas", "numpy", "faker", "jupyter_core"]
MODULES_OPTIONNELS = ["matplotlib"]

erreurs = 0


def check(label, ok, detail="", warn=False):
    global erreurs
    tag = OK if ok else (WARN if warn else KO)
    if not ok and not warn:
        erreurs += 1
    print(f"{tag} {label}" + (f" — {detail}" if detail else ""))


def main():
    print("=" * 62)
    print(" Vérification de l'environnement — Big Data Engineering UCAD")
    print("=" * 62)

    # 1. Python
    v = sys.version_info
    check(f"Python {v.major}.{v.minor}.{v.micro}", (v.major, v.minor) >= PY_MIN,
          f"minimum requis : {PY_MIN[0]}.{PY_MIN[1]}")

    # 2. pip
    try:
        out = subprocess.run([sys.executable, "-m", "pip", "--version"],
                             capture_output=True, text=True, timeout=30)
        check("pip", out.returncode == 0, out.stdout.strip().split(" from ")[0])
    except Exception as e:
        check("pip", False, str(e))

    # 3. environnement virtuel ?
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    check("Environnement virtuel actif", in_venv,
          "recommandé : python3 -m venv venv-bigdata", warn=not in_venv)

    # 4. Git
    git = shutil.which("git")
    if git:
        ver = subprocess.run(["git", "--version"], capture_output=True,
                             text=True).stdout.strip()
        check("Git installé", True, ver)
        for cle in ("user.name", "user.email"):
            val = subprocess.run(["git", "config", "--global", cle],
                                 capture_output=True, text=True).stdout.strip()
            check(f"git config {cle}", bool(val), val or
                  f'à faire : git config --global {cle} "..."')
    else:
        check("Git installé", False, "https://git-scm.com/downloads")

    # 5. Bibliothèques Python
    for mod in MODULES_REQUIS:
        try:
            m = importlib.import_module(mod)
            check(f"module {mod}", True, getattr(m, "__version__", ""))
        except ImportError:
            check(f"module {mod}", False, "pip install -r requirements.txt")
    for mod in MODULES_OPTIONNELS:
        try:
            importlib.import_module(mod)
            check(f"module {mod} (optionnel)", True)
        except ImportError:
            check(f"module {mod} (optionnel)", True, "absent — non bloquant",
                  warn=True)

    # 6. Bilan
    print("-" * 62)
    if erreurs == 0:
        print("Tout est vert : vous pouvez passer à la partie B du TP.")
    else:
        print(f"{erreurs} point(s) bloquant(s) — corrigez avant de continuer "
              "(appelez l'enseignant si besoin).")
    return 1 if erreurs else 0


if __name__ == "__main__":
    sys.exit(main())
