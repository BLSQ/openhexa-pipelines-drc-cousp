# RDC SiteRep — SitRep MVE (Ebola, COUSP-RDC)

Production automatique du **SitRep MVE** (Rapport de Situation de la 17ᵉ
épidémie de Maladie à Virus Ebola, COUSP-RDC) au format Word, dans des pipelines
**OpenHexa**, à partir du tracker **DHIS2** de notification.

Le dépôt contient **deux pipelines** complémentaires :

| Pipeline | Rôle |
|---|---|
| `code/dhis2_tracker_extract/` | Extrait les événements/enrôlements du tracker DHIS2 et alimente la table SQL `mve_notification_events` du workspace OpenHexa. |
| `code/generate_sitrep/` | Lit cette table, calcule les indicateurs, génère les visuels et rend le rapport `.docx` à partir d'un template à marqueurs. |

## Architecture

```
RDC SiteRep/
├── code/
│   ├── dhis2_tracker_extract/      # Pipeline 1 — extraction DHIS2 → table SQL
│   │   ├── pipeline.py             #   entrée OpenHexa
│   │   ├── toolbox.py              #   appels API DHIS2 / transformations
│   │   ├── db_operations.py        #   écriture en base du workspace
│   │   ├── config.py
│   │   └── tests/                  #   pytest (conftest, toolbox, pipeline)
│   │
│   ├── generate_sitrep/            # Pipeline 2 — génération du SitRep .docx
│   │   ├── pipeline.py             #   entrée OpenHexa (4 paramètres)
│   │   ├── core.py                 #   orchestration build_sitrep()
│   │   ├── config.py               #   constantes, géo, âge/sexe, layout
│   │   ├── data/                   #   données → modèle
│   │   │   ├── loader.py           #     load_from_db / load_raw + _clean
│   │   │   ├── indicators.py       #     pivot enrollment-level → indicateurs MVE
│   │   │   ├── metrics.py          #     compute() → SitRepData (KPI, tableaux)
│   │   │   └── model.py            #     dataclass SitRepData
│   │   ├── reporting/              #   visuels → rendu
│   │   │   ├── charts.py           #     courbe épi, pyramide, combinaison symptômes
│   │   │   ├── zone_map.py         #     cartes geopandas (province + zone de santé)
│   │   │   ├── highlights.py       #     faits saillants « à date »
│   │   │   ├── render.py           #     remplit le template par marqueurs [[...]]
│   │   │   ├── narrative.py        #     sections narratives (narrative.yaml)
│   │   │   └── build_template.py   #     fallback v2 (legacy)
│   │   ├── utils/                  #   helpers : dates, numbers, geo, docx
│   │   └── narrative.yaml
│   └── __init__.py                 # shim : ré-exporte la stdlib `code`
│
├── data/                           # local (auto-détecté par config._resolve_layout)
│   ├── extract_data_openhexa/      #   extractions CSV/Parquet (entrées dev)
│   ├── geometry/                   #   provinces.parquet, zone_sante.parquet
│   ├── templates/                  #   Template_SitRep_v3.docx (marqueurs)
│   ├── generate_files/             #   sorties .docx (gitignorées)
│   ├── docs SitRep/                #   références (PDF officiels)
│   └── extrait image SitRep/       #   captures de la cible
│
├── CLAUDE.md                       # guide de travail détaillé + règles métier
├── pyproject.toml                  # deps (uv) ; packages=["code"]
└── .claude/skills/generate-sitrep/ # mode opératoire (skill)
```

## Source de données

- **Production** : table SQL `mve_notification_events` du workspace OpenHexa
  (grain événement, flags `n_*` 0/1), lue via `data.load_from_db()`.
- **Dev / local** : extraction CSV/Parquet au même schéma via `data.load_raw()`,
  ou format long DHIS2 via `data.build_definitive_data()` (pivot enrollment).

## Environnement & commandes

Projet **Python 3.13** géré avec **uv**. Toujours `uv run` depuis la racine.
Les chemins s'auto-détectent (local sous `data/`, OpenHexa sous
`<workspace>/pipelines/sitrep/`).

```bash
# Générer le SitRep en local (sortie dans data/generate_files/)
PYTHONPATH=code/generate_sitrep uv run python -c \
  "from datetime import date; from core import build_sitrep; \
   build_sitrep(reporting_end=date(2026,5,31), period_days=1)"

# Lint / format
uv run ruff check code/generate_sitrep/
uv run ruff format code/generate_sitrep/

# Tests du pipeline d'extraction
uv run pytest code/dhis2_tracker_extract/tests/ -q
```

> Imports « bare » enracinés à `code/generate_sitrep/` : ce dossier doit être
> sur le `sys.path` (OpenHexa le fournit ; en local `PYTHONPATH=…`, et marquer
> ce dossier comme *Sources Root* dans l'IDE).

## Pipeline OpenHexa — génération

`generate_sitrep/pipeline.py` expose 4 paramètres : `reporting_end`,
`period_days`, `dst_file` (optionnel), `dst_dataset` (optionnel). Il lit la
table SQL, appelle `core.build_sitrep()`, et publie le `.docx` dans le workspace.

## Template

`data/templates/Template_SitRep_v3.docx` reste **éditable à la main** dans Word
tant que les marqueurs `[[...]]` sont conservés ; `render.py` les remplit
déterministement. Ne pas écraser un template retouché.

## En savoir plus

Voir **`CLAUDE.md`** (règles métier, schéma, conventions, pièges) et le skill
`.claude/skills/generate-sitrep/SKILL.md` (mode opératoire détaillé).
