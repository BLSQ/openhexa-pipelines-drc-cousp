# CLAUDE.md

Guidance pour travailler dans ce dépôt. Voir aussi le skill `generate-sitrep`
(`.claude/skills/generate-sitrep/SKILL.md`) pour le mode opératoire détaillé.

## Objectif du projet

Produire automatiquement le **SitRep MVE** (Rapport de Situation de la 17ᵉ
épidémie de Maladie à Virus Ebola, COUSP-RDC) au format Word, dans un pipeline
**OpenHexa**, à partir de la table d'événements de notification du tracker DHIS2.

- **Source** : table SQL **`mve_notification_events`** du workspace, au **format
  long du tracker** (une ligne par valeur de data element : `event_id,
  data_element_id, value, value_norm, enrolled_at, level_1..5_name` + attributs
  TEI en colonnes). Les drapeaux `n_*` **ne sont pas** dans la source : ils sont
  **dérivés** par `data/indicators.py` (pivot au grain enrollment + sémantique
  des optionSets).
- **Production (OpenHexa)** : `data.build_definitive_data()` lit la table SQL
  (`workspace.database_url`) puis pivote/dérive/nettoie.
- **Dev / local** : même logique sur **fichier** via `data.load_raw()` (par
  défaut `config.DEFAULT_CSV` =
  `data/extract_data_openhexa/mve_tracker_events_notifications.csv`, export
  tracker long). `load_raw` lit le CSV tout-Utf8 (évite le crash de typage, ex.
  téléphone), type les dates, et si `data_element_id` est présent route vers
  `indicators.build_definitive_from_raw` ; sinon `_clean` direct (ancien schéma
  large `n_*`, ex. `mve_notification_events_sample.csv`).
- `data.load_from_db()` (= `_clean(_read_db())`) **ne pivote pas** → désormais
  inutilisé par le pipeline (conservé pour une table déjà au schéma large).

Cible : la dernière version officielle du rapport — réf.
`data/docs SitRep/SitRep_MVE_RDC_N017_..._Revised_*.pdf` et
`data/extrait image SitRep/`. Template courant : **`Template_SitRep_v3.docx`**.

## Environnement & commandes

Projet **Python 3.13** géré avec **uv**. Ce pipeline est le dossier **`sitrep/`**
du monorepo `BLSQ/openhexa-pipelines-drc-cousp` : la racine de travail est donc
`sitrep/` (commandes ci-dessous à lancer depuis ce dossier). Les chemins
s'auto-détectent (`config._resolve_layout`) — `parents[2]` depuis
`code/generate_sitrep/config.py` résout bien `sitrep/`, `data/` étant sous
`sitrep/data/` :

- **local** → tout sous `data/` en sous-dossiers : `extract_data_openhexa/`
  (entrées), `geometry/` (parquet), `templates/`, `generate_files/` (sorties) ;
- **OpenHexa** → `<workspace>/pipelines/sitrep/{generated_files, geometry,
  template_docx}`.

```bash
# Générer en local (source = config.DEFAULT_CSV ; sortie dans data/generate_files/)
PYTHONPATH=code/generate_sitrep uv run python -c \
  "from datetime import date; from core import build_sitrep; \
   build_sitrep(reporting_end=date(2026,6,15), period_days=1)"

# Lint / format
uv run ruff check code/generate_sitrep/
uv run ruff format code/generate_sitrep/
```

> **Pas de `cli.py` ni de `tests/`** sur la branche de déploiement (retirés pour
> alléger le push OpenHexa). L'entrée OpenHexa est `pipeline.py`. Si la sortie
> `.docx` est ouverte dans Word (verrou `~$…`), fermer le fichier.

> ⚠️ **Câblage à finaliser** : contrairement aux pipelines voisins du monorepo
> (`pipeline.py` à la racine du dossier), l'entrée de ce pipeline est
> `sitrep/code/generate_sitrep/pipeline.py` (imports « bare » nécessitant
> `code/generate_sitrep` sur le `sys.path`). Pour le déploiement OpenHexa, prévoir
> un `sitrep/pipeline.py` d'amorçage qui ajoute ce dossier au `sys.path` et
> délègue à l'entrée existante.

## Pipeline OpenHexa — `pipeline.py`

**4 paramètres utilisateur** seulement :
`reporting_end` (fin de fenêtre, sur `enrolled_at`), `period_days`, `dst_file`
(optionnel), `dst_dataset` (optionnel). Le pipeline lit la table SQL via
`build_definitive_data()` (pivot + dérivation des indicateurs, nom dans
`config.AGG_TABLE`), appelle `core.build_sitrep(df=…)`, publie le `.docx` dans le
workspace (et le dataset si fourni). Template =
`config.DEFAULT_TEMPLATE` (sur OpenHexa : `template_docx/Template_SitRep.docx`,
qui doit contenir le **contenu v3**). `sitrep_number` = `config.SITREP_NUMBER`.

## Architecture — `code/generate_sitrep/`

Séparation **données → visuels → rendu**, orchestrée par `core.build_sitrep()`
(appelé par `pipeline.py`).

| Module | Rôle |
|---|---|
| `config.py` | Constantes : `AGG_TABLE`, géo, `AGE_BUCKETS`/`SEXE_CANONICAL`, `PROVINCE_TOTAL_ZONES`, `REPORTING_PERIOD_DAYS`, template par défaut, `_resolve_layout` |
| `utils/` | Helpers : `dates` (fr_date, period_label), `numbers` (spell_fr, pct), `geo` (préfixes, province canonique), `docx` (`set_cell` (param `size`), `set_cell_shading`, `para`, `bullet`, `table` (en-tête + Total sur fond bleu `HEADER_FILL`), marqueurs, norm) |
| `data/` | `indicators` (pivot enrollment + dérivation des drapeaux `n_*` : `build_pivot`, `compute_indicators_mve_notifications`, entrées `build_definitive_data` SQL / `build_definitive_from_raw` partagée), `loader` (`load_raw` fichier → route vers `indicators` si format long, sinon `_clean` ; `_read_db`/`load_from_db` legacy), `metrics.compute` → `model.SitRepData`. `__init__` ré-exporte `build_definitive_data, compute_indicators_mve_notifications, load_from_db, compute, SitRepData` |
| `reporting/charts.py` | Courbe épi + pyramide (matplotlib) |
| `reporting/zone_map.py` | Cartes geopandas : `province_situation_map` (national) + `zone_situation_map` (ZS lisible, marqueurs numérotés) |
| `reporting/highlights.py` | Faits saillants **factuels « à date »** |
| `reporting/render.py` | Remplit le template par **marqueurs** `[[...]]` (python-docx) ; langue forcée fr-FR |
| `reporting/narrative.py` + `narrative.yaml` | Sections narratives éditables |
| `core.py` | Orchestration `build_sitrep()` ; `pipeline.py` = entrée OpenHexa |
| `reporting/build_template.py` | **Legacy** : fallback v2 (ne produit pas les marqueurs v3) |

## Schéma & règles métier (à respecter)

- **Schéma source** (`mve_notification_events`, **format long tracker**) :
  `event_id, tracked_entity_id, enrollment_id, enrolled_at, created_at,
  data_element_id, value, value_norm, enrollment_org_unit, level_1..5_name` +
  attributs TEI en colonnes (`MVE-N-Sexe`, `MVE - Age(ans)`, `MVE - DDS …`,
  `MPOX-N-Date … notification`, `MVE - Numéro Epid …`).
- **Pivot & indicateurs** (`data/indicators.py`) : `build_pivot` pivote au grain
  **enrollment** (1 ligne/enrollment, `aggregate_function="last"` trié par
  `created_at`) ; `compute_indicators_mve_notifications` dérive les drapeaux
  `n_*` 0/1 depuis la sémantique des optionSets (DE documentés en tête du
  module) : classification `D6kduc7OZnS` (CC/CP/CS/NC), conclusion alerte
  `KhsBtTYkFZd` (VAL/INV/Enc), résultat labo `j6xabrRDJuo` (POS/NEG/INV), statuts
  prélèvement/final, devenir suspect (CTE/TCTE), issue PEC (GR/DCD/…). Un DE
  absent → colonne `None` typée (extraction partielle tolérée). **Garde-fou de
  complétude** : indicateurs de STOCK (`n_isole_stock, n_cas_actifs_stock`) et de
  PEC (`n_gueris, n_deces_pec, …`) neutralisés à `None`→`ND` si le stage est trop
  peu rempli (seuil 50 %) — mais `build_definitive_data` appelle avec
  `appliquer_garde_fou_stock=False` (valeurs 0/1, `_clean` met les manquants à 0).
- **`loader._clean`** : la **date de rapportage** = `enrolled_at` (sinon
  `date_report`, sinon repli sur `date_notification`). Renomme
  `level_2/3/4_name→province/zone_sante/aire_sante`,
  `date_notification→date_notif` ; parse les dates ; canonise la géo (retire
  préfixes `nk/sk/kn` **et** suffixes « Province » / « Zone de Santé » pour
  matcher la géométrie) ; normalise `sexe` → Masculin/Feminin/Inconnu ;
  bucketise `age` → `tranche_age` (9999 → « Inconnu ») ; complète les flags
  manquants à 0. `date_anomalies` **signale** (sans filtrer) les dates hors
  `config.DATE_PLAUSIBLE_MIN/MAX`.
- **Fenêtre** : « **cumul** » = `date_rapportage <= reporting_end` ;
  « **période** » = fenêtre `period_days` (défaut 2 ; version officielle = 1 jour).
  Courbe épidémique sur **`date_notif`**. « Nouvelles zones touchées » = 1er cas
  confirmé d'une zone tombant dans la fenêtre.
- **Métriques clés** (noms dérivés par `indicators`) : confirmés = `n_confirmes`
  (classification CC) ; décès parmi confirmés = `n_deces_confirmes` ; guéris =
  `n_gueris` (issue PEC `GR`) ; actifs = `n_cas_actifs_stock` ; isolement =
  `n_suspect_isole` (suspects) / `n_confirme_isole` (confirmés) ; labo =
  `n_echantillons_*` (analysés = collectés − en cours).
- **Jamais inventer un champ absent → `ND`** : « cas suspects en cours
  d'investigation », suivi des contacts (Tableau IV), mouvement des patients
  (Tableau VII), létalité hospitalière, lits CTE.
- **Carte = deux choroplèthes dédiées** (`zone_map`) : par province (national,
  `geometry/provinces.parquet`) + par zone de santé (zoom provinces touchées,
  `geometry/zone_sante.parquet`). Jointures sur province canonique / (province, zone).

## Template v3 (marqueurs)

`data/templates/Template_SitRep_v3.docx` (= `config.DEFAULT_TEMPLATE` en local)
reste **éditable à la main** dans Word tant que les marqueurs `[[...]]` sont
conservés ; `render.py` les remplit déterministement. Marqueurs attendus :
`TITRE_NUMERO, FAITS_SAILLANTS, CONTEXTE, TABLEAU_I` (distribution par province),
`TABLEAU_II` (par zone de santé), `COURBE_EPI, PYRAMIDE, TABLEAU_CROISE, CARTE,
ACTIONS_COORDINATION, TABLEAU_III` (surveillance), `TABLEAU_IV` (contacts, ND),
`TABLEAU_V` (labo), `ACTIONS_PCI_WASH, ACTIONS_PRISE_EN_CHARGE, TABLEAU_VI`
(prise en charge), `TABLEAU_VII` (mouvement patients, ND), `ACTIONS_CREC,
ACTIONS_LOGISTIQUE, ACTIONS_SECURITE, DEFIS, RECOMMANDATIONS, CONTACTS`. Les
tableaux d'en-tête (identité, bandeau KPI, distribution par province) sont
remplis **par position** (cf. `_fill_identity/_fill_kpi/_fill_province_rows`).
La table récap d'accueil affiche le décompte entre parenthèses (« Provinces
touchées (N) », « Zones de Santé touchées (N) ») et la répartition par zone avec
comptage par province (`• Ituri (22/36) : …`), en **police 12** (fonds conservés).
Les tableaux générés (`utils.docx.table`, Tableaux I–VII + croisé) ont
**en-tête et ligne Total sur fond bleu** `HEADER_FILL` (`DEEAF6`).

⚠️ **Ne pas écraser** un template retouché à la main. `build_template.py` est un
**fallback v2 obsolète** (ne crée pas les marqueurs v3) — ne pas l'utiliser pour
v3. Sur OpenHexa, vérifier que `template_docx/Template_SitRep.docx` est bien le v3.

## Conventions de code

- **Polars** uniquement (pas de pandas). Type hints, docstrings, fonctions
  courtes, commentaires en français.
- **Imports « bare »** enracinés à `code/generate_sitrep/` (`import config`,
  `from data import …`, `from reporting import …`) : ce dossier doit être sur le
  `sys.path` (OpenHexa le fournit en exécutant depuis le dossier du pipeline ;
  en local utiliser `PYTHONPATH=code/generate_sitrep`, et marquer ce dossier
  comme *Sources Root* dans l'IDE, sinon imports « rouges »).
- ⚠️ Le package racine `code` occulte le module stdlib `code` ; `code/__init__.py`
  **ré-exporte l'API stdlib** — ne pas le supprimer.
- Dépendances dans `pyproject.toml` ; `[tool.hatch.build.targets.wheel]
  packages = ["code"]`. Deps clés ajoutées : `connectorx`/`sqlalchemy` (lecture
  SQL), `adjustText` (cartes).

## Pièges connus

- Génération `.docx` impossible si le fichier cible est ouvert dans Word.
- Pas de LibreOffice/poppler : valider la sortie en extrayant texte/tables avec
  `python-docx` (zéro marqueur `[[` résiduel, somme des provinces == Total).
- La lecture SQL (`build_definitive_data` / `_read_db`) n'est testable que sur
  OpenHexa (accès `workspace.database_url`) ; en local, passer par `load_raw` sur
  un fichier (export tracker long → même pivot via `indicators`).
- Définitions d'indicateurs : modifier **`data/indicators.py`** (sémantique des
  DE / optionSets), pas `loader`/`metrics`. `compute_indicators_mve_tdb/` est un
  pipeline dashboard **distinct** (source `.py` hors dépôt).

## Git & monorepo

- **Monorepo OpenHexa** : ce pipeline est le dossier top-level **`sitrep/`** du
  dépôt `BLSQ/openhexa-pipelines-drc-cousp`. Import à plat : `code/generate_sitrep`
  + `data/` sous `sitrep/` ; géométries en `.parquet` (GeoJSON sources exclus) ;
  `requirements.txt` (convention monorepo) **+** `pyproject.toml`/`uv.lock`
  conservés pour le dev local.
- `compute_indicators_mve_tdb/` et `dhis2_tracker_extract/` sont des dossiers
  top-level **séparés** du même monorepo (pipelines distincts).
- Convention du monorepo : 1 dossier par pipeline, avec `pipeline.py` à sa racine
  et `requirements.txt` ; un seul `pyproject.toml` racine (config ruff partagée +
  `openhexa.sdk`).
- Branche par défaut : `main`. Commiter/pousser uniquement sur demande.
- Gitignore (sous `sitrep/`) : `data/generate_files/SitRep_MVE_RDC_*.docx`,
  `data/_sitrep_assets/`, `data/templates/*.bak.docx`, `~$*`, `.DS_Store`.
