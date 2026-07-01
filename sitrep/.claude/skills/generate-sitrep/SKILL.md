---
name: generate-sitrep
description: >-
  Générer, modifier ou étendre le SitRep MVE RDC (rapport Ebola Word produit
  depuis la table SQL d'événements DHIS2 mve_notification_events du workspace
  OpenHexa). Utiliser dès qu'on demande de produire le rapport pour une
  date/période, d'ajouter un indicateur, un graphique, une section, un tableau,
  d'ajuster les faits saillants/template, ou de toucher au pipeline / à la
  lecture des données.
---

# SitRep MVE RDC — opérations

Pipeline OpenHexa dans `code/generate_sitrep/`. Détails dans `CLAUDE.md` (racine)
— **ne le relire que si nécessaire**, ce skill suffit pour la plupart des tâches.

## Source de données (v3)

- **Format long tracker** : table SQL **`mve_notification_events`** = 1 ligne par
  valeur de data element (`data_element_id, value, value_norm, enrolled_at,
  level_*_name` + attributs TEI). Les drapeaux `n_*` **sont dérivés**, pas dans la
  source.
- **Prod (OpenHexa)** : `data.build_definitive_data()` → lit SQL + pivot +
  indicateurs + `_clean`.
- **Dev/local** : `data.load_raw()` sur fichier (défaut `config.DEFAULT_CSV` =
  `data/extract_data_openhexa/mve_tracker_events_notifications.csv`). `load_raw`
  lit tout-Utf8, type les dates, et si `data_element_id` présent route vers
  `indicators.build_definitive_from_raw` (même pivot que la prod) ; sinon `_clean`
  (ancien schéma large).
- **Pivot/dérivation** : `data/indicators.py` (`build_pivot` au grain enrollment +
  `compute_indicators_mve_notifications` ; sémantique des DE/optionSets en tête du
  module). `loader._clean` normalise ensuite → schéma interne : `date_rapportage,
  date_notif, province, zone_sante, aire_sante, sexe_norm, tranche_age` + flags `n_*`.
- `data.load_from_db()` (= `_clean(_read_db())`) **ne pivote pas** → legacy/inutilisé.

## Démarrage rapide (toujours `uv run`, depuis la racine)

```bash
# Générer en local (sortie auto dans data/generate_files/)
PYTHONPATH=code/generate_sitrep uv run python -c \
  "from datetime import date; from core import build_sitrep; \
   build_sitrep(reporting_end=date(2026,6,15), period_days=1)"

uv run ruff check code/generate_sitrep/
```

Chemins auto-détectés (`config._resolve_layout`) : local = `data/`
(`extract_data_openhexa/`, `geometry/`, `templates/`, `generate_files/`) ;
OpenHexa = `<workspace>/pipelines/sitrep/…`. **Pas de `cli.py` ni `tests/`** sur
cette branche (retirés pour le push). Verrou Word `~$…` → fermer le `.docx`.

> **Déploiement** : publié dans le monorepo `BLSQ/openhexa-pipelines-drc-cousp`
> comme dossier top-level **`sitrep/`** (la racine devient `sitrep/`, la structure
> `code/generate_sitrep` + `data/` est préservée dessous). Commandes ci-dessus
> inchangées, à lancer depuis `sitrep/`. `compute_indicators_mve_tdb/` et
> `dhis2_tracker_extract/` y sont des pipelines top-level séparés.

## Où agir selon la tâche (éditer un seul module en général)

| Demande | Fichier | Geste |
|---|---|---|
| **Définition** d'un drapeau `n_*` (sémantique DE/optionSet) | `data/indicators.py` | `compute_indicators_mve_notifications` (expr 0/1 par enrollment) |
| Nouvel **indicateur / KPI / colonne** (agrégation) | `data/metrics.py` (`compute`) + `data/model.py` | calcul polars (sommes de flags) + champ `SitRepData` |
| **Faits saillants « à date »** | `reporting/highlights.py` | `build_highlights` ; nombres via `utils.numbers.spell_fr` |
| **Courbe épi / pyramide** | `reporting/charts.py` | fonction + `build_all` |
| **Carte** (province / ZS) | `reporting/zone_map.py` | `province_situation_map` / `zone_situation_map` |
| **Tableau / image / section** dans le doc | `reporting/render.py` | filler + `_replace_marker("[[…]]", …)` ; tables via `utils.docx.table` (en-tête + Total **fond bleu** `DEEAF6`) |
| **Texte narratif** (actions par pilier, contexte…) | `narrative.yaml` | aucune logique ; clé sous `actions_reponse` |
| **Helper** (date, nombre, géo, docx) | `utils/{dates,numbers,geo,docx}.py` | factoriser ici (`set_cell(size=…)`, `set_cell_shading`) |
| **Lecture des données / nouveau champ source** | `data/loader.py` + `data/indicators.py` | `load_raw` (route long→pivot), `_clean` (rename, flags `METRICS`) ; pivot/dérivation dans `indicators` |
| **Paramètres / publication** | `pipeline.py` | 4 params : `reporting_end, period_days, dst_file?, dst_dataset?` |
| Constantes (table SQL, zones, âge/sexe) | `config.py` | `AGG_TABLE`, `PROVINCE_TOTAL_ZONES`, `AGE_BUCKETS`, `SEXE_CANONICAL` |

Flux : `build_definitive_data()`/`load_raw()` (pivot + indicateurs `indicators`)
→ `data.compute()` → `charts.build_all()` + `zone_map.*` → `render.render()`,
orchestrés par `core.build_sitrep()`.

## Marqueurs du template v3 (`render.py` les remplit)

`TABLEAU_I` distribution/province · `TABLEAU_II` par zone · `COURBE_EPI` ·
`PYRAMIDE` · `TABLEAU_CROISE` · `CARTE` (province + ZS) · `TABLEAU_III`
surveillance · `TABLEAU_IV` contacts (ND) · `TABLEAU_V` labo · `TABLEAU_VI`
prise en charge · `TABLEAU_VII` mouvement patients (ND) · `ACTIONS_*`
(coordination, prise_en_charge, pci_wash, crec, logistique, securite) ·
`FAITS_SAILLANTS, CONTEXTE, DEFIS, RECOMMANDATIONS, CONTACTS`. En-têtes
identité/KPI/distribution remplis **par position**.

## Règles métier (ne pas dévier)

- **Cumul vs période** : cumul = jusqu'à `reporting_end` (sur `date_rapportage`) ;
  période = fenêtre `period_days`. Courbe épi sur `date_notif`. Nouvelles zones =
  1er confirmé dans la fenêtre.
- **Faits saillants = situation à date, PAS de cumul.** YAML = uniquement le
  non-calculable (cérémonies, dons…).
- **Métriques** : confirmés = `n_confirmes` (class. CC), décès =
  `n_deces_confirmes`, guéris = `n_gueris` (issue PEC `GR`), actifs =
  `n_cas_actifs_stock`, isolement = `n_suspect_isole`/`n_confirme_isole`. Labo =
  `n_echantillons_*` (analysés = collectés − en cours). Stock/PEC sous garde-fou
  de complétude (→ `ND` si stage trop peu rempli).
- **Jamais inventer un champ absent → `ND`** : suspects en cours d'investigation,
  contacts (IV), mouvement patients (VII), létalité, lits.
- **Géo** : retirer préfixes `nk/sk/kn`, canoniser provinces ; cartes via
  `geometry/{provinces,zone_sante}.parquet`.

## Vérifier la sortie (pas de LibreOffice/poppler)

```python
import docx; d = docx.Document("data/generate_files/SitRep_….docx")
print([p.text for p in d.paragraphs if "[[" in p.text])  # doit être []
for t in d.tables: print(" | ".join(c.text for c in t.rows[0].cells))
```
Contrôler : zéro marqueur `[[` résiduel, somme des provinces == Total, période OK.

## Garde-fous

- Toujours finir par `ruff check`.
- **Ne pas écraser** un template retouché. `build_template.py` est un fallback v2
  obsolète (pas de marqueurs v3).
- **Définitions d'indicateurs** : éditer `data/indicators.py`, pas
  `loader`/`metrics`. `compute_indicators_mve_tdb/` est un pipeline dashboard
  distinct (source `.py` hors dépôt).
- Imports « bare » → `code/generate_sitrep/` doit être sur `sys.path`
  (`PYTHONPATH=…` en local). Ne pas retirer le shim `code/__init__.py`.
- Lecture SQL (`build_definitive_data`) testable seulement sur OpenHexa ; en local
  passer par `load_raw` (export tracker long → même pivot).
