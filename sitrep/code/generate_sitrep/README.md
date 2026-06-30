# generate_sitrep — Génération du SitRep MVE (.docx)

Pipeline OpenHexa qui produit le **SitRep MVE** (Rapport de Situation de la 17ᵉ
épidémie d'Ebola, COUSP-RDC) au format Word, à partir de la table d'événements
de notification du tracker DHIS2.

Voir aussi le `README.md` racine (vue d'ensemble des deux pipelines) et
`CLAUDE.md` (règles métier, schéma, pièges).

## Flux

```
load_from_db / load_raw / build_definitive_data   (data/)
        │  DataFrame nettoyé (schéma interne)
        ▼
   metrics.compute()  ──►  SitRepData              (data/)
        │
        ├──►  charts.build_all()   + zone_map.*     (reporting/) → PNG
        │
        └──►  render.render(template, marqueurs)    (reporting/) → .docx
                       ▲
              core.build_sitrep()  orchestre le tout
                       ▲
              pipeline.py  (entrée OpenHexa)
```

## Modules

| Module | Rôle |
|---|---|
| `pipeline.py` | Entrée OpenHexa. 4 paramètres : `reporting_end`, `period_days`, `dst_file?`, `dst_dataset?`. Lit la table SQL, appelle `core.build_sitrep()`, publie le `.docx`. |
| `core.py` | Orchestration `build_sitrep()` : données → visuels → rendu. |
| `config.py` | Constantes : `AGG_TABLE`, géo, `AGE_BUCKETS` / `SEXE_CANONICAL`, `PROVINCE_TOTAL_ZONES`, `SITREP_NUMBER`, `DE_UUID_SYMPTOMES` / `SIGNES`, template par défaut, `_resolve_layout` (local vs OpenHexa). |
| `data/loader.py` | `load_from_db` (SQL) / `load_raw` (fichier) + `_clean` (renommage, dates, canonisation géo, sexe/âge, flags). |
| `data/indicators.py` | `build_pivot` (enrollment-level) + `compute_indicators_mve_notifications` + `build_definitive_data` (lecture DB → pivot → indicateurs CUMUL/STOCK/PEC). |
| `data/metrics.py` | `compute()` → `SitRepData` : KPI, distribution par province/zone, pyramide, tableau croisé, surveillance, labo, prise en charge. |
| `data/model.py` | Dataclass `SitRepData` (porte aussi `raw`). |
| `reporting/charts.py` | Courbe épidémique, pyramide âge × sexe, combinaison de symptômes (cas confirmés). |
| `reporting/zone_map.py` | Cartes geopandas : `province_situation_map` (national) + `zone_situation_map` (zones de santé, zoom foyer). |
| `reporting/highlights.py` | Faits saillants **factuels « à date »**. |
| `reporting/render.py` | Remplit le template par **marqueurs** `[[...]]` (python-docx) ; langue forcée fr-FR ; en-têtes par position. |
| `reporting/narrative.py` + `narrative.yaml` | Sections narratives éditables (non calculables). |
| `reporting/build_template.py` | **Legacy** : fallback v2 (ne produit pas les marqueurs v3). |
| `utils/` | Helpers : `dates`, `numbers`, `geo`, `docx`. |

## Source de données

- **Production** : table SQL `mve_notification_events` du workspace OpenHexa
  (grain événement, flags `n_*` 0/1) → `data.load_from_db()`.
- **Dev / local** : extraction CSV/Parquet au même schéma → `data.load_raw()` ;
  ou extraction tracker au format long DHIS2 → `data.build_definitive_data()`
  (pivot enrollment-level, calcul des indicateurs).

`loader._clean` normalise vers le schéma interne : `date_rapportage` (=
`enrolled_at`), `date_notif`, `province`, `zone_sante`, `aire_sante`,
`sexe_norm`, `tranche_age` + flags `n_*`.

## Exécution locale

Imports « bare » : `code/generate_sitrep/` doit être sur le `sys.path`
(`PYTHONPATH=…` en local ; *Sources Root* dans l'IDE).

```bash
# Depuis la racine du dépôt — sortie dans data/generate_files/
PYTHONPATH=code/generate_sitrep uv run python -c \
  "from datetime import date; from core import build_sitrep; \
   build_sitrep(reporting_end=date(2026,5,31), period_days=1)"

uv run ruff check code/generate_sitrep/
uv run ruff format code/generate_sitrep/
```

> `load_from_db` / `build_definitive_data` ne sont testables que sur OpenHexa
> (accès `workspace.database_url`) ; en local, passer par `load_raw` sur un
> fichier, ou monkeypatcher `data.indicators._read_db`.

## Règles métier (à respecter)

- **Cumul vs période** : cumul = `date_rapportage <= reporting_end` ; période =
  fenêtre `period_days` (officiel = 1 jour). Courbe épi sur `date_notif`.
- **Faits saillants = situation « à date »**, jamais de cumul ; le YAML ne porte
  que le non-calculable.
- **Métriques** : décès confirmés = `n_deces_confirmes` ; actifs =
  `n_cas_actifs_stock` ; isolement = `n_isole_stock` / `n_confirme_isole`.
- **Jamais inventer un champ absent → `ND`** (contacts, mouvement patients,
  létalité, lits).

## Template à marqueurs

`data/templates/Template_SitRep_v3.docx` (local) reste **éditable à la main**
dans Word tant que les marqueurs `[[...]]` sont conservés ; `render.py` les
remplit déterministement. Marqueurs : `TITRE_NUMERO`, `FAITS_SAILLANTS`,
`CONTEXTE`, `TABLEAU_I`…`TABLEAU_VII`, `COURBE_EPI`, `COURBE_EPI_SYMPTOME`,
`PYRAMIDE`, `TABLEAU_CROISE`, `CARTE`, `ACTIONS_*`, `DEFIS`, `RECOMMANDATIONS`,
`CONTACTS`. **Ne pas écraser** un template retouché à la main.

## Validation de la sortie (sans LibreOffice/poppler)

```python
import docx; d = docx.Document("data/generate_files/SitRep_….docx")
print([p.text for p in d.paragraphs if "[[" in p.text])   # doit être []
for t in d.tables: print(" | ".join(c.text for c in t.rows[0].cells))
```

Contrôler : zéro marqueur `[[` résiduel, somme des provinces == Total,
fenêtre de rapportage correcte.
