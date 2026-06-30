# Pipeline `compute_indicators_mve_tdb`

Calcule et publie les indicateurs du **tableau de bord MVE** (17ᵉ épidémie Ebola,
COUSP-RDC) dans la base du workspace OpenHexa, à partir de la table d'événements
de notification du tracker DHIS2.

## Flux

```
get_organisation_units ──┬─> build_indicators ──────────────┐
   (métadonnées DHIS2)    ├─> build_org_units("zone_sante") ─┤
                          └─> build_org_units("province") ───┤
                                                             ▼
            export_aggregate(date_notif)            ┐
            export_aggregate(date_debut_symptomes)  ├─ branches parallèles
            export_aggregate(date_deces)            │  (agrégation + COPY)
            export_individu                         ┘
```

Le pipeline est un **DAG OpenHexa** : `build_indicators` et les deux
`build_org_units` ne dépendent que des unités d'organisation et tournent en
parallèle ; les **4 branches d'export** (3 axes d'agrégation + la liste de ligne
individuelle) sont indépendantes et exécutées concurremment.

- `build_indicators` : lit `mve_notification_events`, pivote au grain
  enrôlement, extrait les attributs TEI et le résumé labo, consolide la liste de
  ligne nominative puis dérive les drapeaux `is_*` (suspect, confirmé, décès,
  guéri…).
- `export_aggregate` : agrège par (date, zone de santé, province, sexe, tranche
  d'âge) sur un axe temporel et écrit la table de staging.
- `export_individu` : liste de ligne au grain cas (délais bornés, statut vital,
  variables labo / Ct).

## Paramètres

| Paramètre | Type | Défaut | Rôle |
|---|---|---|---|
| `dhis_con` | DHIS2Connection | — | Connexion à l'instance tracker MVE |
| `date_min` | str (`YYYY-MM-DD`) | `2026-05-01` | Borne basse incluse sur `enrolled_at` |
| `date_max` | str (`YYYY-MM-DD`) | *(vide)* | Borne haute incluse ; vide = aucun plafond |

## Tables produites

Écrites en mode `replace` via **ADBC** (`COPY` Postgres en masse) ; déclarées
comme sorties du run (`add_database_output`).

| Table | Grain | Axe / contenu |
|---|---|---|
| `COD_MVE_Tracker_Agg` | agrégat | `date_notif` |
| `COD_MVE_Tracker_DDS_Agg` | agrégat | `date_debut_symptomes` |
| `COD_MVE_Tracker_Deces` | agrégat | `date_deces` (décès uniquement) |
| `COD_MVE_Tracker_Individu` | cas | liste de ligne nominative (`config.LLN_COLS`) |

## Configuration

Les nappes (`AXES_EXPORT`, `LLN_TABLE`), le mapping des data elements DHIS2
(`DICO_DE_MAPPING`, `RENAME_MAP`, `DICO_TEI`), les tranches d'âge, les délais
(`DELAI_DEFS` / `DELAI_BORNES`) et le schéma publié (`LLN_COLS`) sont centralisés
dans `config.py`. Helpers géo / âge dans `utils.py`.

## Exécution

OpenHexa exécute le pipeline depuis ce dossier (ajouté au `sys.path`, d'où les
imports « bare » `import config`, `from utils import …`). L'accès à la base
(`workspace.database_url`) et aux métadonnées DHIS2 n'est disponible que sur le
workspace : le pipeline n'est **pas exécutable en local**. Dépendances runtime
dans `requirements.txt` (dont `adbc-driver-postgresql`).

## Note de performance

Chaque tâche tourne dans un process séparé : ses arguments sont **picklés**. La
liste `indicators` est transmise aux 4 branches d'export — gain attendu (les
`COPY` réseau se recouvrent) vs coût de sérialisation à arbitrer selon le volume.
La géométrie (`coordinates_*`) est répétée par ligne dans toutes les tables :
candidat à une externalisation en table de référence pour réduire le volume.
