# Pipeline `compute_indicators_mve_tdb`

Calcule et publie les indicateurs du **tableau de bord MVE** (17ᵉ épidémie Ebola,
COUSP-RDC) dans la base du workspace OpenHexa, à partir de la table d'événements
de notification du tracker DHIS2.

## Flux

```
get_organisation_units ──┬─> build_case_data ───────────────┐
   (métadonnées DHIS2)    ├─> build_org_units("zone_sante") ─┤
                          └─> build_org_units("province") ───┤
                                                             ▼
            export_tables      ┐  4 tables, séquentiellement
            export_to_dataset  ┘  LLN → dataset
```

Le pipeline est un **DAG OpenHexa** : `build_case_data` et les deux
`build_org_units` ne dépendent que des unités d'organisation et tournent en
parallèle ; les deux branches d'export sont ensuite indépendantes.

Les 4 tables sont écrites **séquentiellement dans une seule tâche**, et non plus
par 4 tâches concurrentes : voir la note de performance, c'est ce qui évite de
faire tuer un worker par l'OOM killer (et donc de bloquer le run).

- `build_case_data` : lit `mve_notification_events` (fenêtre et data elements
  filtrés **en SQL**), pivote au grain enrôlement, extrait les attributs TEI, la
  fenêtre d'événements et le résumé labo, puis produit **deux sorties** — la LLN
  partagée (écrite en parquet) et les indicateurs au grain cas (drapeaux `is_*` :
  suspect, confirmé, décès, guéri…). Toute la chaîne lourde reste dans cette
  tâche unique : les tâches OpenHexa tournant dans des process séparés, faire
  circuler la table d'événements entre tâches coûte ~450 Mo de sérialisation par
  passage.
- `export_tables` : pour chaque axe temporel, agrège par (date, zone de santé,
  province, sexe, tranche d'âge) et écrit la table de staging ; puis publie la
  liste de ligne au grain cas (délais bornés, statut vital, variables labo / Ct).
  Chaque table est relue après écriture (`verifier_export`) pour comparer
  volumétrie et cardinalités à la source.
- `export_to_dataset` : publie la LLN dans une version du dataset OpenHexa,
  **uniquement si son contenu a changé** (empreinte SHA-256), accompagnée d'un
  JSON de métadonnées (fenêtre couverte, volumétrie, taux de remplissage par
  colonne, mention de confidentialité).

## Paramètres

| Paramètre | Type | Défaut | Rôle |
|---|---|---|---|
| `dhis_con` | DHIS2Connection | — | Connexion à l'instance tracker MVE |
| `date_min` | str (`YYYY-MM-DD`) | `2026-05-01` | Borne basse incluse sur `enrolled_at` |
| `date_max` | str (`YYYY-MM-DD`) | *(vide)* | Borne haute incluse ; vide = aucun plafond |
| `lln_dataset` | Dataset | `lln-tracker-mve` | Dataset où publier la LLN partagée |

> `lln_dataset` a un défaut : le paramètre est donc **toujours résolu** et
> l'export a lieu à chaque run. Le slug doit exister dans le workspace, sinon le
> run échoue à la validation des paramètres, avant toute exécution.

## Tables produites

Écrites en mode `replace` via **ADBC** (`COPY` Postgres en masse) ; déclarées
comme sorties du run (`add_database_output`).

| Table | Grain | Axe / contenu |
|---|---|---|
| `COD_MVE_Tracker_Agg` | agrégat | `date_notif` |
| `COD_MVE_Tracker_DDS_Agg` | agrégat | `date_debut_symptomes` |
| `COD_MVE_Tracker_Deces` | agrégat | `date_deces` (décès uniquement) |
| `COD_MVE_Tracker_Individu` | cas | liste de ligne nominative (`config.LLN_COLS`) |

## Dataset produit — LLN partagée

Publiée dans le dataset `lln_dataset` sous des **noms de fichiers stables** :

| Fichier | Contenu |
|---|---|
| `lln_mve_notifications.parquet` | LLN, **un enrôlement par ligne**, schéma `config.DATASET_LLN_COLS` |
| `lln_mve_notifications_metadata.json` | fenêtre couverte, volumétrie, taux de remplissage par colonne, colonnes vides, mention de confidentialité |

Le workspace consommateur peut donc écrire
`dataset.latest_version.get_file("lln_mve_notifications.parquet")` sans deviner
de nom horodaté ; c'est le **nom de version** qui porte l'horodatage lorsque la
numérotation `vN` n'est pas exploitable.

Conventions du fichier :

- **grain** : un enrôlement (`enrollment_id`). `occurred_at` n'entre pas dans le
  pivot (ce serait +43 % de lignes, un cas répété jusqu'à 15 fois) : la fenêtre
  d'événements est résumée par `date_premier_event`, `date_dernier_event`,
  `n_events` ;
- **géographie canonisée** : `province`, `zone_sante`, `aire_sante` sans préfixe
  ni suffixe DHIS2 (« it Ituri Province » → « Ituri »), avec les identifiants
  `*_id` correspondants — jointure directe avec des géométries possible ;
- **schéma stable** : une colonne mappée mais absente de la source est créée à
  NULL plutôt que supprimée, et l'ordre des colonnes est figé ;
- **confidentialité** : liste nominative comportant des quasi-identifiants et les
  coordonnées GPS du domicile (`gps_domicile`) → partage restreint aux personnes
  habilitées.

## Configuration

Les nappes (`AXES_EXPORT`, `LLN_TABLE`), la table source (`EVENTS_TABLE`), le
mapping des data elements DHIS2 (`DICO_DE_MAPPING`, `RENAME_MAP`, `DICO_TEI`,
`DATASET_LLN_MAPPING`, `DE_UTILES`), les tranches d'âge, les délais
(`DELAI_DEFS` / `DELAI_BORNES`) et les schémas publiés (`LLN_COLS` pour la table,
`DATASET_LLN_COLS` pour le dataset) sont centralisés dans `config.py`. Helpers
géo / âge / dataset dans `utils.py`.

Deux mappings de data elements coexistent : `DICO_DE_MAPPING` (vocabulaire
historique du tableau de bord, contrat figé) et `DATASET_LLN_MAPPING` (LLN
partagée, surensemble des mêmes DE). Depuis l'alignement des noms, les DE
communs aux deux dictionnaires portent le **même nom de colonne** dans le
dataset et dans `COD_MVE_Tracker_Individu`, sans que `DICO_DE_MAPPING`/
`RENAME_MAP` (donc la table et les tableaux de bord qui s'y connectent) n'aient
été modifiés. Deux exceptions volontaires, documentées dans `config.py` :
`resultat_labo` (`j6xabrRDJuo`, déjà identique au nom publié dans la table) et
`date_deces_final` (`x1aazi4fgKO`, laissé sous le nom brut du DE plutôt que
`date_deces` — qui dans la table désigne une date reconstruite à partir de
plusieurs DE, pas ce DE seul). Les ~23 DE propres à la LLN partagée (PEC hors
CTE, PCI, localisation du cas confirmé…), sans équivalent dans le vocabulaire du
tableau de bord, gardent leur nom `DATASET_LLN_MAPPING` d'origine.

⚠️ Ce renommage change le schéma du fichier `lln_mve_notifications.parquet` :
toute pipeline ou notebook d'un autre workspace qui lit ce dataset par nom de
colonne codé en dur (ex. `temperature`, `ct_ebov`, `resultat_labo` réutilisé
ailleurs sous l'ancien sens, `sympt_*`, `date_deces` pour ce DE précis…) doit
être mis à jour. Une nouvelle version du dataset sera publiée au prochain run
(le hash de contenu change), avec la fenêtre couverte inchangée.

## Exécution

OpenHexa exécute le pipeline depuis ce dossier (ajouté au `sys.path`, d'où les
imports « bare » `import config`, `from utils import …`). L'accès à la base
(`workspace.database_url`) et aux métadonnées DHIS2 n'est disponible que sur le
workspace : le pipeline n'est **pas exécutable en local**. Dépendances runtime
dans `requirements.txt` (dont `adbc-driver-postgresql`).

## Note de performance

Chaque tâche tourne dans un process séparé (`multiprocess`, contexte *spawn*) :
ses arguments et son résultat sont **picklés**. D'où deux choix structurants :

- l'ingestion est **une seule tâche** (`build_case_data`) : la table
  d'événements (~450 Mo picklés sur la volumétrie actuelle) ne traverse jamais
  une frontière de process ;
- la LLN circule **par son chemin de fichier**, pas par valeur ; seule la liste
  `indicators` est transmise aux deux branches d'export.

La lecture SQL est filtrée à la source (fenêtre `enrolled_at` + `DE_UTILES`) :
≈ 50 % de lignes en moins, sans perdre d'enrôlement.

### La géométrie répétée par ligne, et l'OOM qu'elle a provoqué

`coordinates_zs` / `coordinates_province` répètent le **même anneau de polygone
à chaque ligne**. Mesuré en base sur `COD_MVE_Tracker_Individu` (28 025 lignes) :

| Colonne | Texte total | Anneaux distincts |
|---|---|---|
| `coordinates_zs` | 164 Mo | 118 |
| `coordinates_province` | 240 Mo | 9 |

Soit **~400 Mo de JSON par copie, dont 99,6 % de duplication**. Côté pandas le
coût reste faible (les lignes partagent les mêmes objets `str`), mais chaque
conversion en Arrow, puis le tampon `COPY` d'ADBC, matérialisent ces 400 Mo. Avec
4 branches d'export concurrentes, le pic atteignait plusieurs Go : le 17/08 le
worker `export_individu` a été **tué par l'OOM killer**, ce qui laisse le run
bloqué indéfiniment (un process mort ne rend jamais son résultat à
`Pool.apply_async`, et le parent l'attend jusqu'au timeout).

Parade appliquée, sans toucher au schéma lu par le dashboard : **exports
séquentiels**, donc le pic mémoire d'une seule table à la fois (÷4).

⚠️ Une écriture **par tranches** avait aussi été tentée (découpage du DataFrame
pandas puis conversion tranche par tranche, avec `astype("string")` pour figer le
schéma). Elle divisait bien le pic Arrow par 5,6 mais a **corrompu silencieusement
les données** : `write_database` passe le DataFrame polars à ADBC via PyCapsule
(`data = self` dès `adbc_driver_manager >= 1.6`), donc son layout de chaînes
natif ; sur une colonne adossée à pyarrow et *tranchée*, le pilote a lu de
mauvais offsets et écrit des `zone_sante` recomposées à partir des octets voisins
(`aAdiKomandaLolw`, `aAruLayboNizi`…), 846 valeurs distinctes au lieu de 133.
Ne pas réintroduire de découpage ni de conversion pyarrow en amont sans vérifier
la sortie : la conversion doit partir de colonnes `object`, pour que polars
construise lui-même ses tampons.

**`verifier_export`** relit donc chaque table après écriture et compare
volumétrie et cardinalités (`province`, `zone_sante`, `aire_sante`,
`numero_epid`, `tranche_age`) à la source : une corruption de ce type échoue
désormais le run au lieu de passer inaperçue.

La vraie correction reste d'**externaliser la géométrie en table de référence**
(province / zone de santé → anneau, ~127 lignes) : ~400 Mo deviendraient ~800 Ko
par table. Non appliquée, car le dashboard devrait alors joindre cette table —
c'est un changement de contrat à valider avec son propriétaire.

Enfin, toutes les tâches sont encapsulées par `tache_robuste` : une exception
native (Polars, ADBC) devient une `RuntimeError` picklable, sinon elle ne remonte
pas au process parent et le run reste « running » jusqu'au timeout.
