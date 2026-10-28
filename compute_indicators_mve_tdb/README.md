# Pipeline `compute_indicators_mve_tdb`

Calcule et publie les indicateurs du **tableau de bord MVE** (17ᵉ épidémie Ebola,
COUSP-RDC) dans la base du workspace OpenHexa, à partir de la table d'événements
de notification du tracker DHIS2.

## Flux

```
get_organisation_units ──┬─> build_case_data ───────────────┐
   (métadonnées DHIS2)    ├─> build_contacts_data ───────────┤
                          ├─> build_org_units("zone_sante") ─┤
                          └─> build_org_units("province") ───┤
                                                             ▼
            export_tables      ┐  4 tables, séquentiellement
            export_to_dataset  ┘  LLN notifications + contacts → dataset
```

Le pipeline est un **DAG OpenHexa** : `build_case_data` et les deux
`build_org_units` ne dépendent que des unités d'organisation et tournent en
parallèle ; les deux branches d'export sont ensuite indépendantes.

Les 4 tables sont écrites **séquentiellement dans une seule tâche**, et non plus
par 4 tâches concurrentes : voir la note de performance, c'est ce qui évite de
faire tuer un worker par l'OOM killer (et donc de bloquer le run).

- `build_case_data` : lit `mve_notification_events` (fenêtre filtrée
  **en SQL**, tous data elements confondus), pivote au grain enrôlement, extrait les attributs TEI, la
  fenêtre d'événements et le résumé labo, puis produit **deux sorties** — la LLN
  partagée (écrite en parquet) et les indicateurs au grain cas (drapeaux `is_*` :
  suspect, confirmé, décès, guéri…). Toute la chaîne lourde reste dans cette
  tâche unique : les tâches OpenHexa tournant dans des process séparés, faire
  circuler la table d'événements entre tâches coûte ~450 Mo de sérialisation par
  passage.
- `build_contacts_data` : lit `mve_contacts_events` (programme « Fiche
  d'identification et suivi individuel des contacts », **extraction complète,
  sans fenêtre de dates**, restreinte aux 10 data elements de `config.DICO_DE_MAPPING_CONTACTS`), pivote au **grain
  event** (un suivi journalier par ligne : le programme n'a qu'un stage), joint les
  attributs TEI du contact (lus à part, une ligne par TEI via `DISTINCT ON`),
  écrit la table `COD_MVE_Tracker_Contacts` puis la LLN contacts en parquet. Tâche indépendante et légère : elle tourne en
  parallèle de `build_case_data`.
- `export_tables` : pour chaque axe temporel, agrège par (date, zone de santé,
  province, sexe, tranche d'âge) et écrit la table de staging ; puis publie la
  liste de ligne au grain cas (délais bornés, statut vital, variables labo / Ct).
  Chaque table est relue après écriture (`verifier_export`) pour comparer
  volumétrie et cardinalités à la source.
- `export_to_dataset` : publie les deux LLN (notifications et contacts) dans
  **une même version** du dataset OpenHexa, **uniquement si le contenu d'un des
  parquets a changé** (empreinte SHA-256), chacune accompagnée d'un JSON de
  métadonnées (fenêtre couverte, volumétrie, taux de remplissage par
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
| `COD_MVE_Tracker_Contacts` | suivi journalier | liste linéaire du suivi des contacts (`config.DATASET_LLN_CONTACTS_COLS`) |

## Dataset produit — LLN partagée

Publiée dans le dataset `lln_dataset` sous des **noms de fichiers stables** :

| Fichier | Contenu |
|---|---|
| `lln_mve_notifications.parquet` | LLN, **un enrôlement par ligne**, schéma `config.DATASET_LLN_COLS` |
| `lln_mve_notifications_metadata.json` | fenêtre couverte, volumétrie, taux de remplissage par colonne, colonnes vides, mention de confidentialité |
| `lln_mve_contacts.parquet` | LLN du suivi des contacts, **un suivi journalier (event) par ligne**, schéma `config.DATASET_LLN_CONTACTS_COLS` (sans nom, téléphone ni coordonnées géographiques du contact), extraction complète sans fenêtre de dates |
| `lln_mve_contacts_metadata.json` | mêmes métadonnées, pour la LLN contacts |

Une version de dataset OpenHexa ne contient que les fichiers qui y sont déposés :
les quatre fichiers sont donc toujours publiés ensemble, chaque version reste
complète.

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

Champs ajoutés pour la classification des cas (note « Circuit du patient et
classification des cas ») : lien épidémiologique (`contact_connu`,
`contact_suivi`, `ma1/ma2/ma3_cas_ebola`, `participation_funerailles`,
`pf1/pf2_cas_ebola`), décès et confirmation (`date_deces_investigation`,
`date_confirmation_s6`), mobilisation sociale (`mobilisation_sociale_prevenue`),
issue et prélèvement PCI (`issue_cas_pci`, `date_deces_issue_pci`,
`date_sortie_pci`, `prelevement_effectue_pci`, `date_prelevement_pci`,
`resultat_prelevement_pci`). Données brutes uniquement ; aucun indicateur dérivé
ni table Superset modifiés.

### Drapeaux `is_*` et date de décès reconstruite

En fin de schéma (`config.COLS_LLN_FLAGS`), `compute_lln_flags` ajoute les mêmes
colonnes dérivées que la table `COD_MVE_Tracker_Individu`
(`compute_indicators`/`reconstruct_date_deces`), calculées à partir de plusieurs
data elements plutôt que reprises telles quelles :

- **drapeaux booléens** `is_alerte`, `is_alerte_valide`, `is_suspect`,
  `is_non_cas`, `is_confirme`, `is_preleve`, `is_recu`, `is_analyse`,
  `is_resultat_valide`, `is_deces`, `is_gueri`, `is_deces_confirme`,
  `is_deces_suspect`, `is_confirme_gueri`, `is_confirme_vivant`.
  `is_confirme` = `n_pos >= 1` ET alerte validée (`conclusion_alerte ==
  "Validée"`) — historique cumulé des tests, pas seulement le dernier résultat
  connu ; une définition qui peut diverger de `classification_finale_cas`
  (DHIS2), le pipeline loguant un avertissement quand les deux comptages
  divergent. `is_non_cas` = `n_neg >= 1` ET alerte validée ET NOT `is_confirme`.
  `is_suspect` = alerte validée ET NOT `is_confirme` ET NOT `is_non_cas` — les
  trois drapeaux sont mutuellement exclusifs et reposent uniquement sur
  l'historique cumulé des tests (`n_pos`/`n_neg`, cf. `build_lab_summary`), plus
  sur le dernier résultat connu. `is_resultat_valide` = `is_confirme` OU
  `is_non_cas`. `is_deces_suspect` = `is_deces` ET `is_suspect`.
  `is_confirme_vivant` = `is_confirme` ET NOT `is_deces` (un cas confirmé et
  guéri compte donc aussi comme vivant) ;
- **`date_deces`** : reconstruite par cascade de priorité — `date_deces_final`
  (saisie), sinon `date_deces_notification`, sinon proxy PCI
  (`date_deces_pci`), sinon proxy prélèvement (`date_prelevement` si décès
  constaté au prélèvement), sinon proxy `date_notification` — appliquée
  uniquement quand `is_deces` est vrai. Ce n'est donc pas nécessairement une
  date saisie telle quelle.

Aucun de ces noms ne recouvre une colonne déjà publiée plus haut dans le
fichier : un garde-fou dans `config.py` fait échouer le chargement du module si
un doublon de nom apparaissait un jour entre les blocs du schéma.

## Structure du code

`pipeline.py` ne contient que la déclaration du pipeline, ses paramètres et ses
tâches (orchestration) ; la logique métier est répartie par responsabilité :

| Module | Contenu |
|---|---|
| `pipeline.py` | `@pipeline`, paramètres, tâches OpenHexa, `tache_robuste` |
| `extraction/notifications.py` | lecture SQL de `mve_notification_events`, pivot au grain enrôlement, attributs TEI, fenêtre d'événements, résumé labo |
| `extraction/contacts.py` | lecture SQL de `mve_contacts_events` (événements + attributs TEI) |
| `lln/commun.py` | géographie canonisée (`joindre_geographie`), schéma publié, écriture parquet, métadonnées JSON |
| `lln/notifications.py` | LLN notifications et drapeaux `is_*` |
| `lln/contacts.py` | LLN du suivi des contacts |
| `indicateurs/cas.py` | indicateurs au grain cas, date de décès reconstruite, liste de ligne `COD_MVE_Tracker_Individu` |
| `indicateurs/agregats.py` | agrégats par axe temporel (dont `date_rapportage`) |
| `geo.py` | unités d'organisation par niveau (géométries) et référentiel géographique |
| `export/base_de_donnees.py` | écriture ADBC (repli SQLAlchemy) et contrôle post-écriture |
| `export/dataset.py` | publication des fichiers dans les datasets OpenHexa |
| `config.py` / `utils.py` | constantes et mappings / helpers transverses |

Ajouter une nouvelle source suit le même schéma : un module dans `extraction/`,
un module dans `lln/` (ou `indicateurs/`), puis une tâche dans `pipeline.py`.

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

⚠️ Ce renommage, ainsi que l'ajout des colonnes `is_*`/`date_deces` en fin de
schéma, change le schéma du fichier `lln_mve_notifications.parquet` : toute
pipeline ou notebook d'un autre workspace qui lit ce dataset par nom de colonne
codé en dur (ex. `temperature`, `ct_ebov`, `resultat_labo` réutilisé ailleurs
sous l'ancien sens, `sympt_*`, `date_deces` pour ce DE précis…) doit être mis à
jour. Une nouvelle version du dataset sera publiée au prochain run (le hash de
contenu change), avec la fenêtre couverte inchangée.

## Exécution

OpenHexa exécute le pipeline depuis ce dossier (ajouté au `sys.path`, d'où les
imports « bare » `import config`, `from utils import …`, `from lln.contacts
import …`). L'accès à la base
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

La lecture SQL n'est filtrée à la source que sur la fenêtre `enrolled_at` et la
borne `created_at` (dernière génération des analytics). Le filtre sur
`DE_UTILES` a été retiré : il réduisait d'environ 50 % le volume lu, mais
faisait perdre des données par rapport à une lecture complète de la table.

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
