"""Pipeline OpenHexa du tableau de bord MVE : orchestration des tâches.

Ce fichier ne contient que la déclaration du pipeline, ses paramètres et ses
tâches ; la logique métier vit dans les modules :

- extraction/ : lecture SQL des événements tracker (notifications, contacts)
- lln/        : listes linéaires publiées dans le dataset LLN
- indicateurs/: indicateurs au grain cas et agrégats du tableau de bord
- geo.py      : unités d'organisation et référentiel géographique
- export/     : écriture en base et publication dans les datasets
"""

from __future__ import annotations

import functools
import json
import traceback
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import config
import polars as pl
from export.base_de_donnees import export_to_database
from export.dataset import publier_version, publish_data_to_dataset, save_data_for_dataset
from extraction.contacts import load_contacts_events
from extraction.notifications import (
    build_event_dates,
    build_lab_summary,
    extract_tei_attributes,
    load_notification_events,
    pivot_enrollments,
)
from geo import build_geo_referentiel, prepare_org_units
from indicateurs.agregats import aggregate_indicators
from indicateurs.cas import build_line_list_individu, compute_indicators, consolidate_line_list
from lln.commun import build_lln_metadata, write_lln_parquet
from lln.contacts import build_contacts_line_list
from lln.notifications import build_line_list
from openhexa.sdk import Dataset, DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2, dataframe
from utils import last_analytics_update

# Ticket: https://bluesquare.atlassian.net/browse/PATHEOC-419

CaseData = dict[str, object]


def tache_robuste(fonction: Callable) -> Callable:
    """Convertit toute exception d'une tâche en RuntimeError explicite.

    Les tâches OpenHexa tournent dans des process séparés : une exception non
    picklable (erreur native Polars/ADBC) ne remonte pas au parent, qui attend
    alors un résultat qui n'arrivera jamais - le run reste « running » jusqu'au
    timeout. On journalise la trace et on relance un type standard.

    Returns:
        La fonction encapsulée.
    """

    @functools.wraps(fonction)
    def _tache(*args: object, **kwargs: object) -> object:
        try:
            return fonction(*args, **kwargs)
        except Exception as exc:
            current_run.log_error(
                f"Échec de la tâche « {fonction.__name__} » : {type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc(limit=8)}"
            )
            raise RuntimeError(f"{fonction.__name__}: {type(exc).__name__}: {exc}") from None

    return _tache


@pipeline("compute_indicators_mve_tdb")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    name="Connexion DHIS2",
    help="Connexion à l'instance tracker MVE",
    required=True,
)
@parameter(
    "date_min",
    type=str,
    name="Date de début (incluse)",
    help="Borne basse sur enrolled_at, au format YYYY-MM-DD.",
    default="2026-05-01",
)
@parameter(
    "date_max",
    type=str,
    name="Date de fin (incluse)",
    help="Borne haute sur enrolled_at (YYYY-MM-DD). Laisser vide pour aucun plafond.",
    required=False,
)
@parameter(
    "lln_dataset",
    type=Dataset,
    name="Dataset LLN Tracker MVE",
    help="Dataset de staging pour la liste lineaire nominative (LLN).",
    required=False,
    default="lln-tracker-mve",
)
def compute_indicators_mve_tdb(
    dhis_con: DHIS2Connection,
    date_min: str,
    date_max: str | None = None,
    lln_dataset: Dataset | None = None,
) -> None:
    """Calcule et publie les indicateurs du tableau de bord MVE."""
    fenetre_min = date.fromisoformat(date_min)
    fenetre_max = date.fromisoformat(date_max) if date_max else None

    tracker = DHIS2(dhis_con, Path(workspace.files_path) / ".cache")
    last_analytics_run = last_analytics_update(tracker)

    borne_max = fenetre_max.isoformat() if fenetre_max else "aucune"
    current_run.log_info(
        f"Fenêtre d'analyse sur enrolled_at : {fenetre_min.isoformat()} → {borne_max}."
    )
    borne_analytics = (
        last_analytics_run.isoformat() if last_analytics_run else "aucune (tables jamais générées)"
    )
    current_run.log_info(f"Borne analytics sur created_at (exclue) : {borne_analytics}.")

    db_url = workspace.database_url
    org_units = get_organisation_units(tracker)

    case_data = build_case_data(org_units, fenetre_min, fenetre_max, db_url, last_analytics_run)
    contacts_data = build_contacts_data(org_units, db_url)
    ou_zone_sante = build_org_units(org_units, "zone_sante")
    ou_aire_sante = build_org_units(org_units, "aire_sante")
    ou_provinces = build_org_units(org_units, "province")

    export_tables(case_data, ou_zone_sante, ou_aire_sante, ou_provinces, db_url)

    export_to_dataset(case_data, contacts_data, lln_dataset)


@compute_indicators_mve_tdb.task
@tache_robuste
def get_organisation_units(tracker: DHIS2) -> pl.DataFrame:
    """Récupère les unités d'organisation DHIS2 (métadonnées + géométries).

    Args:
        tracker: instance DHIS2 utilisée pour accéder aux métadonnées.

    Returns:
        DataFrame contenant les unités d'organisation.
    """
    return dataframe.get_organisation_units(tracker)


@compute_indicators_mve_tdb.task
@tache_robuste
def build_case_data(
    org_units: pl.DataFrame,
    date_min: date,
    date_max: date | None,
    db_url: str,
    last_analytics_run: datetime | None = None,
) -> CaseData:
    """Ingère les événements et construit les deux sorties, en une seule tâche.

    Toute la chaîne lourde (lecture SQL, pivot au grain enrôlement, attributs
    TEI, résumé labo) reste **dans cette tâche** : les tâches OpenHexa tournant
    dans des process séparés, faire circuler la table d'événements entre
    plusieurs tâches coûte plusieurs centaines de Mo de sérialisation par
    passage. Seuls les résultats compacts sont renvoyés.

    Args:
        org_units: Unités d'organisation (jointure géographique).
        date_min: Borne basse incluse sur enrolled_at.
        date_max: Borne haute incluse sur enrolled_at, ou None.
        db_url: URI de connexion à la base du workspace.
        last_analytics_run: Date de la dernière exécution des analyses, ou None.

    Returns:
        Les indicateurs au grain cas, le chemin du parquet LLN et ses métadonnées.
    """
    events = load_notification_events(db_url, date_min, date_max, last_analytics_run)
    tei = extract_tei_attributes(events)
    enrollments = pivot_enrollments(events, org_units)
    event_dates = build_event_dates(events)
    lab_summary = build_lab_summary(events)

    # Sortie 1 - LLN partagée (dataset OpenHexa), schéma config.DATASET_LLN_COLS
    lln = build_line_list(enrollments, tei, org_units, lab_summary, event_dates)
    horodatage = datetime.now(UTC)
    lln_path = write_lln_parquet(lln)
    metadata = build_lln_metadata(lln, date_min, date_max, horodatage)

    # Sortie 2 - indicateurs au grain cas (tables du tableau de bord)
    line_list = consolidate_line_list(enrollments, tei, lab_summary)
    indicators = compute_indicators(line_list)

    return {
        "indicators": indicators,
        "lln_path": str(lln_path),
        "lln_meta_file": config.LLN_DATASET_META,
        "lln_metadata": metadata,
        "horodatage": horodatage.strftime("%Y%m%d-%H%M"),
    }


@compute_indicators_mve_tdb.task
@tache_robuste
def build_contacts_data(org_units: pl.DataFrame, db_url: str) -> CaseData:
    """Construit et publie la LLN du suivi des contacts (programme tracker distinct).

    Indépendante de build_case_data() (autre table source, extraite sans
    fenêtre de dates) : elle tourne dans sa propre tâche, écrit la table
    config.CONTACTS_TABLE et ne renvoie que le chemin du parquet et ses
    métadonnées.

    Args:
        org_units: Unités d'organisation (jointure géographique).
        db_url: URI de connexion à la base du workspace.

    Returns:
        Le chemin du parquet LLN contacts et ses métadonnées.
    """
    events, tei = load_contacts_events(db_url)
    lln = build_contacts_line_list(events, tei, org_units)
    export_to_database(lln, config.CONTACTS_TABLE, db_url)
    lln_path = write_lln_parquet(lln, config.LLN_CONTACTS_FILE)
    metadata = build_lln_metadata(
        lln,
        None,
        None,
        datetime.now(UTC),
        source=(
            f"public.{config.CONTACTS_EVENTS_TABLE} "
            "(événements du tracker de suivi des contacts MVE, format long)"
        ),
        grain="un suivi journalier de contact (event_id) par ligne",
        confidentialite=(
            "Liste linéaire nominative des contacts. Contient des quasi-identifiants "
            "(identifiant contact, numéro Epid du cas source, âge, sexe, village) et "
            "des coordonnées GPS : diffusion restreinte aux personnes habilitées. "
            "Nom et téléphone du contact non publiés."
        ),
    )
    return {
        "lln_path": str(lln_path),
        "lln_meta_file": config.LLN_CONTACTS_META,
        "lln_metadata": metadata,
    }


@compute_indicators_mve_tdb.task
@tache_robuste
def export_tables(
    case_data: CaseData,
    ou_zone_sante: pl.DataFrame,
    ou_aire_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
    db_url: str,
) -> None:
    """Publie les tables du tableau de bord, **séquentiellement**.

    Il y avait auparavant une tâche par table, donc quatre process concurrents.
    Chacun reconstruit les anneaux de géométrie répétés à chaque ligne (~400 Mo
    de texte pour 127 anneaux distincts) puis les convertit en Arrow : plusieurs
    Go simultanés, et le worker le plus gros finit tué par l'OOM killer - ce qui
    laisse le run bloqué, un process mort ne rendant jamais son résultat. En
    séquentiel, le pic mémoire est celui d'une seule table.

    Args:
        case_data: Charge utile issue de build_case_data().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_aire_sante: Unités d'organisation aire de santé (référentiel complet),
            pour le référentiel géographique (province, zone_sante, aire_sante)
            publié séparément — indépendant du grain daté des 4 tables agrégées.
        ou_provinces: Unités d'organisation province (coordonnées).
        db_url: URI de connexion à la base du workspace.
    """
    indicators = case_data["indicators"]
    file_paths = []
    for colonne_date, table_name in config.AXES_EXPORT:
        agg = aggregate_indicators(indicators, ou_zone_sante, ou_provinces, colonne_date)  # type: ignore
        export_to_database(agg, table_name, db_url)
        file_paths.extend(
            save_data_for_dataset(agg, table_name, Path("/home/jovyan/tmp/mve_tracker_tables"))
        )
        del agg

    individu = build_line_list_individu(indicators, ou_zone_sante, ou_provinces)  # type: ignore
    export_to_database(individu, config.LLN_TABLE, db_url)

    geo_referentiel = build_geo_referentiel(ou_aire_sante)
    file_paths.extend(
        save_data_for_dataset(
            geo_referentiel,
            config.GEO_REFERENTIEL_TABLE,
            Path("/home/jovyan/tmp/mve_tracker_tables"),
        )
    )

    publish_data_to_dataset(
        file_paths=file_paths,
        dataset_id=config.MVE_TRACKER_DATASET_ID,
    )


@compute_indicators_mve_tdb.task
@tache_robuste
def build_org_units(
    org_units: pl.DataFrame,
    niveau: Literal["province", "zone_sante", "aire_sante"],
) -> pl.DataFrame:
    """Prépare les unités d'organisation d'un niveau donné (cf. geo.prepare_org_units).

    Returns:
        Les unités du niveau demandé, avec geo_hierarchie et coordinates (JSON).
    """
    return prepare_org_units(org_units, niveau)


@compute_indicators_mve_tdb.task
@tache_robuste
def export_to_dataset(
    case_data: CaseData,
    contacts_data: CaseData,
    dataset: Dataset | None,
) -> None:
    """Publie les LLN (notifications et contacts) dans une même version du dataset.

    Une version de dataset OpenHexa ne contient que les fichiers qu'on y
    dépose : les deux LLN et leurs métadonnées sont donc toujours publiées
    ensemble, pour que chaque version reste complète.

    Args:
        case_data: Charge utile issue de build_case_data().
        contacts_data: Charge utile issue de build_contacts_data().
        dataset: Dataset cible, ou None pour ne rien publier.
    """
    chemins = [Path(str(charge["lln_path"])) for charge in (case_data, contacts_data)]
    if dataset is None:
        current_run.log_info(
            f"Aucun dataset cible : LLN laissées dans « {', '.join(map(str, chemins))} »."
        )
        return

    chemins_meta = []
    for chemin, charge in zip(chemins, (case_data, contacts_data), strict=True):
        chemin_meta = chemin.with_name(str(charge["lln_meta_file"]))
        chemin_meta.write_text(
            json.dumps(charge["lln_metadata"], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        chemins_meta.append(chemin_meta)

    try:
        publier_version(dataset, chemins, chemins_meta, str(case_data["horodatage"]))
    finally:
        for chemin in (*chemins, *chemins_meta):
            chemin.unlink(missing_ok=True)


if __name__ == "__main__":
    compute_indicators_mve_tdb()
