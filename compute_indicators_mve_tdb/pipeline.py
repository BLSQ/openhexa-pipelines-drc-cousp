from __future__ import annotations

import functools
import json
import traceback
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import config
import numpy as np
import pandas as pd
import polars as pl
from adbc_driver_postgresql import dbapi as pgdbapi
from openhexa.sdk import Dataset, DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2, dataframe
from utils import (
    canoniser_geo_expr,
    compter_oui,
    in_dataset_version,
    nom_prochaine_version,
    parse_geo,
    tranche_age,
)

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


# level_* DHIS2 -> noms publiés dans la LLN partagée
GEO_RENAME = {
    "level_2_id": "province_id",
    "level_2_name": "province",
    "level_3_id": "zone_sante_id",
    "level_3_name": "zone_sante",
    "level_4_id": "aire_sante_id",
    "level_4_name": "aire_sante",
}

# Sources des drapeaux is_* (compute_lln_flags), avec leur type de repli si le
# data element n'a pas été collecté sur la fenêtre demandée (colonne alors
# absente de la LLN avant apply_dataset_schema).
FLAG_SOURCE_DTYPES: dict[str, pl.DataType] = {
    "conclusion_alerte": pl.String,
    "resultat_labo": pl.String,
    "lab_confirme": pl.Boolean,
    "nature_alerte": pl.String,
    "statut_final_patient": pl.String,
    "date_deces_final": pl.Date,
    "statut_patient_prelevement": pl.String,
    "date_deces_pci": pl.Date,
    "modalite_sortie_cte": pl.String,
    "date_prelevement": pl.Date,
    "date_reception_labo": pl.Date,
    "date_analyse_labo": pl.Date,
    "date_notification": pl.Datetime,
    "date_deces_notification": pl.Date,
    "classification_finale_cas": pl.String,
}


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

    borne_max = fenetre_max.isoformat() if fenetre_max else "aucune"
    current_run.log_info(
        f"Fenêtre d'analyse sur enrolled_at : {fenetre_min.isoformat()} → {borne_max}."
    )

    db_url = workspace.database_url
    org_units = get_organisation_units(tracker)

    case_data = build_case_data(org_units, fenetre_min, fenetre_max, db_url)
    ou_zone_sante = build_org_units(org_units, "zone_sante")
    ou_provinces = build_org_units(org_units, "province")

    export_tables(case_data, ou_zone_sante, ou_provinces, db_url)

    export_to_dataset(case_data, lln_dataset)


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

    Returns:
        Les indicateurs au grain cas, le chemin du parquet LLN et ses métadonnées.
    """
    events = load_notification_events(db_url, date_min, date_max)
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
        "lln_metadata": metadata,
        "horodatage": horodatage.strftime("%Y%m%d-%H%M"),
    }


@compute_indicators_mve_tdb.task
@tache_robuste
def export_tables(
    case_data: CaseData,
    ou_zone_sante: pl.DataFrame,
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
        ou_provinces: Unités d'organisation province (coordonnées).
        db_url: URI de connexion à la base du workspace.
    """
    indicators = case_data["indicators"]

    for colonne_date, table_name in config.AXES_EXPORT:
        agg = aggregate_indicators(indicators, ou_zone_sante, ou_provinces, colonne_date)  # type: ignore
        export_to_database(agg, table_name, db_url)
        del agg

    individu = build_line_list_individu(indicators, ou_zone_sante, ou_provinces)  # type: ignore
    export_to_database(individu, config.LLN_TABLE, db_url)


@compute_indicators_mve_tdb.task
@tache_robuste
def build_org_units(
    org_units: pl.DataFrame,
    niveau: Literal["province", "zone_sante"],
) -> pl.DataFrame:
    """Prépare les unités d'organisation d'un niveau donné (province ou zone de santé).

    Filtre sur le niveau hiérarchique, reconstruit la hiérarchie géographique et
    extrait l'anneau extérieur du polygone, sérialisé en JSON (jointure carto).

    Args:
        org_units: Unités d'organisation issues de la toolbox DHIS2.
        niveau: « province » (level 2) ou « zone_sante » (level 3).

    Returns:
        Les unités du niveau demandé, avec geo_hierarchie et coordinates (JSON).
    """
    if niveau == "zone_sante":
        level, cols_geo = 3, ["level_1_name", "level_2_name", "level_3_name"]
    else:
        level, cols_geo = 2, ["level_1_name", "level_2_name"]

    def _anneau_exterieur(geom: object) -> object:
        """Anneau extérieur du polygone (les ZS sont imbriquées d'un niveau de plus).

        Returns:
            La liste de coordonnées de l'anneau extérieur, ou None si absente.
        """
        if not isinstance(geom, str):
            return None
        coords = json.loads(geom)["coordinates"][0]
        return coords[0] if niveau == "zone_sante" else coords

    prepared = (
        org_units.filter((pl.col("level") == level) & pl.col("geometry").is_not_null())
        .with_columns(
            pl.concat_str(cols_geo, separator=" / ").alias("geo_hierarchie"),
            pl.col("geometry")
            .map_elements(_anneau_exterieur, return_dtype=pl.Object)
            .alias("coordinates"),
        )
        .with_columns(
            pl.col("coordinates")
            .map_elements(json.dumps, return_dtype=pl.Utf8)
            .alias("coordinates")
        )
    )
    current_run.log_info(
        f"Unités d'organisation « {niveau} » préparées : {prepared.height} géométries."
    )
    return prepared


@compute_indicators_mve_tdb.task
@tache_robuste
def export_to_dataset(case_data: CaseData, dataset: Dataset | None) -> None:
    """Publie la LLN (parquet + métadonnées) dans une version du dataset.

    Args:
        case_data: Charge utile issue de build_case_data().
        dataset: Dataset cible, ou None pour ne rien publier.
    """
    chemin = Path(str(case_data["lln_path"]))
    if dataset is None:
        current_run.log_info(f"Aucun dataset cible : LLN laissée dans « {chemin} ».")
        return

    chemin_meta = chemin.with_name(config.LLN_DATASET_META)
    chemin_meta.write_text(
        json.dumps(case_data["lln_metadata"], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    try:
        publier_version(dataset, chemin, chemin_meta, str(case_data["horodatage"]))
    finally:
        chemin.unlink(missing_ok=True)
        chemin_meta.unlink(missing_ok=True)


def publier_version(
    dataset: Dataset,
    chemin: Path,
    chemin_meta: Path,
    horodatage: str,
) -> None:
    """Crée une version du dataset et y dépose la LLN, si le contenu a changé.

    Args:
        dataset: Dataset cible.
        chemin: Parquet de la LLN.
        chemin_meta: JSON de métadonnées accompagnant la LLN.
        horodatage: Horodatage de repli pour le nom de version.
    """
    derniere = dataset.latest_version
    if derniere is not None and in_dataset_version(chemin, derniere):
        current_run.log_info(
            f"LLN inchangée depuis la version « {derniere.name} » du dataset "
            f"« {dataset.name} » : aucune nouvelle version créée."
        )
        return

    version = dataset.create_version(nom_prochaine_version(derniere, horodatage))
    version.add_file(chemin, chemin.name)
    version.add_file(chemin_meta, chemin_meta.name)
    current_run.log_info(
        f"LLN publiée dans le dataset « {dataset.name} », version « {version.name} » "
        f"({chemin.name} + {chemin_meta.name})."
    )


def load_notification_events(
    db_url: str,
    date_min: date,
    date_max: date | None,
    table_name: str = config.EVENTS_TABLE,
) -> pl.DataFrame:
    """Charge la table d'événements de notification MVE depuis le workspace.

    Args:
        db_url: URI de connexion à la base du workspace.
        date_min: Borne basse incluse sur enrolled_at.
        date_max: Borne haute incluse sur enrolled_at, ou None.
        table_name: Table source (format long du tracker).

    Returns:
        Les événements bruts (grain événement DHIS2), prêts à être pivotés.
    """
    colonnes = [
        "event_id",
        "tracked_entity_id",
        "enrollment_id",
        "enrollment_org_unit",
        "enrolled_at",
        "occurred_at",
        "created_at",
        "data_element_id",
        "value_norm",
        *config.DICO_TEI,
    ]
    projection = ", ".join(f'"{colonne}"' for colonne in colonnes)
    data_elements = ", ".join(f"'{de}'" for de in config.DE_UTILES)
    conditions = [
        f'"data_element_id" IN ({data_elements})',
        f""""enrolled_at" >= DATE '{date_min.isoformat()}'""",
    ]
    if date_max is not None:
        conditions.append(f""""enrolled_at" <= DATE '{date_max.isoformat()}'""")

    df = pl.read_database_uri(
        f'SELECT {projection} FROM "public"."{table_name}" WHERE {" AND ".join(conditions)}',
        uri=db_url,
    )
    df = df.rename(config.DICO_TEI).with_columns(
        pl.col("enrolled_at").cast(pl.Date, strict=False),
        pl.col("date_notification").cast(pl.Datetime, strict=False),
        pl.col("date_debut_symptomes").cast(pl.Date, strict=False),
    )
    current_run.log_info(f"Événements de notification chargés : {df.height} lignes ")
    return df


def pivot_enrollments(events: pl.DataFrame, org_units: pl.DataFrame) -> pl.DataFrame:
    """Pivote les événements tracker au grain ENROLLMENT (une ligne par enrôlement).

    Args:
        events: Événements bruts issus de load_notification_events().
        org_units: Unités d'organisation (noms de niveaux 1 à 4).

    Returns:
        Un DataFrame au grain enrôlement, une colonne par data element.
    """
    contexte = events.group_by(["enrollment_id", "tracked_entity_id", "enrolled_at"]).agg(
        pl.col("enrollment_org_unit")
        .sort_by(["occurred_at", "created_at", "event_id"], nulls_last=True)
        .last()
    )
    valeurs = events.sort(  # noqa: PD010
        ["tracked_entity_id", "enrollment_id", "occurred_at", "created_at", "event_id"],
        nulls_last=True,
    ).pivot(
        on="data_element_id",
        index="enrollment_id",
        values="value_norm",
        aggregate_function="last",
    )
    enrollments = contexte.join(valeurs, on="enrollment_id", how="left")
    enrollments = enrollments.join(
        org_units.select(
            ["id", "level_1_name", "level_2_name", "level_3_name", "level_4_name"]
        ).rename({"id": "enrollment_org_unit"}),
        on="enrollment_org_unit",
        how="left",
    )
    current_run.log_info(f"Pivot au grain enrôlement : {enrollments.height} enrôlements retenus.")
    return enrollments


def extract_tei_attributes(
    events: pl.DataFrame,
    colonnes: list[str] = config.EXPR_TEI,
) -> pl.DataFrame:
    """Extrait les attributs d'entité suivie (TEI), un enregistrement par TEI.

    Returns:
        Les attributs TEI dédoublonnés (dernière occurrence par tracked_entity_id).
    """
    tei = (
        events.select(colonnes)
        .sort("tracked_entity_id")
        .unique(subset=["tracked_entity_id"], keep="last")
    )
    current_run.log_info(f"Attributs TEI extraits : {tei.height} entités suivies.")
    return tei


def build_event_dates(events: pl.DataFrame) -> pl.DataFrame:
    """Résume la fenêtre d'événements de chaque enrôlement.

    Conserve l'information portée par ``occurred_at`` sans changer le grain :
    date du premier et du dernier événement, nombre d'événements.

    Args:
        events: Événements bruts issus de load_notification_events().

    Returns:
        Une ligne par enrôlement (date_premier_event, date_dernier_event, n_events).
    """
    return events.group_by("enrollment_id").agg(
        pl.col("occurred_at").min().cast(pl.Date, strict=False).alias("date_premier_event"),
        pl.col("occurred_at").max().cast(pl.Date, strict=False).alias("date_dernier_event"),
        pl.col("event_id").n_unique().alias("n_events"),
    )


def build_lab_summary(events: pl.DataFrame) -> pl.DataFrame:
    """Résume l'historique de laboratoire par enrôlement (data element « Résultat final MVE »).

    Agrège les tests successifs (Positif/Négatif/Invalide) : confirmation, dates
    clés, compteurs et drapeaux de réversion (positif puis négatif / invalide).
    La fenêtre temporelle est déjà appliquée par load_notification_events().

    Args:
        events: Événements bruts issus de load_notification_events().

    Returns:
        Un résumé labo, une ligne par enrôlement testé.
    """
    de_resultat = config.DICO_DE_MAPPING["resultat_final_mve"]

    lab_summary = (
        events.filter(pl.col("data_element_id") == de_resultat)
        .with_columns(
            pl.col("occurred_at").alias("event_dt"),
            (pl.col("value_norm") == "Positif").alias("is_pos"),
            (pl.col("value_norm") == "Négatif").alias("is_neg"),
            (pl.col("value_norm") == "Invalide").alias("is_inv"),
        )
        .sort(["enrollment_id", "event_dt", "created_at", "event_id"], nulls_last=True)
        .group_by(["enrollment_id", "enrolled_at", "tracked_entity_id"], maintain_order=True)
        .agg(
            # A déjà été positif au moins une fois
            pl.col("is_pos").any().alias("lab_confirme"),
            # Date du premier test positif
            pl.col("event_dt").filter(pl.col("is_pos")).min().alias("date_confirmation"),
            # Statut du dernier test connu
            pl.col("value_norm").last().alias("lab_resultat_courant"),
            # Date du dernier test
            pl.col("event_dt").max().alias("date_dernier_test"),
            # Compteurs de tests
            pl.len().alias("n_tests_labo"),
            pl.col("is_pos").sum().alias("n_pos"),
            pl.col("is_neg").sum().alias("n_neg"),
            pl.col("is_inv").sum().alias("n_inv"),
            # Positif puis négatif / invalide (réversion du statut)
            (pl.col("is_pos").any() & (pl.col("value_norm").last() == "Négatif")).alias(
                "flag_pos_puis_neg"
            ),
            (pl.col("is_pos").any() & (pl.col("value_norm").last() == "Invalide")).alias(
                "flag_pos_puis_inv"
            ),
        )
    )
    n_confirmes = int(lab_summary.get_column("lab_confirme").sum())
    current_run.log_info(
        f"Résumé labo : {lab_summary.height} enrôlements testés, dont {n_confirmes} confirmés."
    )
    return lab_summary


def compute_lln_flags(lln: pl.DataFrame) -> pl.DataFrame:
    """Ajoute à la LLN les drapeaux is_* et la date de décès reconstruite.

    Réplique, au grain enrôlement de la LLN, la même méthodologie que
    compute_indicators()/reconstruct_date_deces() (grain cas, table
    COD_MVE_Tracker_Individu) : des drapeaux booléens combinant plusieurs data
    elements (ex. is_confirme = lab_confirme ET conclusion_alerte == « Validée »
    — une définition qui peut différer de classification_finale_cas, DHIS2) et
    une date de décès reconstruite par cascade de priorité (date finale → date
    notifiée → proxy PCI → proxy prélèvement si décès au prélèvement → proxy
    date de notification). Colonnes ajoutées en fin de schéma
    (config.COLS_LLN_FLAGS) : aucune ne recouvre un nom déjà publié par
    ailleurs dans la LLN (cf. le garde-fou de doublons dans config.py).

    Args:
        lln: LLN après jointures TEI/événements/labo et normalisation des dates
            (colonnes ``date_*`` déjà castées en pl.Date).

    Returns:
        La LLN enrichie des colonnes is_* et date_deces.
    """
    manquantes = [c for c in FLAG_SOURCE_DTYPES if c not in lln.columns]
    if manquantes:
        current_run.log_warning(
            f"LLN : {len(manquantes)} data element(s) absent(s) sur la période, "
            f"traité(s) comme non renseigné(s) pour les drapeaux is_* "
            f"({', '.join(manquantes)})."
        )
        lln = lln.with_columns(
            pl.lit(None, dtype=FLAG_SOURCE_DTYPES[col]).alias(col) for col in manquantes
        )

    # fill_null(False) après chaque comparaison : Polars applique la logique de
    # Kleene (une comparaison à une valeur nulle donne « inconnu », pas faux),
    # alors que la méthodologie de référence (pandas, compute_indicators) traite
    # un data element non renseigné comme une absence de correspondance (False).
    # Sans ce garde-fou, un simple None ferait basculer un OU/ET entier à null.
    is_alerte_valide = pl.col("conclusion_alerte").eq("Validée").fill_null(False)
    is_valide = pl.col("resultat_labo").is_in(["Positif", "Négatif"]).fill_null(False)
    is_suspect = is_alerte_valide & ~is_valide
    is_confirme = pl.col("lab_confirme").fill_null(False) & is_alerte_valide
    is_deces = (
        pl.col("nature_alerte").eq("Décès").fill_null(False)
        | pl.col("statut_final_patient").eq("Décédé").fill_null(False)
        | pl.col("date_deces_final").is_not_null()
        | pl.col("statut_patient_prelevement").eq("Décédé").fill_null(False)
        | pl.col("date_deces_pci").is_not_null()
    )
    is_gueri = pl.col("modalite_sortie_cte").eq("Guéri(e)").fill_null(False)  # noqa: RUF001

    lln = lln.with_columns(
        pl.lit(True).alias("is_alerte"),
        is_alerte_valide.alias("is_alerte_valide"),
        (pl.col("date_prelevement").is_not_null() | pl.col("date_reception_labo").is_not_null()).alias(
            "is_preleve"
        ),
        pl.col("date_reception_labo").is_not_null().alias("is_recu"),
        pl.col("date_analyse_labo").is_not_null().alias("is_analyse"),
        is_valide.alias("is_valide"),
        is_suspect.alias("is_suspect"),
        is_confirme.alias("is_confirme"),
        is_deces.alias("is_deces"),
        is_gueri.alias("is_gueri"),
    )
    lln = lln.with_columns(
        (pl.col("is_deces") & pl.col("is_confirme")).alias("is_deces_confirme"),
        (pl.col("is_confirme") & pl.col("is_gueri")).alias("is_confirme_gueri"),
        (pl.col("is_confirme") & ~pl.col("is_deces") & ~pl.col("is_gueri")).alias("is_confirme_vivant"),
    )

    # Cascade de priorité : date finale saisie -> date notifiée -> proxy PCI ->
    # proxy prélèvement (si décès au prélèvement) -> proxy date de notification.
    date_deces = pl.col("date_deces_final").fill_null(pl.col("date_deces_notification"))
    proxy_pci = date_deces.is_null() & pl.col("is_deces") & pl.col("date_deces_pci").is_not_null()
    date_deces = pl.when(proxy_pci).then(pl.col("date_deces_pci")).otherwise(date_deces)
    proxy_prelev = (
        date_deces.is_null()
        & pl.col("is_deces")
        & pl.col("statut_patient_prelevement").eq("Décédé").fill_null(False)
        & pl.col("date_prelevement").is_not_null()
    )
    date_deces = pl.when(proxy_prelev).then(pl.col("date_prelevement")).otherwise(date_deces)
    proxy_notif = date_deces.is_null() & pl.col("is_deces") & pl.col("date_notification").is_not_null()
    date_deces = (
        pl.when(proxy_notif).then(pl.col("date_notification").cast(pl.Date)).otherwise(date_deces)
    )
    lln = lln.with_columns(date_deces.alias("date_deces"))

    n_confirme = int(lln.get_column("is_confirme").sum())
    n_classification = int(
        (lln.get_column("classification_finale_cas") == "Cas confirmé").fill_null(False).sum()
    )
    if n_confirme != n_classification:
        current_run.log_warning(
            f"LLN : écart de confirmation : {n_confirme} cas via labo + alerte validée contre "
            f"{n_classification} « Cas confirmé » selon la classification finale DHIS2."
        )
    current_run.log_info(
        f"LLN : drapeaux calculés — {n_confirme} confirmés, "
        f"{int(lln.get_column('is_deces').sum())} décès, "
        f"{int(lln.get_column('is_gueri').sum())} guéris."
    )
    return lln


def build_line_list(
    enrollments: pl.DataFrame,
    tei: pl.DataFrame,
    org_units: pl.DataFrame,
    lab_summary: pl.DataFrame,
    event_dates: pl.DataFrame,
) -> pl.DataFrame:
    """Construit la LLN publiée dans le dataset (un enrôlement par ligne).

    Renomme les data elements (config.DATASET_LLN_MAPPING), joint les attributs
    TEI, la fenêtre d'événements et le résumé labo, calcule les drapeaux is_* et
    la date de décès reconstruite (compute_lln_flags, même méthodologie que
    COD_MVE_Tracker_Individu), canonise la géographie puis applique le schéma
    publié config.DATASET_LLN_COLS.

    Args:
        enrollments: Pivot au grain enrôlement.
        tei: Attributs d'entité suivie.
        org_units: Unités d'organisation (hiérarchie géographique).
        lab_summary: Résumé de l'historique laboratoire.
        event_dates: Fenêtre d'événements par enrôlement.

    Returns:
        La LLN au schéma config.DATASET_LLN_COLS, triée de façon déterministe.
    """
    de_columns = [de for de in config.DATASET_LLN_MAPPING if de in enrollments.columns]
    lln = enrollments.select(
        [
            "enrollment_id",
            "tracked_entity_id",
            pl.col("enrollment_org_unit").alias("organisation_unit_id"),
            "enrolled_at",
            *de_columns,
        ]
    ).rename({de: nom for de, nom in config.DATASET_LLN_MAPPING.items() if de in de_columns})

    lln = lln.join(tei, on="tracked_entity_id", how="inner")
    lln = lln.join(event_dates, on="enrollment_id", how="left")

    lln = lln.join(
        lab_summary.drop("enrolled_at", "tracked_entity_id", "lab_resultat_courant"),
        on="enrollment_id",
        how="left",
    )

    cols_texte = [
        col for col in lln.columns if col.startswith("date_") and lln.schema[col] == pl.String
    ]
    lln = lln.with_columns(
        pl.col(cols_texte).str.strip_chars().str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False)
    ).with_columns(pl.col(["date_confirmation", "date_dernier_test"]).cast(pl.Date, strict=False))

    lln = compute_lln_flags(lln)

    # Géographie : libellés canonisés (« it Ituri Province » -> « Ituri »)
    lln = dataframe.join_object_names(df=lln, organisation_units=org_units)
    lln = lln.rename({brut: nom for brut, nom in GEO_RENAME.items() if brut in lln.columns})
    lln = lln.with_columns(
        [
            canoniser_geo_expr(niveau)
            for niveau in ("province", "zone_sante", "aire_sante")
            if niveau in lln.columns
        ]
    )

    lln = apply_dataset_schema(lln, config.DATASET_LLN_COLS)
    current_run.log_info(
        f"LLN partagée : {lln.height} enrôlements, "
        f"{lln['tracked_entity_id'].n_unique()} entités suivies, {lln.width} colonnes."
    )
    # Tri déterministe : garantit un parquet reproductible octet à octet, donc
    # une nouvelle version de dataset uniquement quand les données changent.
    return lln.sort(["enrolled_at", "enrollment_id"], nulls_last=True)


def apply_dataset_schema(df: pl.DataFrame, colonnes: list[str]) -> pl.DataFrame:
    """Applique le schéma publié : colonnes manquantes créées à NULL, ordre figé.

    Args:
        df: LLN construite depuis la source.
        colonnes: Schéma cible (config.DATASET_LLN_COLS).

    Returns:
        Le DataFrame restreint et ordonné selon ``colonnes``.
    """
    manquantes = [col for col in colonnes if col not in df.columns]
    if manquantes:
        current_run.log_warning(
            f"LLN : {len(manquantes)} colonne(s) absente(s) de la source, créée(s) à NULL "
            f"({', '.join(manquantes)})."
        )
        df = df.with_columns([pl.lit(None, dtype=pl.String).alias(col) for col in manquantes])
    return df.select(colonnes)


def write_lln_parquet(lln: pl.DataFrame) -> Path:
    """Écrit la LLN dans le dossier de travail du workspace, sous un nom stable.

    Args:
        lln: LLN au schéma publié.

    Returns:
        Le chemin du parquet écrit.
    """
    dossier = Path(workspace.files_path, *config.LLN_EXPORT_DIR.split("/"))
    dossier.mkdir(parents=True, exist_ok=True)
    chemin = dossier / config.LLN_DATASET_FILE
    lln.write_parquet(chemin)
    current_run.log_info(f"LLN écrite dans « {chemin} » ({chemin.stat().st_size / 1e6:.1f} Mo).")
    return chemin


def build_lln_metadata(
    lln: pl.DataFrame,
    date_min: date,
    date_max: date | None,
    horodatage: datetime,
) -> dict[str, object]:
    """Décrit la LLN publiée : périmètre, volumétrie, remplissage, confidentialité.

    Ce dictionnaire est publié en JSON à côté du parquet : sans lui, le workspace
    consommateur ne peut savoir ni quelle fenêtre est couverte, ni quelles
    colonnes sont vides faute de collecte.

    Args:
        lln: LLN au schéma publié.
        date_min: Borne basse de la fenêtre sur enrolled_at.
        date_max: Borne haute de la fenêtre, ou None.
        horodatage: Instant de génération (UTC).

    Returns:
        Les métadonnées du fichier, sérialisables en JSON.
    """
    total = lln.height
    non_nuls = {col: total - lln[col].null_count() for col in lln.columns}
    remplissage = {col: round(100 * n / total, 1) for col, n in non_nuls.items()} if total else {}
    # Compté sur les valeurs, pas sur le pourcentage arrondi : une colonne
    # remplie à 0,04 % arrondit à 0,0 sans être vide pour autant.
    vides = [col for col, n in non_nuls.items() if n == 0]
    if vides:
        current_run.log_warning(
            f"LLN : {len(vides)} colonne(s) entièrement vide(s) sur la période "
            f"({', '.join(vides)})."
        )
    return {
        "pipeline": "compute_indicators_mve_tdb",
        "genere_le": horodatage.isoformat(timespec="seconds"),
        "source": f"public.{config.EVENTS_TABLE} (événements du tracker MVE, format long)",
        "grain": "un enrôlement (enrollment_id) par ligne",
        "fenetre_enrolled_at": {
            "min": date_min.isoformat(),
            "max": date_max.isoformat() if date_max else None,
        },
        "lignes": total,
        "colonnes": lln.width,
        "entites_suivies": lln["tracked_entity_id"].n_unique(),
        "taux_remplissage_pct": remplissage,
        "colonnes_vides": vides,
        "confidentialite": (
            "Liste linéaire nominative. Contient des quasi-identifiants (numéro Epid, "
            "identifiant labo, âge, sexe, profession, aire de santé) et des coordonnées "
            "GPS du domicile : diffusion restreinte aux personnes habilitées."
        ),
    }


def consolidate_line_list(
    enrollments: pl.DataFrame,
    tei: pl.DataFrame,
    lab_summary: pl.DataFrame,
) -> pd.DataFrame:
    """Consolide la liste de ligne nominative (une ligne par cas).

    Joint le pivot enrôlement aux attributs TEI puis au résumé labo, renomme les
    data elements (identifiants DHIS2 → noms lisibles), reconstruit la
    hiérarchie géographique et crée à None les data elements jamais collectés.

    Returns:
        La liste de ligne consolidée (pandas), entrée du calcul d'indicateurs.
    """
    line_list = enrollments.join(tei, on="tracked_entity_id", how="left").with_columns(
        pl.concat_str(
            ["level_1_name", "level_2_name", "level_3_name", "level_4_name"],
            separator=" / ",
        ).alias("geo_hierarchie"),
    )

    # Identifiants DHIS2 → noms lisibles (uniquement les colonnes présentes)
    de_vers_nom = {
        de_id: nom for nom, de_id in config.DICO_DE_MAPPING.items() if de_id in line_list.columns
    }
    line_list = line_list.rename(de_vers_nom)

    # Data elements jamais collectés sur la période → colonnes vides
    de_absents = [nom for nom in config.DICO_DE_MAPPING if nom not in line_list.columns]
    line_list = line_list.with_columns(pl.lit(None).alias(nom) for nom in de_absents)

    line_list = line_list.select(["tracked_entity_id", *config.RENAME_MAP.values()])
    line_list = line_list.join(
        lab_summary.join(tei, on="tracked_entity_id", how="left").select(config.COLS_PRELEV),
        on="tracked_entity_id",
        how="left",
    )
    if de_absents:
        current_run.log_debug(
            f"Data elements absents de la période (créés vides) : {len(de_absents)}."
        )
    current_run.log_info(f"Liste de ligne consolidée : {line_list.height} cas.")
    return line_list.drop("tracked_entity_id").to_pandas()


def compute_indicators(line_list: pd.DataFrame) -> pd.DataFrame:
    """Calcule les indicateurs au grain cas à partir de la liste de ligne.

    Découpe la hiérarchie géographique, normalise les dates, dérive la semaine
    épidémiologique, la tranche d'âge et le sexe, puis les drapeaux booléens
    (alerte, suspect, confirmé, décès, guéri…) qui seront sommés à l'agrégation.

    Returns:
        La liste de ligne enrichie des colonnes dérivées et des drapeaux is_*.
    """
    geo = line_list["geo_hierarchie"].apply(parse_geo).apply(pd.Series)
    line_list = pd.concat([line_list, geo], axis=1)
    line_list["geo_hierarchie"] = line_list["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:3])
    )
    for cible, source in config.DATE_COLS.items():
        line_list[cible] = pd.to_datetime(line_list[source], errors="coerce")  # .dt.normalize()

    # Semaine épidémiologique ISO (recalculée depuis date_notif propre)
    iso = line_list["date_notif"].dt.isocalendar()
    line_list["semaine_epidemio"] = iso.year.astype(str) + "-S" + iso.week.astype(str).str.zfill(2)
    line_list["tranche_age"] = line_list.apply(tranche_age, axis=1)

    line_list["sexe_norm"] = (
        line_list["sexe"]
        .astype(str)
        .str.strip()
        .str.capitalize()
        .replace({"Nan": "Inconnu", "None": "Inconnu"})
    )

    line_list["is_alerte"] = True
    line_list["is_alerte_valide"] = line_list["conclusion_alerte"] == "Validée"
    line_list["is_preleve"] = (
        line_list["date_prelevement"].notna() | line_list["date_reception_labo"].notna()
    )
    line_list["is_recu"] = line_list["date_reception_labo"].notna()
    line_list["is_analyse"] = line_list["date_analyse_labo"].notna()
    line_list["is_valide"] = line_list["resultat_final_mve"].isin(["Positif", "Négatif"])
    line_list["is_suspect"] = line_list["is_alerte_valide"] & ~line_list["is_valide"]
    line_list["is_confirme"] = (line_list["lab_confirme"] == True) & (  # noqa: E712
        line_list["conclusion_alerte"] == "Validée"
    )

    line_list["is_deces"] = (
        (line_list["nature_alerte"] == "Décès")
        | (line_list["statut_final_patient"] == "Décédé")
        | (line_list["date_deces_final"].notna())
        | (line_list["statut_patient_prelevement"] == "Décédé")
        | (line_list["date_deces_pci"].notna())
    )
    line_list["is_deces_confirme"] = line_list["is_deces"] & line_list["is_confirme"]
    line_list["is_deces_suspect"] = line_list["is_deces"] & line_list["is_suspect"]
    line_list["is_suspect_lien_epi"] = line_list["is_suspect"] & (
        line_list["lien_epidemiologique"] == "Oui"
    )

    # Guérison : modalité de sortie du CTE (DE « Statut au moment de la sortie »).
    modalite_sortie = line_list.get("modalite_sortie_cte")
    line_list["is_gueri"] = modalite_sortie.eq("Guéri(e)") if modalite_sortie is not None else False
    line_list["is_confirme_gueri"] = line_list["is_confirme"] & line_list["is_gueri"]
    line_list["is_confirme_vivant"] = (
        line_list["is_confirme"] & ~line_list["is_deces"] & ~line_list["is_gueri"]
    )

    current_run.log_info(
        f"Indicateurs au grain cas : {len(line_list)} cas, "
        f"{int(line_list['is_confirme'].sum())} confirmés, "
        f"{int(line_list['is_deces_confirme'].sum())} décès confirmés, "
        f"{int(line_list['is_gueri'].sum())} guéris."
    )

    n_derive = int(line_list["is_confirme"].sum())
    n_classification = int((line_list["classification_finale_cas"] == "Cas confirmé").sum())
    if n_derive != n_classification:
        current_run.log_warning(
            f"Écart de confirmation : {n_derive} cas via labo + alerte validée contre "
            f"{n_classification} « Cas confirmé » selon la classification finale DHIS2."
        )
    return line_list


def reconstruct_date_deces(df: pd.DataFrame) -> pd.Series:
    """Reconstruit une date de décès unique par cas.

    Priorité aux dates réellement saisies : date de décès finale, sinon date
    notifiée du décès, sinon date de décès saisie en PCI. À défaut, proxys
    (date de prélèvement si patient décédé au prélèvement, sinon date de
    notification) lorsque le décès est avéré (is_deces).

    Args:
        df: Liste de ligne contenant les dates et drapeaux de décès.

    Returns:
        La série des dates de décès (NaT si non décédé ou date inconnue).
    """
    date_deces = df["date_deces_final"].fillna(df["date_deces_notification"])

    proxy_pci = date_deces.isna() & df["is_deces"] & df["date_deces_pci"].notna()
    date_deces = date_deces.mask(proxy_pci, df["date_deces_pci"])

    proxy_prelev = (
        date_deces.isna()
        & df["is_deces"]
        & (df["statut_patient_prelevement"] == "Décédé")
        & df["date_prelevement"].notna()
    )
    date_deces = date_deces.mask(proxy_prelev, df["date_prelevement"])

    proxy_notif = date_deces.isna() & df["is_deces"] & df["date_notif"].notna()
    return date_deces.mask(proxy_notif, df["date_notif"])


def build_line_list_individu(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
) -> pd.DataFrame:
    """Construit la liste de ligne nominative (grain cas) pour le tableau de bord.

    Reconstruit la date de décès, rattache les coordonnées carto (ZS et
    province), dérive les délais (en jours, bornés), le statut vital et les
    variables labo (résultat, valeurs Ct des positifs, classe Ct), puis restreint
    au schéma publié (config.LLN_COLS).

    Args:
        indicators: Liste de ligne enrichie issue de compute_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).

    Returns:
        La liste de ligne individuelle, une ligne par cas, colonnes LLN_COLS.
    """
    line_list = indicators.copy()
    line_list["date_deces"] = reconstruct_date_deces(line_list)

    # ── Coordonnées carto (ZS puis province) ─────────────────────────────────
    # NB : redondant (géométrie répétée par ligne) ; à externaliser plus tard.
    line_list = line_list.merge(
        ou_zone_sante.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_zs"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )
    line_list["geo_hierarchie"] = line_list["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:2])
    )
    line_list = line_list.merge(
        ou_provinces.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_province"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )

    # ── Délais (jours, float - bornés aux valeurs plausibles) ────────────────
    for nom, (col_fin, col_debut) in config.DELAI_DEFS.items():
        delai = (line_list[col_fin] - line_list[col_debut]).dt.total_seconds() / 86_400
        min_, max_ = config.DELAI_BORNES[nom]
        line_list[nom] = delai.where(delai.between(min_, max_), other=np.nan)

    # Statut vital (Décédé → Guéri → Vivant)
    line_list["statut_vital"] = np.where(
        line_list["is_deces"], "Décédé", np.where(line_list["is_gueri"], "Guéri", "Vivant")
    )

    # ── Labo : résultat + valeurs Ct (positifs uniquement) + classe Ct ───────
    line_list["resultat_labo"] = line_list["resultat_final_mve"]
    positif = line_list["is_confirme"].astype(bool)
    for col in ("valeur_ct_ebov", "valeur_ct_hec"):
        line_list[col] = pd.to_numeric(line_list[col], errors="coerce").where(positif)
    line_list["ct_ebov_classe"] = pd.cut(
        line_list["valeur_ct_ebov"],
        bins=[-np.inf, 18, 21, 24, 27, 30, 33, np.inf],
        labels=["<18", "18–21", "21–24", "24–27", "27–30", "30–33", ">33"],  # noqa: RUF001
        right=False,
    ).astype("object")

    # Schéma publié - reindex pour tolérer une colonne absente (créée à NULL
    # plutôt que de lever KeyError).
    manquantes = [c for c in config.LLN_COLS if c not in line_list.columns]
    if manquantes:
        current_run.log_warning(f"LLN : colonnes absentes créées à NULL : {manquantes}.")
    current_run.log_info(f"Liste de ligne individuelle : {len(line_list)} cas.")
    return line_list.reindex(columns=config.LLN_COLS)


def aggregate_indicators(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
    colonne_date: str,
) -> pd.DataFrame:
    """Agrège les indicateurs par date, géographie, sexe et tranche d'âge.

    Pour l'axe « date_deces », reconstruit d'abord une date de décès unique
    (date finale, sinon notification, sinon proxy prélèvement/notification) et
    restreint aux décès. Rattache enfin les coordonnées ZS et province.

    Args:
        indicators: Liste de ligne enrichie issue de compute_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).
        colonne_date: Axe temporel d'agrégation (date_notif, date_debut_symptomes
            ou date_deces).

    Returns:
        Les agrégats, une ligne par (date, ZS, province, sexe, tranche d'âge).
    """
    if colonne_date == "date_deces":
        indicators = indicators.copy()
        indicators["date_deces"] = reconstruct_date_deces(indicators)
        indicators = indicators[indicators["is_deces"]]

    group_keys = [
        colonne_date,
        "zone_sante",
        "province",
        "sexe_norm",
        "tranche_age",
        "geo_hierarchie",
    ]

    aggregated = (
        indicators.groupby(group_keys, dropna=False)
        .agg(
            n_alertes=("numero_epid", "nunique"),
            n_alertes_valides=("is_alerte_valide", "sum"),
            n_suspects=("is_suspect", "sum"),
            n_suspects_lien_epi=("is_suspect_lien_epi", "sum"),
            n_preleves=("is_preleve", "sum"),
            n_recus=("is_recu", "sum"),
            n_analyses=("is_analyse", "sum"),
            n_echantillons_valides=("is_valide", "sum"),
            n_confirmes=("is_confirme", "sum"),
            n_deces=("is_deces", "sum"),
            n_deces_suspects=("is_deces_suspect", "sum"),
            n_deces_confirmes=("is_deces_confirme", "sum"),
            n_gueri=("is_gueri", "sum"),
            # n_confirmes_deces == n_deces_confirmes (même drapeau, conservé pour le TDB)
            n_confirmes_deces=("is_deces_confirme", "sum"),
            n_confirmes_gueri=("is_confirme_gueri", "sum"),
            n_confirmes_vivants=("is_confirme_vivant", "sum"),
            # ── Signes cliniques ────────────────────────────────────────────────
            n_signe_fievre=("signe_fievre", compter_oui),
            n_signe_vomissements=("signe_nausees_vomissements", compter_oui),
            n_signe_diarrhees=("signe_diarrhees", compter_oui),
            n_signe_fatigue=("signe_fatigue", compter_oui),
            n_signe_cephalees=("signe_cephalees", compter_oui),
            n_signe_coma=("signe_coma", compter_oui),
            n_signe_confusion=("signe_confusion", compter_oui),
            n_signe_saignements=("signe_saignements", compter_oui),
            n_signe_saignement_gencives=("signe_saignement_gencives", compter_oui),
            n_signe_epistaxis=("signe_epistaxis", compter_oui),
            n_signe_melenas=("signe_melenas", compter_oui),
            n_signe_hemorragique=("signes_hemorragiques_maladie", compter_oui),
        )
        .reset_index()
        .sort_values([colonne_date, "province", "zone_sante"])
        .reset_index(drop=True)
    )

    aggregated = aggregated.merge(
        ou_zone_sante.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_zs"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )
    # Réduit la hiérarchie ZS → province pour rattacher les coordonnées province
    aggregated["geo_hierarchie"] = aggregated["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:2])
    )
    aggregated = aggregated.merge(
        ou_provinces.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_province"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    ).drop(columns="geo_hierarchie")
    current_run.log_info(f"Agrégation sur « {colonne_date} » : {len(aggregated)} lignes.")
    return aggregated


def ingerer_adbc(
    frame: pl.DataFrame,
    table_name: str,
    db_url: str,
    mode: Literal["append", "replace", "fail"],
) -> None:
    """Ingère un DataFrame via ADBC en forçant les chaînes en `large_string`.

    Args:
        frame: Table à publier.
        table_name: Table de staging cible.
        db_url: URI de connexion à la base du workspace.
        mode: Stratégie si la table existe (vocabulaire polars).
    """
    modes: dict[str, Literal["create", "replace", "create_append"]] = {
        "fail": "create",
        "replace": "replace",
        "append": "create_append",
    }

    table_arrow = frame.to_arrow(compat_level=pl.CompatLevel.oldest())
    with pgdbapi.connect(db_url) as conn, conn.cursor() as cursor:
        cursor.adbc_ingest(table_name, table_arrow, mode=modes[mode])
        conn.commit()


def to_polars_for_adbc(df: pd.DataFrame) -> pl.DataFrame:
    """Convertit en Polars en neutralisant les types qu'ADBC ne sait pas écrire.

    Args:
        df: Table pandas à publier.

    Returns:
        Le DataFrame Polars prêt pour l'ingestion ADBC.
    """
    frame = pl.DataFrame(df)

    vides = [nom for nom, dtype in frame.schema.items() if dtype == pl.Null]
    if vides:
        current_run.log_warning(f"Colonnes entièrement vides castées en texte : {vides}.")
        frame = frame.with_columns(pl.col(nom).cast(pl.Utf8) for nom in vides)

    nanosecondes = [
        nom
        for nom, dtype in frame.schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_unit == "ns"
    ]
    if nanosecondes:
        frame = frame.with_columns(pl.col(nom).cast(pl.Datetime("us")) for nom in nanosecondes)
    return frame


def export_to_database(
    df: pd.DataFrame,
    table_name: str,
    db_url: str,
    mode: Literal["append", "replace", "fail"] = "replace",
) -> None:
    """Écrit un DataFrame dans une table de la base du workspace (remplace par défaut).

    Args:
        df: Agrégats à publier.
        table_name: Nom de la table de staging cible.
        db_url: URI de connexion à la base du workspace (workspace.database_url).
        mode: Stratégie si la table existe (replace par défaut).

    Raises:
        RuntimeError: si les deux moteurs échouent (messages d'origine conservés).
    """
    current_run.log_info(f"Export des données vers la table `{table_name}` de la base de données.")
    frame = to_polars_for_adbc(df)

    echec_adbc: str | None = None
    try:
        ingerer_adbc(frame, table_name, db_url, mode)
    # Capture volontairement large : l'erreur ADBC ne doit pas remonter telle quelle
    except Exception as exc:
        echec_adbc = f"{type(exc).__module__}.{type(exc).__name__}: {exc}"

    if echec_adbc is not None:
        schema = {nom: str(dtype) for nom, dtype in frame.schema.items()}
        current_run.log_error(f"Échec de l'écriture ADBC de « {table_name} » : {echec_adbc}")
        current_run.log_error(f"Schéma envoyé ({frame.height} lignes) : {schema}")

        current_run.log_warning(f"Repli SQLAlchemy pour « {table_name} » (écriture dégradée).")
        echec_repli: str | None = None
        try:
            frame.write_database(
                table_name, connection=db_url, if_table_exists=mode, engine="sqlalchemy"
            )
        except Exception as exc:
            echec_repli = f"{type(exc).__module__}.{type(exc).__name__}: {exc}"

        if echec_repli is not None:
            raise RuntimeError(
                f"Écriture de « {table_name} » échouée - ADBC : {echec_adbc} "
                f"- repli SQLAlchemy : {echec_repli}"
            )

    current_run.log_info(f"Table « {table_name} » écrite ({mode}) : {len(df)} lignes.")
    verifier_export(frame, table_name, db_url)
    current_run.add_database_output(table_name)


def verifier_export(frame: pl.DataFrame, table_name: str, db_url: str) -> None:
    """Relit la table publiée et compare volumétrie et cardinalités à la source.

    Args:
        frame: Données telles qu'envoyées à la base.
        table_name: Table à relire.
        db_url: URI de connexion à la base du workspace.

    Raises:
        RuntimeError: Si la volumétrie ou une cardinalité diffère de la source.
    """
    temoins = [col for col in config.COLS_TEMOINS if col in frame.columns]
    projection = ", ".join(
        ["count(*) AS lignes", *[f'count(DISTINCT "{col}") AS "d_{col}"' for col in temoins]]
    )
    relu = pl.read_database_uri(
        f'SELECT {projection} FROM "public"."{table_name}"', uri=db_url
    ).row(0, named=True)

    ecarts = []
    if int(relu["lignes"]) != frame.height:
        ecarts.append(f"{relu['lignes']} lignes en base contre {frame.height} envoyées")
    for col in temoins:
        # count(DISTINCT) ignore les NULL, n_unique() les compte : on aligne.
        attendu = frame[col].n_unique() - (1 if frame[col].null_count() else 0)
        obtenu = int(relu[f"d_{col}"])
        if obtenu != attendu:
            ecarts.append(f"{col} : {obtenu} valeurs distinctes en base contre {attendu} attendues")

    if ecarts:
        raise RuntimeError(f"Contrôle de l'export « {table_name} » - " + " ; ".join(ecarts))
    current_run.log_info(
        f"Contrôle de « {table_name} » : volumétrie et cardinalités conformes "
        f"({', '.join(temoins) or 'aucune colonne témoin'})."
    )


if __name__ == "__main__":
    compute_indicators_mve_tdb()
