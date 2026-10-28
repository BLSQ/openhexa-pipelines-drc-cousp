"""Lecture et mise en forme des événements du tracker de notification MVE."""

from __future__ import annotations

from datetime import date, datetime

import config
import polars as pl
from openhexa.sdk import current_run


def load_notification_events(
    db_url: str,
    date_min: date,
    date_max: date | None,
    last_analytics_run: datetime | None,
    table_name: str = config.EVENTS_TABLE,
) -> pl.DataFrame:
    """Charge la table d'événements de notification MVE depuis le workspace.

    Args:
        db_url: URI de connexion à la base du workspace.
        date_min: Borne basse incluse sur enrolled_at.
        date_max: Borne haute incluse sur enrolled_at, ou None.
        last_analytics_run: Date de la dernière exécution des analyses, ou None.
        table_name: Table source (format long du tracker).

    Returns:
        Les événements bruts (grain événement DHIS2), prêts à être pivotés.
    """
    colonnes = [
        "event_id",
        "deleted",
        "tracked_entity_id",
        "enrollment_id",
        "enrollment_org_unit",
        "enrollment_deleted",
        "enrolled_at",
        "occurred_at",
        "created_at",
        "data_element_id",
        "value_norm",
        *config.DICO_TEI,
    ]
    projection = ", ".join(f'"{colonne}"' for colonne in colonnes)
    # data_elements = ", ".join(f"'{de}'" for de in config.DE_UTILES)
    # f'"data_element_id" IN ({data_elements})',
    conditions = conditions_fenetre(date_min, date_max, last_analytics_run)

    df = pl.read_database_uri(
        f'SELECT {projection} FROM "public"."{table_name}" WHERE {" AND ".join(conditions)}',
        uri=db_url,
    )
    df = df.rename(config.DICO_TEI).with_columns(
        pl.col("enrolled_at").cast(pl.Date, strict=False),
        pl.col("date_notification").cast(pl.Datetime, strict=False),
        pl.col("date_debut_symptomes").cast(pl.Date, strict=False),
    )
    current_run.log_info(f"Événements de notification lus : {df.height} lignes brutes.")
    # Événements et enrôlements supprimés dans DHIS2 (soft delete), ainsi que les
    # enrôlements sans numéro Epid ni sexe : ils ne correspondent à aucun cas
    # exploitable et gonflent les compteurs. Un drapeau de suppression nul exclut
    # la ligne, comme dans les tables analytiques DHIS2.
    df = df.filter(
        (~pl.col("enrollment_deleted"))
        & (~pl.col("deleted"))
        & (pl.col("numero_epid").is_not_null())
        & (pl.col("sexe").is_not_null())
    ).drop(["enrollment_deleted", "deleted"])
    current_run.log_info(
        f"Événements de notification retenus : {df.height} lignes, après exclusion des "
        "enrôlements et événements supprimés, et des numero_epid / sexe non renseignés."
    )
    return df


def conditions_fenetre(
    date_min: date,
    date_max: date | None,
    last_analytics_run: datetime | None,
) -> list[str]:
    """Conditions SQL de la fenêtre d'analyse (enrolled_at, borne analytics).

    Returns:
        Les conditions à combiner par AND dans la clause WHERE.
    """
    conditions = [f""""enrolled_at" >= DATE '{date_min.isoformat()}'"""]
    if date_max is not None:
        conditions.append(f""""enrolled_at" <= DATE '{date_max.isoformat()}'""")
    if last_analytics_run is not None:
        conditions.append(f""""created_at" < TIMESTAMP '{last_analytics_run.isoformat()}'""")
    return conditions


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
    valeurs = events.sort(
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
