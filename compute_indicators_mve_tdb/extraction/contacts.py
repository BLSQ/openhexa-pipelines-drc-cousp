"""Lecture des événements du tracker de suivi des contacts MVE."""

from __future__ import annotations

import config
import polars as pl
from openhexa.sdk import current_run


def load_contacts_events(
    db_url: str,
    table_name: str = config.CONTACTS_EVENTS_TABLE,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Charge les événements de suivi des contacts et les attributs TEI.

    Extraction complète de la table, sans fenêtre de dates. Deux lectures
    distinctes : les événements (restreints aux data elements de
    config.DICO_DE_MAPPING_CONTACTS) sans les attributs TEI, répétés sinon sur
    chaque ligne du format long, puis une ligne d'attributs par entité suivie
    (la plus récemment mise à jour). Les événements et enrôlements supprimés
    (soft delete) sont exclus, un drapeau nul excluant aussi la ligne, comme
    dans load_notification_events().

    Args:
        db_url: URI de connexion à la base du workspace.
        table_name: Table source (format long du tracker de suivi des contacts).

    Returns:
        Les événements bruts (grain événement x data element) et les attributs TEI.
    """
    data_elements = ", ".join(f"'{de}'" for de in config.DICO_DE_MAPPING_CONTACTS)

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
    ]
    projection = ", ".join(f'"{colonne}"' for colonne in colonnes)
    events = pl.read_database_uri(
        f'SELECT {projection} FROM "public"."{table_name}" '
        f'WHERE "data_element_id" IN ({data_elements})',
        uri=db_url,
    ).with_columns(
        pl.col("enrolled_at").cast(pl.Date, strict=False),
        pl.col("occurred_at").cast(pl.Date, strict=False),
    )
    current_run.log_info(f"Événements de suivi des contacts lus : {events.height} lignes brutes.")
    events = events.filter((~pl.col("enrollment_deleted")) & (~pl.col("deleted"))).drop(
        ["enrollment_deleted", "deleted"]
    )
    current_run.log_info(
        f"Événements de suivi des contacts retenus : {events.height} lignes, après exclusion "
        "des enrôlements et événements supprimés."
    )
    attributs = sorted(
        {nom for expr in config.EXPR_TEI_CONTACTS for nom in expr.meta.root_names()}
    )
    projection_tei = ", ".join(f'"{attribut}"' for attribut in attributs)
    tei = pl.read_database_uri(
        f'SELECT DISTINCT ON ("tracked_entity_id") {projection_tei} '
        f'FROM "public"."{table_name}" '
        'ORDER BY "tracked_entity_id", "updated_at_tei" DESC NULLS LAST, "created_at" DESC',
        uri=db_url,
    ).select(config.EXPR_TEI_CONTACTS)
    current_run.log_info(f"Attributs TEI des contacts extraits : {tei.height} entités suivies.")
    return events, tei
