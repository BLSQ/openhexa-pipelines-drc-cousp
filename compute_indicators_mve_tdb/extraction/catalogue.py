"""Catalogue des tables d'événements : libellés des data elements et attributs TEI.

Sert à la LLN nominative, qui publie **tous** les data elements et attributs
d'entité suivie présents dans la source, et pas seulement ceux que la LLN
partagée renomme.
"""

from __future__ import annotations

import config
import polars as pl
from openhexa.sdk import current_run


def load_data_element_names(db_url: str, table_name: str) -> dict[str, str]:
    """Associe à chaque data element de la table son libellé DHIS2.

    Un data element renommé dans DHIS2 peut porter plusieurs libellés dans
    l'historique : on retient le plus grand (ordre lexicographique), pour un
    résultat déterministe.

    Args:
        db_url: URI de connexion à la base du workspace.
        table_name: Table source (format long du tracker).

    Returns:
        Le dictionnaire ``{data_element_id: data_element_name}``.
    """
    df = pl.read_database_uri(
        'SELECT "data_element_id", MAX("data_element_name") AS "data_element_name" '
        f'FROM "public"."{table_name}" WHERE "data_element_id" IS NOT NULL '
        'GROUP BY "data_element_id"',
        uri=db_url,
    )
    return dict(df.iter_rows())


def tei_attribute_columns(db_url: str, table_name: str) -> list[str]:
    """Liste les colonnes d'attributs TEI de la table.

    La table longue porte une colonne par attribut d'entité suivie (nom =
    libellé DHIS2), à côté des colonnes d'événement et d'enrichissement : est
    considérée comme attribut toute colonne absente de
    config.COLONNES_NON_ATTRIBUTS et qui ne suit pas le motif ``level_*``.

    Args:
        db_url: URI de connexion à la base du workspace.
        table_name: Table source (format long du tracker).

    Returns:
        Les noms des colonnes d'attributs, dans l'ordre de la table.
    """
    colonnes = pl.read_database_uri(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = 'public' AND table_name = '{table_name}' "
        "ORDER BY ordinal_position",
        uri=db_url,
    ).get_column("column_name")
    return [
        col
        for col in colonnes
        if col not in config.COLONNES_NON_ATTRIBUTS and not col.startswith("level_")
    ]


def load_tei_attributes(db_url: str, table_name: str, exclues: list[str]) -> pl.DataFrame:
    """Charge tous les attributs TEI, une ligne par entité suivie.

    Retient, pour chaque entité, la ligne la plus récemment mise à jour (même
    règle que load_contacts_events()).

    Args:
        db_url: URI de connexion à la base du workspace.
        table_name: Table source (format long du tracker).
        exclues: Attributs déjà publiés par la LLN partagée (libellés DHIS2),
            à ne pas reprendre.

    Returns:
        ``tracked_entity_id`` + une colonne par attribut, sous son libellé DHIS2.
    """
    attributs = [col for col in tei_attribute_columns(db_url, table_name) if col not in exclues]
    projection = ", ".join(f'"{col}"' for col in ["tracked_entity_id", *attributs])
    tei = pl.read_database_uri(
        f'SELECT DISTINCT ON ("tracked_entity_id") {projection} '
        f'FROM "public"."{table_name}" '
        'ORDER BY "tracked_entity_id", "updated_at_tei" DESC NULLS LAST, "created_at" DESC',
        uri=db_url,
    )
    current_run.log_info(
        f"Attributs TEI complets de « {table_name} » : {tei.height} entités suivies, "
        f"{len(attributs)} attribut(s) en plus de la LLN partagée."
    )
    return tei
