"""Écriture des tables dans la base du workspace (ADBC, repli SQLAlchemy)."""

from __future__ import annotations

from typing import Literal

import config
import pandas as pd
import polars as pl
from adbc_driver_postgresql import dbapi as pgdbapi
from openhexa.sdk import current_run


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


def to_polars_for_adbc(df: pd.DataFrame | pl.DataFrame) -> pl.DataFrame:
    """Convertit en Polars en neutralisant les types qu'ADBC ne sait pas écrire.

    Args:
        df: Table (pandas ou Polars) à publier.

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
    df: pd.DataFrame | pl.DataFrame,
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
