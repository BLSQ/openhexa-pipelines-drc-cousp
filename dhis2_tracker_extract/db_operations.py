from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl

import config

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def read_watermark(engine: "Engine", program_short: str) -> datetime | None:
    """Lit le filigrane (dernière exécution réussie) d'un programme.

    Parameters
    ----------
    engine : Engine
        Moteur SQLAlchemy (cf. :func:`get_engine`).
    program_short : str
        Nom court du programme.

    Returns
    -------
    datetime | None
        Horodatage de la dernière exécution, ou ``None`` si la table d'état
        n'existe pas encore ou si le programme n'y figure pas (première
        exécution).
    """
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table(config.STATE_TABLE):
        return None
    with engine.connect() as conn:
        row = conn.execute(
            text(f'SELECT last_run_at FROM "{config.STATE_TABLE}" WHERE program = :p'),
            {"p": program_short},
        ).fetchone()
    return row[0] if row else None


def write_watermark(engine: "Engine", program_short: str, ts: datetime) -> None:
    """Enregistre l'instant d'exécution comme nouveau filigrane du programme.

    Crée la table d'état au besoin et fait un upsert (``ON CONFLICT``) sur la
    clé ``program``.

    Parameters
    ----------
    engine : Engine
        Moteur SQLAlchemy (cf. :func:`get_engine`).
    program_short : str
        Nom court du programme.
    ts : datetime
        Horodatage à mémoriser (typiquement le début du run).
    """
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE IF NOT EXISTS "{config.STATE_TABLE}" '
                "(program TEXT PRIMARY KEY, last_run_at TIMESTAMPTZ)"
            )
        )
        conn.execute(
            text(
                f'INSERT INTO "{config.STATE_TABLE}" (program, last_run_at) '
                "VALUES (:p, :t) "
                "ON CONFLICT (program) DO UPDATE SET last_run_at = EXCLUDED.last_run_at"
            ),
            {"p": program_short, "t": ts},
        )


def upsert_events(
    engine: "Engine",
    table: str,
    df: pl.DataFrame,
    key: str = config.UPSERT_KEY,
) -> int:
    """Insère/met à jour ``df`` dans ``table`` au grain ``key`` (``event_id``).

    Comme une soumission modifiée peut voir ses data values changer (ajout,
    suppression), on **remplace toutes les lignes** des ``event_id`` du lot :
    écriture dans une table de staging, suppression des clés correspondantes dans
    la cible, puis insertion. À la première écriture, la staging devient
    directement la table cible.

    Parameters
    ----------
    engine : Engine
        Moteur SQLAlchemy (cf. :func:`get_engine`).
    table : str
        Nom de la table cible (cf. :func:`config.table_name`).
    df : pl.DataFrame
        Lot enrichi à persister (sortie de
        :func:`toolbox.extract_tracker`).
    key : str, optional
        Clé d'unicité de l'upsert (défaut : ``event_id``).

    Returns
    -------
    int
        Nombre de lignes écrites (0 si ``df`` est vide).
    """
    from sqlalchemy import inspect, text

    if df.is_empty():
        return 0

    staging = f"{table}__staging"
    df.write_database(
        staging, connection=engine, if_table_exists="replace", engine="sqlalchemy"
    )

    insp = inspect(engine)
    with engine.begin() as conn:
        if not insp.has_table(table):
            conn.execute(text(f'ALTER TABLE "{staging}" RENAME TO "{table}"'))
            return df.height

        target_cols = {c["name"] for c in insp.get_columns(table)}
        staging_cols = insp.get_columns(staging)

        for col in staging_cols:
            if col["name"] not in target_cols:
                col_type = col["type"].compile(engine.dialect)
                conn.execute(
                    text(f'ALTER TABLE "{table}" ADD COLUMN "{col["name"]}" {col_type}')
                )
                target_cols.add(col["name"])

        common = [c["name"] for c in staging_cols if c["name"] in target_cols]
        col_list = ", ".join(f'"{c}"' for c in common)

        conn.execute(
            text(
                f'DELETE FROM "{table}" WHERE "{key}" IN (SELECT DISTINCT "{key}" FROM "{staging}")'
            )
        )
        conn.execute(
            text(
                f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM "{staging}"'
            )
        )
        conn.execute(text(f'DROP TABLE "{staging}"'))
    return df.height
