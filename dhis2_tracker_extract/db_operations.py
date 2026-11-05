from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

import config
import polars as pl
from sqlalchemy import inspect, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def read_watermark(engine: Engine, program_short: str) -> datetime | None:
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
    if not inspect(engine).has_table(config.STATE_TABLE):
        return None
    with engine.connect() as conn:
        row = conn.execute(
            text(f'SELECT last_run_at FROM "{config.STATE_TABLE}" WHERE program = :p'),
            {"p": program_short},
        ).fetchone()
    return row[0] if row else None


def write_watermark(engine: Engine, program_short: str, ts: datetime) -> None:
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
    engine: Engine,
    table: str,
    df: pl.DataFrame,
    key: str = config.UPSERT_KEY,
    chunk_size: int = config.UPSERT_CHUNK_SIZE,
    log: Callable[[str], None] | None = None,
) -> int:
    """Insère/met à jour ``df`` dans ``table`` au grain ``key`` (``event_id``).

    Comme une soumission modifiée peut voir ses data values changer (ajout,
    suppression), on **remplace toutes les lignes** des ``event_id`` du lot :
    écriture dans une table de staging, suppression des clés correspondantes dans
    la cible, puis insertion. À la première écriture, la staging devient
    directement la table cible.

    La staging est écrite par tranches de ``chunk_size`` lignes : chaque
    tranche est convertie en pandas séparément, ce qui borne la mémoire. La
    cible n'est modifiée qu'à la fin, dans une seule transaction.

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
    chunk_size : int, optional
        Nombre de lignes par tranche écrite dans la staging.
    log : Callable[[str], None], optional
        Journalise la progression de l'écriture.

    Returns
    -------
    int
        Nombre de lignes écrites (0 si ``df`` est vide).
    """
    if df.is_empty():
        return 0

    staging = f"{table}__staging"
    n_chunks = -(-df.height // chunk_size)
    for i, chunk in enumerate(df.iter_slices(chunk_size), start=1):
        chunk.write_database(
            staging,
            connection=engine,
            if_table_exists="replace" if i == 1 else "append",
            engine="sqlalchemy",
        )
        if log:
            log(f"Staging {table} : tranche {i}/{n_chunks} écrite")

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
                conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{col["name"]}" {col_type}'))
                target_cols.add(col["name"])

        common = [c["name"] for c in staging_cols if c["name"] in target_cols]
        col_list = ", ".join(f'"{c}"' for c in common)

        conn.execute(
            text(f'DELETE FROM "{table}" WHERE "{key}" IN (SELECT DISTINCT "{key}" FROM "{staging}")')
        )
        conn.execute(text(f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM "{staging}"'))
        conn.execute(text(f'DROP TABLE "{staging}"'))
    return df.height
