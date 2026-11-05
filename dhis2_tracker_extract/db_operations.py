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
    replace: bool = False,
) -> int:
    """Insère/met à jour ``df`` dans ``table`` au grain ``key`` (``event_id``).

    Comme une soumission modifiée peut voir ses data values changer (ajout,
    suppression), on **remplace toutes les lignes** des ``event_id`` du lot :
    écriture dans une table de staging, suppression des clés correspondantes dans
    la cible, puis insertion. À la première écriture, la staging devient
    directement la table cible.

    Les lignes supprimées dans DHIS2 (``config.DELETED_FLAGS`` vrai) ne sont
    **pas stockées** : elles servent seulement à retirer de la cible les lignes
    de leur ``event_id``, puis sont écartées avant l'insertion. Les drapeaux
    eux-mêmes ne sont pas écrits : la table ne contient que des données
    actives.

    Avec ``replace`` (rechargement complet), la cible est **recréée** depuis la
    staging au lieu de ne retirer que les clés du lot : les événements purgés de
    DHIS2, qui ne reviennent plus dans l'API, disparaissent, et le schéma repart
    de la source (colonnes obsolètes retirées).

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
    replace : bool, optional
        Recrée la cible au lieu d'un upsert (rechargement complet).

    Returns
    -------
    int
        Nombre de lignes actives écrites (0 si ``df`` est vide).
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
    staging_cols = [c["name"] for c in insp.get_columns(staging)]
    flags = [c for c in staging_cols if c in config.DELETED_FLAGS]
    # Les drapeaux de suppression ne servent qu'à trier les lignes : ils ne sont
    # pas écrits dans la cible.
    colonnes = [c for c in staging_cols if c not in config.DELETED_FLAGS]
    actives = "WHERE NOT ({})".format(" OR ".join(f'COALESCE("{f}", FALSE)' for f in flags)) if flags else ""
    with engine.begin() as conn:
        n = conn.execute(text(f'SELECT COUNT(*) FROM "{staging}" {actives}')).scalar_one()
        if replace or not insp.has_table(table):
            # Table recréée : schéma repartant de la source (colonnes obsolètes et
            # drapeaux de suppression disparaissent). Échoue, sans rien modifier,
            # si un objet de la base (vue…) dépend de la table.
            if insp.has_table(table):
                conn.execute(text(f'DROP TABLE "{table}"'))
            col_list = ", ".join(f'"{c}"' for c in colonnes)
            conn.execute(text(f'CREATE TABLE "{table}" AS SELECT {col_list} FROM "{staging}" {actives}'))
        else:
            target_cols = {c["name"] for c in insp.get_columns(table)}
            staging_types = {c["name"]: c["type"] for c in insp.get_columns(staging)}
            for col in colonnes:
                if col not in target_cols:
                    col_type = staging_types[col].compile(engine.dialect)
                    conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {col_type}'))
            col_list = ", ".join(f'"{c}"' for c in colonnes)
            conn.execute(
                text(f'DELETE FROM "{table}" WHERE "{key}" IN (SELECT DISTINCT "{key}" FROM "{staging}")')
            )
            conn.execute(
                text(f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM "{staging}" {actives}')
            )
        conn.execute(text(f'DROP TABLE "{staging}"'))
    return n
