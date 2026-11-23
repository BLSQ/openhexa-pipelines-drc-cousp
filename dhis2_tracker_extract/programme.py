from __future__ import annotations

import config
import db_operations
import polars as pl
import toolbox
from journal import log_step, memory_mb
from openhexa.sdk import DHIS2Connection, current_run
from openhexa.toolbox.dhis2 import DHIS2
from sqlalchemy import create_engine


def extraire_programme(
    dhis_con: DHIS2Connection,
    database_url: str,
    program_uid: str,
    short_name: str,
    replace: bool = False,
    **extract_kwargs: object,
) -> dict[str, int | float]:
    """Extrait un programme puis écrit le résultat dans sa table.

    Exécutée dans un processus dédié (cf. :func:`isolation.executer_isole`) :
    les objets lourds (client DHIS2, moteur SQLAlchemy, tables) sont recréés
    ici à partir d'arguments sérialisables, et toute la mémoire est rendue au
    système quand le processus se termine.

    Parameters
    ----------
    dhis_con : DHIS2Connection
        Connexion DHIS2.
    database_url : str
        URI de la base du workspace.
    program_uid : str
        UID du programme tracker.
    short_name : str
        Nom court du programme (table ``{short_name}_events``).
    replace : bool, optional
        Recrée la table (rechargement complet) au lieu d'un upsert au grain
        ``event_id``.
    **extract_kwargs : object
        Arguments transmis à :func:`toolbox.extract_tracker` (métadonnées,
        org unit racine, fenêtre, ``updated_after``).

    Returns
    -------
    dict[str, int | float]
        ``lignes`` (actives écrites), ``cas``, ``supprimees`` (écartées) et
        ``pic_mo`` (pic mémoire du processus).

    Raises
    ------
    RuntimeError
        En cas d'échec, avec le programme et l'étape. Le type est volontairement
        standard : une exception native (polars, pilote SQL) pourrait ne pas se
        sérialiser vers le processus parent.
    """
    etape = ["extraction"]
    try:
        return _extraire_et_ecrire(
            dhis_con, database_url, program_uid, short_name, replace, etape, extract_kwargs
        )
    except Exception as exc:
        message = f"{short_name} : échec pendant l'{etape[0]} — {type(exc).__name__}: {str(exc)[:500]}"
        current_run.log_error(message)
        raise RuntimeError(message) from None


def _extraire_et_ecrire(
    dhis_con: DHIS2Connection,
    database_url: str,
    program_uid: str,
    short_name: str,
    replace: bool,
    etape: list[str],
    extract_kwargs: dict[str, object],
) -> dict[str, int | float]:
    """Corps de :func:`extraire_programme`, ``etape[0]`` suivant l'étape en cours.

    Returns
    -------
    dict[str, int | float]
        Cf. :func:`extraire_programme`.
    """
    table = f"{short_name}_events"

    def log(msg: str) -> None:
        log_step(f"{short_name} : {msg}")

    df = toolbox.extract_tracker(DHIS2(dhis_con), program_uid, log=log, **extract_kwargs)
    if df.is_empty():
        log("aucun événement nouveau/modifié.")
        return {"lignes": 0, "cas": 0, "supprimees": 0, "pic_mo": memory_mb()[1]}

    etape[0] = "écriture en base"
    flags = [pl.col(f).fill_null(False) for f in config.DELETED_FLAGS if f in df.columns]
    supprime = pl.any_horizontal(flags) if flags else pl.lit(False)
    n_supprimees = df.filter(supprime).height
    n_cases = df.filter(~supprime)["tracked_entity_id"].n_unique()
    engine = create_engine(database_url)
    try:
        n = db_operations.upsert_events(engine, table, df, log=log, replace=replace)
    finally:
        engine.dispose()

    mode = "remplacée" if replace else "mise à jour"
    log(
        f"{n} lignes actives écrites ({n_cases} cas), {n_supprimees} lignes supprimées dans "
        f"DHIS2 écartées -> table {table} {mode}"
    )
    return {"lignes": n, "cas": n_cases, "supprimees": n_supprimees, "pic_mo": memory_mb()[1]}
