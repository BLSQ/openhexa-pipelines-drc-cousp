from __future__ import annotations

import resource
import sys
from collections.abc import Callable
from datetime import datetime, UTC
from pathlib import Path

import config
import polars as pl
import db_operations
import toolbox
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from sqlalchemy import create_engine, Engine


@pipeline("DHIS2 Tracker Extract MVE")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    name="Connexion DHIS2",
    help="Connexion à l'instance tracker MVE",
    required=True,
)
@parameter(
    "program_uids",
    type=str,
    multiple=True,
    name="Programmes tracker",
    help="UID des programmes tracker à extraire (une table par programme). "
    "Par défaut : les deux trackers MVE.",
    required=False,
)
@parameter(
    "org_unit_parent",
    type=str,
    name="Unité d'organisation racine",
    connection="dhis_con",
    widget=DHIS2Widget.ORG_UNITS,
    help="UID de l'org unit dont on extrait les descendants.",
    default="ymGeqzoPhN3",
    required=False,
)
@parameter(
    "occurred_after",
    type=str,
    name="Événements depuis (YYYY-MM-DD)",
    help="Fenêtre manuelle par date de survenue. Vide = extraction incrémentale "
    "depuis la dernière exécution.",
    required=False,
)
@parameter(
    "occurred_before",
    type=str,
    name="Événements jusqu'à (YYYY-MM-DD)",
    help="Borne supérieure de la fenêtre manuelle (optionnel).",
    required=False,
)
@parameter(
    "full_refresh",
    type=bool,
    name="Rechargement complet",
    help="Ignore le filigrane et ré-extrait tout l'historique (upsert).",
    default=False,
    required=False,
)
def dhis2_tracker_extract_pipeline(
    dhis_con: DHIS2Connection,
    program_uids: list[str] | None = None,
    org_unit_parent: str = "ymGeqzoPhN3",
    occurred_after: str | None = None,
    occurred_before: str | None = None,
    full_refresh: bool = False,
) -> None:
    """Extrait des programmes tracker DHIS2 et upsert une table BD par programme."""
    if program_uids is None:
        program_uids = list(config.PROGRAMS)
    validate_date(occurred_after, "occurred_after")
    validate_date(occurred_before, "occurred_before")
    program_uids = list(dict.fromkeys(program_uids or config.PROGRAMS))

    run_started_at = datetime.now(UTC)
    manual_window = bool(occurred_after or occurred_before)

    tracker = DHIS2(dhis_con)
    engine = create_engine(workspace.database_url)

    current_run.log_info(f"Instance DHIS2 {toolbox.check_dhis2_version(tracker)}")
    programs = toolbox.get_programs(tracker, program_uids)

    log_step("Chargement des métadonnées (data elements, options, géo)…")
    df_stage_de = toolbox.get_program_stage_data_elements(tracker, program_uids)
    df_options = toolbox.get_option_sets(tracker)
    df_org_levels = toolbox.get_org_unit_levels(tracker)
    de_options_map, multi_de = toolbox.build_value_decoder(df_stage_de, df_options)
    log_step(f"Métadonnées : {df_stage_de.height} data elements, {df_org_levels.height} org units")

    for program_uid, short_name in programs.items():
        table = f"{short_name}_events"

        updated_after = incremental_watermark(
            engine, short_name, manual_window=manual_window, full_refresh=full_refresh
        )
        mode = (
            "fenêtre manuelle"
            if manual_window
            else ("complet" if updated_after is None else f"incrémental depuis {updated_after}")
        )
        log_step(f"Extraction {short_name} ({program_uid}) — {mode}…")

        def log(msg: str, short_name: str = short_name) -> None:
            log_step(f"{short_name} : {msg}")

        step = ["extraction"]
        try:
            extract_and_upsert(
                tracker,
                engine,
                program_uid,
                table,
                step,
                log,
                replace=full_refresh and not manual_window,
                df_stage_de=df_stage_de,
                de_options_map=de_options_map,
                multi_de=multi_de,
                df_org_levels=df_org_levels,
                org_unit_parent=org_unit_parent,
                occurred_after=occurred_after if manual_window else None,
                occurred_before=occurred_before if manual_window else None,
                updated_after=updated_after,
            )
        except Exception as exc:
            current_run.log_error(
                f"{short_name} : échec pendant l'{step[0]} — {type(exc).__name__}: {str(exc)[:500]}"
            )
            raise

        if not manual_window:
            db_operations.write_watermark(engine, short_name, run_started_at)


def extract_and_upsert(
    tracker: DHIS2,
    engine: Engine,
    program_uid: str,
    table: str,
    step: list[str],
    log: Callable[[str], None],
    replace: bool = False,
    **extract_kwargs: object,
) -> None:
    """Extrait un programme puis upsert le résultat dans sa table.

    La table extraite est locale à cette fonction : elle est libérée à son
    retour, avant le programme suivant (deux tables ne coexistent pas).

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    engine : Engine
        Moteur SQLAlchemy de la base du workspace.
    program_uid : str
        UID du programme tracker.
    table : str
        Table cible.
    step : list[str]
        Étape en cours (``step[0]``), mise à jour pour le message d'erreur.
    log : Callable[[str], None]
        Journalisation des étapes.
    replace : bool, optional
        Remplace le contenu de la table (rechargement complet) au lieu d'un
        upsert au grain ``event_id``.
    **extract_kwargs : object
        Arguments transmis à :func:`toolbox.extract_tracker`.
    """
    df = toolbox.extract_tracker(tracker, program_uid, log=log, **extract_kwargs)
    if df.is_empty():
        log("aucun événement nouveau/modifié.")
        return
    step[0] = "écriture en base"
    flags = [pl.col(f).fill_null(False) for f in config.DELETED_FLAGS if f in df.columns]
    supprime = pl.any_horizontal(flags) if flags else pl.lit(False)
    n_supprimees = df.filter(supprime).height
    n_cases = df.filter(~supprime)["tracked_entity_id"].n_unique()
    n = db_operations.upsert_events(engine, table, df, log=log, replace=replace)
    current_run.add_database_output(table)
    mode = "remplacée" if replace else "mise à jour"
    log(
        f"{n} lignes actives écrites ({n_cases} cas), {n_supprimees} lignes supprimées dans "
        f"DHIS2 écartées -> table {table} {mode}"
    )


def memory_mb() -> tuple[float, float]:
    """Mémoire du processus : résidente actuelle et pic, en Mo.

    La valeur courante vient de ``/proc/self/statm`` (Linux, cas des pods
    OpenHexa) ; ailleurs, on retombe sur le pic.

    Returns
    -------
    tuple[float, float]
        ``(rss_mb, peak_mb)``.
    """
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss est en octets sous macOS, en Ko sous Linux.
    peak = peak_kb / 1024 / (1024 if sys.platform == "darwin" else 1)
    try:
        resident_pages = int(Path("/proc/self/statm").read_text(encoding="ascii").split()[1])
        rss = resident_pages * resource.getpagesize() / 1024**2
    except OSError:
        rss = peak
    return rss, peak


def log_step(msg: str) -> None:
    """Journalise une étape avec la mémoire courante et le pic du processus.

    En cas d'arrêt brutal (dépassement de la limite mémoire du pod, qui ne
    laisse aucune trace Python), le dernier message situe l'étape et la mémoire
    atteinte.

    Parameters
    ----------
    msg : str
        Message d'étape.
    """
    rss, peak = memory_mb()
    current_run.log_info(f"{msg} [RAM {rss:.0f} Mo, pic {peak:.0f} Mo]")


def incremental_watermark(
    engine: Engine,
    short_name: str,
    manual_window: bool,
    full_refresh: bool,
) -> str | None:
    """Détermine le filtre ``updatedAfter`` à appliquer pour un programme.

    Parameters
    ----------
    engine : Engine
        Moteur SQLAlchemy (cf. :func:`db.get_engine`).
    short_name : str
        Nom court du programme.
    manual_window : bool
        Vrai si une fenêtre manuelle (occurred_after/before) est demandée.
    full_refresh : bool
        Vrai pour ignorer le filigrane et tout ré-extraire.

    Returns
    -------
    str | None
        Horodatage ``YYYY-MM-DDTHH:MM:SS`` à passer en ``updatedAfter``, ou
        ``None`` (pas de filtre de modification : fenêtre manuelle, rechargement
        complet, ou toute première exécution).
    """
    if manual_window or full_refresh:
        return None
    watermark = db_operations.read_watermark(engine, short_name)
    return watermark.strftime("%Y-%m-%dT%H:%M:%S") if watermark else None


def validate_date(value: str | None, name: str) -> None:
    """Valide le format ``YYYY-MM-DD`` d'un paramètre date optionnel.

    Parameters
    ----------
    value : str | None
        Valeur à valider ; ``None`` est accepté (paramètre absent).
    name : str
        Nom du paramètre, pour le message d'erreur.

    Raises
    ------
    ValueError
        Si ``value`` est non nul et n'est pas une date ``YYYY-MM-DD`` valide.
    """
    if value:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"{name} doit être au format YYYY-MM-DD (reçu : {value!r}).") from exc


if __name__ == "__main__":
    dhis2_tracker_extract_pipeline()
