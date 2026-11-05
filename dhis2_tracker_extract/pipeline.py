from __future__ import annotations

from datetime import datetime, UTC

import config
import db_operations
import programme
import toolbox
from isolation import ProcessusInterrompuError, executer_isole
from journal import log_step
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
    del df_options
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
        log_step(f"Extraction {short_name} ({program_uid}) — {mode}, dans un processus dédié…")

        # Un processus neuf par programme : sa mémoire est entièrement rendue au
        # système à la fin, le programme suivant repart de zéro.
        try:
            resume = executer_isole(
                programme.extraire_programme,
                dhis_con,
                workspace.database_url,
                program_uid,
                short_name,
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
        except ProcessusInterrompuError as exc:
            current_run.log_error(f"{short_name} : {exc}")
            raise
        if resume["lignes"] or full_refresh:
            current_run.add_database_output(table)
        log_step(f"{short_name} : processus terminé (pic {resume['pic_mo']:.0f} Mo), mémoire rendue")

        if not manual_window:
            db_operations.write_watermark(engine, short_name, run_started_at)


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
