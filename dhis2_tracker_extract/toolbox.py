from __future__ import annotations

import config
import polars as pl
from openhexa.toolbox.dhis2 import DHIS2, dataframe


def get_program_stage_data_elements(tracker: DHIS2) -> pl.DataFrame:
    """Extrait le catalogue (program stage x data element) de l'instance.

    Sert à la fois au décodage des valeurs (option set + type) et à l'étiquetage
    des événements (nom lisible du stage et du data element).

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.

    Returns
    -------
    pl.DataFrame
        Une ligne par data element rattaché à un stage, colonnes :
        ``program_stage_id, program_stage_name, data_element_id,
        data_element_name, value_type, option_set_id, option_set_name``.
    """
    metadata = tracker.api.get(
        "programStages",
        params={
            "fields": (
                "id,name,"
                "programStageDataElements["
                "dataElement[id,name,valueType,optionSet[id,name]]"
                "]"
            )
        },
    )
    rows: list[dict] = []
    for stage in metadata.get("programStages", []):
        for psde in stage.get("programStageDataElements", []):
            data_element = psde.get("dataElement", {})
            option_set = data_element.get("optionSet")
            rows.append(
                {
                    "program_stage_id": stage.get("id"),
                    "program_stage_name": stage.get("name"),
                    "data_element_id": data_element.get("id"),
                    "data_element_name": data_element.get("name"),
                    "value_type": data_element.get("valueType"),
                    "option_set_id": option_set.get("id") if option_set else None,
                    "option_set_name": option_set.get("name") if option_set else None,
                }
            )
    return pl.DataFrame(rows)


def get_option_sets(tracker: DHIS2) -> pl.DataFrame:
    """Extrait toutes les options (code -> libellé) de l'instance.

    Table de référence du décodage : chaque option d'un option set y figure avec
    son code (valeur stockée) et son libellé (valeur lisible).

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.

    Returns
    -------
    pl.DataFrame
        Une ligne par option, colonnes : ``option_set_id, option_set_name,
        option_id, option_code, option_name``.
    """
    metadata = tracker.api.get(
        "optionSets",
        params={"fields": "id,name,options[id,code,name]", "paging": "false"},
    )
    rows: list[dict] = []
    for option_set in metadata.get("optionSets", []):
        for option in option_set.get("options", []):
            rows.append(
                {
                    "option_set_id": option_set.get("id"),
                    "option_set_name": option_set.get("name"),
                    "option_id": option.get("id"),
                    "option_code": option.get("code"),
                    "option_name": option.get("name"),
                }
            )
    return pl.DataFrame(rows)


def get_org_unit_levels(tracker: DHIS2) -> pl.DataFrame:
    """Extrait la hiérarchie géographique (libellés de niveau par org unit).

    Conserve l'``id`` (clé de jointure sur ``organisation_unit_id``) et les
    libellés de chaque niveau (province, zone de santé, aire de santé…), utiles à
    l'agrégation.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.

    Returns
    -------
    pl.DataFrame
        Une ligne par org unit, colonnes : ``id`` + ``level_{n}_name``.
    """
    org_units = dataframe.get_organisation_units(tracker)
    level_cols = [c for c in org_units.columns if c.endswith("_name") and c.startswith("level_")]
    return org_units.select(["id", *level_cols])


def get_tracked_entities(tracker: DHIS2, program_uid: str) -> pl.DataFrame:
    """Extrait les tracked entities d'un programme avec leurs attributs.

    Une ligne par TEI ; chaque attribut devient une colonne (clé =
    ``displayName``), exposant directement num_epid, sexe, âge, date de
    notification… pour l'agrégation. La pagination s'arrête dès qu'une page
    revient vide (robuste aux variations de ``pager`` selon les versions DHIS2).

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uid : str
        UID du programme tracker.

    Returns
    -------
    pl.DataFrame
        Une ligne par TEI : ``tracked_entity_id, organisation_unit_id,
        created_at, updated_at`` + une colonne par attribut. DataFrame vide
        (schéma minimal) si le programme n'a aucun TEI.
    """
    all_teis: list[dict] = []
    page = 1
    while True:
        response = tracker.api.get(
            "tracker/trackedEntities",
            params={
                "program": program_uid,
                "fields": "trackedEntity,orgUnit,createdAt,updatedAt,attributes",
                "page": page,
                "pageSize": 100,
            },
        )
        all_teis.extend(response.get("trackedEntities", []))
        pager = response.get("pager", {})

        if not response.get("trackedEntities"):
            break
        if pager.get("pageCount") and page >= pager["pageCount"]:
            break
        page += 1

    if not all_teis:
        return pl.DataFrame(
            schema={"tracked_entity_id": pl.String, "organisation_unit_id": pl.String}
        )

    df = pl.DataFrame(
        [
            {
                "tracked_entity_id": row["trackedEntity"],
                "organisation_unit_id": row["orgUnit"],
                "created_at": row.get("createdAt"),
                "updated_at": row.get("updatedAt"),
                **{att["displayName"].strip(): att["value"] for att in row.get("attributes", [])},
            }
            for row in all_teis
        ]
    )
    df = df.select(
        pl.exclude(
            [
                "MVE - Nom du Chef de quartier",
                "MVE - Nom, post nom et prénom du cas",
                "002 MVE-FS-Nom et prénom du contact",
                "MVE - FC - Fiche remplie par",
                "003 MVE-FS-Chef de famille",
            ]
        )
    )

    return df.with_columns(
        pl.col(col).cast(pl.Datetime, strict=False).dt.date().alias(col)
        for col in ("created_at", "updated_at")
    )


def get_enrollments(
    tracker: DHIS2,
    program_uid: str,
    org_unit_parent: str = config.ORG_UNIT_PARENT,
    page_size: int = config.PAGE_SIZE,
) -> pl.DataFrame:
    """Extrait les enrôlements d'un programme (date d'inscription par cas).

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uid : str
        UID du programme tracker.
    org_unit_parent : str, optional
        Org unit racine ; on extrait tous ses descendants (``DESCENDANTS``).
    page_size : int, optional
        Taille de page des appels paginés.

    Returns
    -------
    pl.DataFrame
        Une ligne par enrôlement, colonnes : ``enrollment_id, enrolled_at,
        enrollment_org_unit`` (``enrollment_id`` = clé de jointure côté
        événements). DataFrame vide (schéma typé) si aucun enrôlement.
    """
    rows: list[dict] = []
    page = 1
    params = {
        "program": program_uid,
        "orgUnit": org_unit_parent,
        "orgUnitMode": "DESCENDANTS",
        "pageSize": page_size,
    }
    while True:
        params["page"] = page
        enrollments = tracker.api.get("tracker/enrollments", params).get("enrollments", [])
        if not enrollments:
            break
        for e in enrollments:
            rows.append(
                {
                    "enrollment_id": e.get("enrollment"),
                    "enrolled_at": e.get("enrolledAt"),
                    "enrollment_org_unit": e.get("orgUnit"),
                }
            )
        page += 1

    if not rows:
        return pl.DataFrame(
            schema={
                "enrollment_id": pl.String,
                "enrolled_at": pl.Datetime,
                "enrollment_org_unit": pl.String,
            }
        )
    return pl.DataFrame(rows).with_columns(pl.col("enrolled_at").cast(pl.Datetime, strict=False))


def get_events(
    tracker: DHIS2,
    program_uid: str,
    org_unit_parent: str = config.ORG_UNIT_PARENT,
    page_size: int = config.PAGE_SIZE,
    occurred_after: str | None = None,
    occurred_before: str | None = None,
    updated_after: str | None = None,
    include_deleted: bool = True,
) -> pl.DataFrame:
    """Extrait les événements d'un programme au format **long**.

    Une ligne par couple (événement, data element). Un événement sans data value
    produit tout de même une ligne (``data_element_id``/``value`` nuls) afin de ne
    perdre aucun événement.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uid : str
        UID du programme tracker.
    org_unit_parent : str, optional
        Org unit racine ; on extrait tous ses descendants (``DESCENDANTS``).
    page_size : int, optional
        Taille de page des appels paginés.
    occurred_after, occurred_before : str, optional
        Bornes par date de survenue (``YYYY-MM-DD``).
    updated_after : str, optional
        Borne par date de **dernière modification** (``YYYY-MM-DDTHH:MM:SS``) :
        c'est ce filtre qui rejoue les soumissions corrigées (incrémental).
    include_deleted : bool, optional
        Ramène aussi les événements supprimés (drapeau ``deleted``), pour
        propager les suppressions lors de l'upsert. Vrai par défaut.

    Returns
    -------
    pl.DataFrame
        Colonnes : métadonnées d'événement (``event_id, status, program_id,
        tracked_entity_id, program_stage_id, enrollment_id,
        organisation_unit_id, occurred_at, created_at, updated_at, deleted``) +
        ``data_element_id, value``. DataFrame vide si aucun événement.
    """
    rows: list[dict] = []
    page = 1
    params = {
        "program": program_uid,
        "orgUnit": org_unit_parent,
        "orgUnitMode": "DESCENDANTS",
        "pageSize": page_size,
    }
    if occurred_after:
        params["occurredAfter"] = occurred_after
    if occurred_before:
        params["occurredBefore"] = occurred_before
    if updated_after:
        params["updatedAfter"] = updated_after
    if include_deleted:
        params["includeDeleted"] = "true"

    while True:
        params["page"] = page
        events = tracker.api.get("tracker/events", params).get("events", [])
        if not events:
            break
        for event in events:
            common = {
                "event_id": event.get("event"),
                "status": event.get("status"),
                "program_id": event.get("program"),
                "tracked_entity_id": event.get("trackedEntity"),
                "program_stage_id": event.get("programStage"),
                "enrollment_id": event.get("enrollment"),
                "organisation_unit_id": event.get("orgUnit"),
                "occurred_at": event.get("occurredAt"),
                "created_at": event.get("createdAt"),
                "updated_at": event.get("updatedAt"),
                "deleted": event.get("deleted"),
            }
            data_values = event.get("dataValues", [])
            if not data_values:
                rows.append({**common, "data_element_id": None, "value": None})
            for dv in data_values:
                rows.append(
                    {
                        **common,
                        "data_element_id": dv.get("dataElement"),
                        "value": dv.get("value"),
                    }
                )
        page += 1

    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame(rows)
    return df.with_columns(
        pl.col(col).cast(pl.Datetime, strict=False).alias(col)
        for col in ("occurred_at", "created_at", "updated_at")
    )


def build_value_decoder(
    df_stage_de: pl.DataFrame, df_options: pl.DataFrame
) -> tuple[dict[str, dict[str, str]], set[str]]:
    """Construit la table de décodage code -> libellé par data element.

    Croise le catalogue des data elements (qui porte l'``option_set_id`` et le
    ``value_type``) avec les options de l'instance.

    Parameters
    ----------
    df_stage_de : pl.DataFrame
        Sortie de :func:`get_program_stage_data_elements`.
    df_options : pl.DataFrame
        Sortie de :func:`get_option_sets`.

    Returns
    -------
    tuple[dict[str, dict[str, str]], set[str]]
        ``(de_options_map, multi_de)`` où ``de_options_map`` associe à chaque
        ``data_element_id`` son dictionnaire ``{code: libellé}``, et ``multi_de``
        l'ensemble des data elements à choix multiples (``MULTI_TEXT``, codes
        séparés par des virgules).
    """
    options = {
        key[0]: dict(zip(group["option_code"], group["option_name"], strict=False))
        for key, group in df_options.group_by("option_set_id")
    }
    de_to_options = (
        df_stage_de.filter(pl.col("option_set_id").is_not_null())
        .select("data_element_id", "option_set_id", "value_type")
        .unique()
    )
    de_options_map = {
        de_id: options.get(os_id, {}) for de_id, os_id, _ in de_to_options.iter_rows()
    }
    multi_de = {de_id for de_id, _, vt in de_to_options.iter_rows() if vt == config.MULTI_TEXT_TYPE}
    return de_options_map, multi_de


def add_value_norm(
    df_events: pl.DataFrame,
    de_options_map: dict[str, dict[str, str]],
    multi_de: set[str],
) -> pl.DataFrame:
    """Ajoute la colonne ``value_norm`` (valeur décodée) aux événements.

    Pour un data element à option set, le code est remplacé par son libellé ;
    pour un champ à choix multiples, chaque code de la liste est décodé. Les
    valeurs sans correspondance (texte libre, dates, numériques) sont conservées
    telles quelles.

    Parameters
    ----------
    df_events : pl.DataFrame
        Événements au format long (sortie de :func:`get_events`).
    de_options_map : dict[str, dict[str, str]]
        Table ``{data_element_id: {code: libellé}}`` (cf.
        :func:`build_value_decoder`).
    multi_de : set[str]
        Data elements à choix multiples.

    Returns
    -------
    pl.DataFrame
        ``df_events`` augmenté de la colonne ``value_norm``.
    """
    if df_events.is_empty():
        return df_events.with_columns(pl.lit(None, dtype=pl.String).alias("value_norm"))

    def _decode(de_id: str | None, value: str | None) -> str | None:
        if value is None or de_id is None:
            return value
        mapping = de_options_map.get(de_id)
        if not mapping:
            return value
        if de_id in multi_de:
            return ", ".join(mapping.get(code.strip(), code.strip()) for code in value.split(","))
        return mapping.get(value, value)

    return df_events.with_columns(
        pl.struct(["data_element_id", "value"])
        .map_elements(lambda s: _decode(s["data_element_id"], s["value"]), return_dtype=pl.String)
        .alias("value_norm")
    )


def enrich_events(
    df_events: pl.DataFrame,
    df_enrollments: pl.DataFrame,
    df_tei: pl.DataFrame,
    df_stage_de: pl.DataFrame,
    df_org_levels: pl.DataFrame,
) -> pl.DataFrame:
    """Joint enrôlements, tracked entities, libellés DE et hiérarchie géo.

    Parameters
    ----------
    df_events : pl.DataFrame
        Événements au format long (idéalement déjà décodés, cf.
        :func:`add_value_norm`).
    df_enrollments : pl.DataFrame
        Enrôlements (jointure sur ``enrollment_id``) :
        ``enrolled_at, enrollment_org_unit``.
    df_tei : pl.DataFrame
        Tracked entities (jointure sur ``tracked_entity_id``) : attributs
        (num_epid, sexe, âge…). ``organisation_unit_id`` est retiré pour ne pas
        masquer celui de l'événement.
    df_stage_de : pl.DataFrame
        Catalogue DE (jointure sur ``program_stage_id, data_element_id``) :
        ``program_stage_name, data_element_name, value_type, option_set_id``.
    df_org_levels : pl.DataFrame
        Hiérarchie géo (jointure ``id`` -> ``organisation_unit_id``) :
        ``level_{n}_name``.

    Returns
    -------
    pl.DataFrame
        Table longue enrichie, prête pour l'agrégation. ``df_events`` inchangé
        s'il est vide.
    """
    if df_events.is_empty():
        return df_events

    out = df_events.join(df_enrollments, on="enrollment_id", how="left")

    if not df_tei.is_empty():
        tei = df_tei.drop("organisation_unit_id", strict=False)
        out = out.join(tei, on="tracked_entity_id", how="left", suffix="_tei")

    out = out.join(
        df_stage_de.select(
            "program_stage_id",
            "program_stage_name",
            "data_element_id",
            "data_element_name",
            "value_type",
            "option_set_id",
        ),
        on=["program_stage_id", "data_element_id"],
        how="left",
    )

    out = out.join(
        df_org_levels.rename({"id": "organisation_unit_id"}),
        on="organisation_unit_id",
        how="left",
    )
    return out


def extract_tracker(
    tracker: DHIS2,
    program_uid: str,
    df_stage_de: pl.DataFrame,
    de_options_map: dict[str, dict[str, str]],
    multi_de: set[str],
    df_org_levels: pl.DataFrame,
    org_unit_parent: str = config.ORG_UNIT_PARENT,
    occurred_after: str | None = None,
    occurred_before: str | None = None,
    updated_after: str | None = None,
) -> pl.DataFrame:
    """Extrait et enrichit un tracker complet -> table longue ``value_norm``.

    Orchestration par programme : événements + enrôlements + tracked entities,
    puis décodage et jointures.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uid : str
        UID du programme tracker.
    df_stage_de : pl.DataFrame
        Catalogue DE partagé (cf. :func:`get_program_stage_data_elements`).
    de_options_map : dict[str, dict[str, str]]
        Table de décodage partagée (cf. :func:`build_value_decoder`).
    multi_de : set[str]
        Data elements à choix multiples (cf. :func:`build_value_decoder`).
    df_org_levels : pl.DataFrame
        Hiérarchie géo partagée (cf. :func:`get_org_unit_levels`).
    org_unit_parent : str, optional
        Org unit racine de l'extraction.
    occurred_after, occurred_before : str, optional
        Fenêtre manuelle par date de survenue (``YYYY-MM-DD``).
    updated_after : str, optional
        Filtre de dernière modification pour l'extraction incrémentale des
        soumissions corrigées (``YYYY-MM-DDTHH:MM:SS``).

    Returns
    -------
    pl.DataFrame
        Table longue enrichie, prête pour l'agrégation et l'upsert. DataFrame
        vide si aucun événement ne correspond aux filtres.
    """
    df_events = get_events(
        tracker,
        program_uid,
        org_unit_parent=org_unit_parent,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        updated_after=updated_after,
    )
    if df_events.is_empty():
        return df_events

    df_enrollments = get_enrollments(tracker, program_uid, org_unit_parent=org_unit_parent)
    df_tei = get_tracked_entities(tracker, program_uid)

    df_events = add_value_norm(df_events, de_options_map, multi_de)
    return enrich_events(df_events, df_enrollments, df_tei, df_stage_de, df_org_levels)
