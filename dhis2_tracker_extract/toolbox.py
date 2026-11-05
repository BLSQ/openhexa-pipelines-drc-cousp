from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable, Iterator

import config
import polars as pl
from openhexa.toolbox.dhis2 import DHIS2, dataframe


def dhis2_minor_version(tracker: DHIS2) -> int:
    """Version mineure de l'instance DHIS2 (``"2.41.6"`` -> ``41``).

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.

    Returns
    -------
    int
        Version mineure, ``0`` si elle n'est pas lisible.
    """
    parts = str(tracker.version or "").split(".")
    return int("".join(ch for ch in parts[1] if ch.isdigit()) or 0) if len(parts) > 1 else 0


def check_dhis2_version(tracker: DHIS2) -> str:
    """Vérifie que l'instance est prise en charge >= 2.{config.MIN_DHIS2_MINOR}.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.

    Returns
    -------
    str
        Version brute de l'instance (pour le log).

    Raises
    ------
    ValueError
        Si la version est antérieure à 2.``MIN_DHIS2_MINOR`` ou illisible.
    """
    if dhis2_minor_version(tracker) < config.MIN_DHIS2_MINOR:
        raise ValueError(
            f"Version DHIS2 non prise en charge : {tracker.version!r} "
            f"(minimum 2.{config.MIN_DHIS2_MINOR})."
        )
    return tracker.version


def tracker_api_names(tracker: DHIS2) -> dict[str, str]:
    """Noms des paramètres du tracker API selon la version de l'instance.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.

    Returns
    -------
    dict[str, str]
        ``{"ou_mode", "trackedEntities", "enrollments"}`` -> nom du paramètre,
        et ``"uid_sep"`` -> séparateur des listes d'UID (``,`` à partir de 2.41,
        ``;`` avant).
    """
    if dhis2_minor_version(tracker) >= 41:
        return {
            "ou_mode": "orgUnitMode",
            "trackedEntities": "trackedEntities",
            "enrollments": "enrollments",
            "uid_sep": ",",
        }
    return {
        "ou_mode": "ouMode",
        "trackedEntities": "trackedEntity",
        "enrollments": "enrollment",
        "uid_sep": ";",
    }


def slugify(text: str, max_length: int = 47) -> str:
    """Nom d'identifiant SQL sûr : minuscules ASCII, ``_`` comme séparateur.

    Parameters
    ----------
    text : str
        Texte source (ex. nom court du programme DHIS2).
    max_length : int, optional
        Longueur maximale. PostgreSQL tronque les identifiants à 63
        caractères : nom court (<= 47) + ``_events__staging`` (16) tient dans
        cette limite.

    Returns
    -------
    str
        Identifiant normalisé, ex. ``"MVE - Contacts"`` -> ``"mve_contacts"``.
    """
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")
    return slug[:max_length].rstrip("_")


def get_programs(tracker: DHIS2, program_uids: list[str]) -> dict[str, str]:
    """Résout les programmes à extraire en ``{uid: nom court}``.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uids : list[str]
        UID des programmes demandés.

    Returns
    -------
    dict[str, str]
        ``{program_uid: short_name}`` dans l'ordre demandé.

    Raises
    ------
    ValueError
        Si un UID est introuvable (ou inaccessible), si ce n'est pas un
        programme tracker (``WITH_REGISTRATION``) ou si deux programmes
        aboutissent au même nom court.
    """
    response = tracker.api.get(
        "programs",
        params={
            "filter": f"id:in:[{','.join(program_uids)}]",
            "fields": "id,name,shortName,programType",
            "paging": "false",
        },
    )
    found = {p["id"]: p for p in response.get("programs", [])}
    missing = [uid for uid in program_uids if uid not in found]
    if missing:
        raise ValueError(f"Programmes introuvables ou inaccessibles : {', '.join(missing)}.")
    not_tracker = [
        uid for uid in program_uids if found[uid].get("programType") != "WITH_REGISTRATION"
    ]
    if not_tracker:
        raise ValueError(
            "Programmes sans inscription (programmes événementiels) non pris en charge : "
            f"{', '.join(not_tracker)}."
        )
    programs = {
        uid: config.PROGRAMS.get(uid) or slugify(found[uid].get("shortName") or found[uid]["name"])
        for uid in program_uids
    }
    if len(set(programs.values())) != len(programs):
        raise ValueError(f"Noms courts de programmes en collision : {programs}.")
    return programs


def get_program_stage_data_elements(
    tracker: DHIS2, program_uids: list[str] | None = None
) -> pl.DataFrame:
    """Extrait le catalogue (program stage x data element) de l'instance.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uids : list[str], optional
        Restreint le catalogue aux stages de ces programmes. ``None`` = tous.

    Returns
    -------
    pl.DataFrame
        Une ligne par data element rattaché à un stage, colonnes :
        ``program_stage_id, program_stage_name, data_element_id,
        data_element_name, value_type, option_set_id, option_set_name``.
    """
    params = {
        "fields": (
            "id,name,programStageDataElements[dataElement[id,name,valueType,optionSet[id,name]]]"
        ),
        "paging": "false",
    }
    if program_uids:
        params["filter"] = f"program.id:in:[{','.join(program_uids)}]"
    metadata = tracker.api.get("programStages", params=params)
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


def _items(response: dict, key: str) -> list[dict]:
    """Extrait la liste d'une réponse tracker.

    Parameters
    ----------
    response : dict
        Réponse JSON du tracker API.
    key : str
        Clé de la liste à partir de DHIS2 2.41 (``events``, ``enrollments``…).

    Returns
    -------
    list[dict]
        Éléments de la page ; repli sur ``instances`` (DHIS2 < 2.41).
    """
    return response.get(key, response.get("instances", []))


def _paginate(tracker: DHIS2, endpoint: str, key: str, params: dict) -> Iterator[list[dict]]:
    """Parcourt les pages d'un endpoint tracker, une page à la fois.

    Générateur : seule la page courante est gardée en mémoire, ce qui borne le
    JSON brut retenu quel que soit le volume du programme.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    endpoint : str
        Endpoint relatif (ex. ``tracker/trackedEntities``).
    key : str
        Clé de la liste dans la réponse (ex. ``trackedEntities``) ; repli sur
        ``instances`` pour DHIS2 < 2.41.
    params : dict
        Paramètres de requête, ``pageSize`` inclus (``page`` est géré ici).

    Yields
    ------
    list[dict]
        Éléments d'une page (la dernière peut être vide ou incomplète).
    """
    page = 1
    while True:
        response = tracker.api.get(endpoint, params={**params, "page": page})
        batch = _items(response, key)
        yield batch
        page_count = response.get("pager", {}).get("pageCount")
        if len(batch) < params["pageSize"] or (page_count and page >= page_count):
            break
        page += 1


def _fetch_by_uids(
    tracker: DHIS2,
    endpoint: str,
    key: str,
    params: dict,
    uid_param: str,
    uids: list[str] | None,
    page_size: int,
) -> list[dict]:
    """Extrait un endpoint, éventuellement restreint à une liste d'UID.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    endpoint, key : str
        Cf. :func:_paginate.
    params : dict
        Paramètres communs (sans page ni pageSize).
    uid_param : str
        Nom du paramètre de filtre UID (cf. :func:tracker_api_names).
    uids : list[str] | None
        UID à extraire ; None = tout le programme.
    page_size : int
        Taille de page de la pagination complète.

    Returns
    -------
    list[dict]
        Éléments extraits.
    """
    if uids is None:
        pages = _paginate(tracker, endpoint, key, {**params, "pageSize": page_size})
        return [item for page in pages for item in page]
    items: list[dict] = []
    size = config.UID_BATCH_SIZE
    sep = tracker_api_names(tracker)["uid_sep"]
    for i in range(0, len(uids), size):
        batch = uids[i : i + size]
        response = tracker.api.get(
            endpoint,
            params={**params, uid_param: sep.join(batch), "page": 1, "pageSize": len(batch)},
        )
        items.extend(_items(response, key))
    return items


def get_tracked_entities(
    tracker: DHIS2,
    program_uid: str,
    tracked_entity_ids: list[str] | None = None,
    page_size: int = config.TEI_PAGE_SIZE,
) -> pl.DataFrame:
    """Extrait les tracked entities d'un programme avec leurs attributs.

    Une ligne par TEI ; chaque attribut devient une colonne (clé =
    ``displayName``), exposant directement num_epid, sexe, âge, date de
    notification… pour l'agrégation.

    Parameters
    ----------
    tracker : DHIS2
        Client DHIS2 authentifié.
    program_uid : str
        UID du programme tracker.
    tracked_entity_ids : list[str], optional
        Restreint l'extraction à ces TEI (typiquement ceux référencés par les
        événements extraits). ``None`` = tous les TEI du programme.
    page_size : int, optional
        Taille de page de la pagination complète.

    Returns
    -------
    pl.DataFrame
        Une ligne par TEI : ``tracked_entity_id, organisation_unit_id,
        created_at, updated_at`` + une colonne par attribut. DataFrame vide
        (schéma minimal) si aucun TEI.
    """
    names = tracker_api_names(tracker)
    all_teis = _fetch_by_uids(
        tracker,
        "tracker/trackedEntities",
        "trackedEntities",
        {
            "program": program_uid,
            names["ou_mode"]: "ACCESSIBLE",
            "fields": "trackedEntity,orgUnit,createdAt,updatedAt,attributes",
        },
        names["trackedEntities"],
        tracked_entity_ids,
        page_size,
    )

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
    # df = df.select(
    #     pl.exclude(
    #         [
    #             "MVE - Nom du Chef de quartier",
    #             "MVE - Nom, post nom et prénom du cas",
    #             "002 MVE-FS-Nom et prénom du contact",
    #             "MVE - FC - Fiche remplie par",
    #             "003 MVE-FS-Chef de famille",
    #         ]
    #     )
    # )

    # Garde-fou : un TEI dupliqué démultiplierait les lignes à la jointure.
    return df.unique(subset="tracked_entity_id", keep="first").with_columns(
        pl.col(col).cast(pl.Datetime, strict=False).dt.date().alias(col)
        for col in ("created_at", "updated_at")
    )


def get_enrollments(
    tracker: DHIS2,
    program_uid: str,
    org_unit_parent: str = config.ORG_UNIT_PARENT,
    enrollment_ids: list[str] | None = None,
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
    enrollment_ids : list[str], optional
        Restreint l'extraction à ces enrôlements (typiquement ceux référencés
        par les événements extraits). ``None`` = tous les enrôlements.
    page_size : int, optional
        Taille de page de la pagination complète.

    Returns
    -------
    pl.DataFrame
        Une ligne par enrôlement, colonnes : ``enrollment_id, enrolled_at,
        enrollment_org_unit`` (``enrollment_id`` = clé de jointure côté
        événements). DataFrame vide (schéma typé) si aucun enrôlement.
    """
    names = tracker_api_names(tracker)
    enrollments = _fetch_by_uids(
        tracker,
        "tracker/enrollments",
        "enrollments",
        {"program": program_uid, "orgUnit": org_unit_parent, names["ou_mode"]: "DESCENDANTS"},
        names["enrollments"],
        enrollment_ids,
        page_size,
    )

    if not enrollments:
        return pl.DataFrame(
            schema={
                "enrollment_id": pl.String,
                "enrolled_at": pl.Datetime,
                "enrollment_org_unit": pl.String,
            }
        )
    rows = [
        {
            "enrollment_id": e.get("enrollment"),
            "enrolled_at": e.get("enrolledAt"),
            "enrollment_org_unit": e.get("orgUnit"),
        }
        for e in enrollments
    ]
    return (
        pl.DataFrame(rows)
        .unique(subset="enrollment_id", keep="first")
        .with_columns(pl.col("enrolled_at").cast(pl.Datetime, strict=False))
    )


def get_events(
    tracker: DHIS2,
    program_uid: str,
    org_unit_parent: str = config.ORG_UNIT_PARENT,
    page_size: int = config.PAGE_SIZE,
    occurred_after: str | None = None,
    occurred_before: str | None = None,
    updated_after: str | None = None,
    include_deleted: bool = True,
    log: Callable[[str], None] | None = None,
) -> pl.DataFrame:
    """Extrait les événements d'un programme au format **long**.

    Une ligne par couple (événement, data element). Un événement sans data value
    produit tout de même une ligne (``data_element_id``/``value`` nuls) afin de ne
    perdre aucun événement.

    Chaque page est convertie aussitôt en DataFrame (schéma fixe) puis libérée :
    on ne garde ni le JSON brut ni des millions de dicts Python. Seuls les
    champs utiles sont demandés (``config.EVENT_FIELDS``).

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
    log : Callable[[str], None], optional
        Journalise la progression toutes les ``config.LOG_EVERY_PAGES`` pages.

    Returns
    -------
    pl.DataFrame
        Colonnes : métadonnées d'événement (``event_id, status, program_id,
        tracked_entity_id, program_stage_id, enrollment_id,
        organisation_unit_id, occurred_at, created_at, updated_at, deleted``) +
        ``data_element_id, value``. DataFrame vide si aucun événement.
    """
    params = {
        "program": program_uid,
        "orgUnit": org_unit_parent,
        tracker_api_names(tracker)["ou_mode"]: "DESCENDANTS",
        "fields": config.EVENT_FIELDS,
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

    frames: list[pl.DataFrame] = []
    n_events = 0
    pages = _paginate(tracker, "tracker/events", "events", params)
    for n_page, events in enumerate(pages, start=1):
        n_events += len(events)
        if events:
            frames.append(_events_page_to_frame(events))
        if log and n_page % config.LOG_EVERY_PAGES == 0:
            n_rows = sum(f.height for f in frames)
            log(f"… {n_page} pages, {n_events} événements, {n_rows} lignes")

    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, rechunk=True)
    del frames
    return df.with_columns(
        pl.col(col).cast(pl.Datetime, strict=False).alias(col)
        for col in ("occurred_at", "created_at", "updated_at")
    )


_EVENT_SCHEMA = {
    "event_id": pl.String,
    "status": pl.String,
    "program_id": pl.String,
    "tracked_entity_id": pl.String,
    "program_stage_id": pl.String,
    "enrollment_id": pl.String,
    "organisation_unit_id": pl.String,
    "occurred_at": pl.String,
    "created_at": pl.String,
    "updated_at": pl.String,
    "deleted": pl.Boolean,
    "data_element_id": pl.String,
    "value": pl.String,
}


def _events_page_to_frame(events: list[dict]) -> pl.DataFrame:
    """Convertit une page d'événements en lignes longues (schéma fixe).

    Le schéma est imposé pour que toutes les pages se concatènent, même quand
    une colonne est entièrement nulle sur une page.

    Parameters
    ----------
    events : list[dict]
        Événements bruts d'une page du tracker API.

    Returns
    -------
    pl.DataFrame
        Une ligne par (événement, data element), colonnes de ``_EVENT_SCHEMA``.
    """
    rows: list[tuple] = []
    for event in events:
        common = (
            event.get("event"),
            event.get("status"),
            event.get("program"),
            event.get("trackedEntity"),
            event.get("programStage"),
            event.get("enrollment"),
            event.get("orgUnit"),
            event.get("occurredAt"),
            event.get("createdAt"),
            event.get("updatedAt"),
            event.get("deleted"),
        )
        data_values = event.get("dataValues", [])
        if not data_values:
            rows.append((*common, None, None))
        for dv in data_values:
            rows.append((*common, dv.get("dataElement"), dv.get("value")))
    return pl.DataFrame(rows, schema=_EVENT_SCHEMA, orient="row")


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

    return out.join(
        df_org_levels.rename({"id": "organisation_unit_id"}),
        on="organisation_unit_id",
        how="left",
    )


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
    log: Callable[[str], None] | None = None,
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
    log : Callable[[str], None], optional
        Journalise chaque étape (volumes), pour situer un éventuel échec.

    Returns
    -------
    pl.DataFrame
        Table longue enrichie, prête pour l'agrégation et l'upsert. DataFrame
        vide si aucun événement ne correspond aux filtres.
    """
    log = log or (lambda _msg: None)
    df_events = get_events(
        tracker,
        program_uid,
        org_unit_parent=org_unit_parent,
        occurred_after=occurred_after,
        occurred_before=occurred_before,
        updated_after=updated_after,
        log=log,
    )
    if df_events.is_empty():
        return df_events
    log(f"Événements : {df_events.height} lignes ({df_events['event_id'].n_unique()} événements)")

    df_enrollments = get_enrollments(
        tracker,
        program_uid,
        org_unit_parent=org_unit_parent,
        enrollment_ids=_distinct_ids(df_events, "enrollment_id"),
    )
    df_tei = get_tracked_entities(
        tracker,
        program_uid,
        tracked_entity_ids=_distinct_ids(df_events, "tracked_entity_id"),
    )
    log(f"Enrôlements : {df_enrollments.height} ; tracked entities : {df_tei.height}")

    df_events = add_value_norm(df_events, de_options_map, multi_de)
    log("Décodage value_norm terminé")
    df = enrich_events(df_events, df_enrollments, df_tei, df_stage_de, df_org_levels)
    log(f"Enrichissement : {df.height} lignes x {df.width} colonnes")
    return df


def _distinct_ids(df: pl.DataFrame, col: str) -> list[str]:
    """Liste triée des UID non nuls et distincts d'une colonne.

    Parameters
    ----------
    df : pl.DataFrame
        Table source (typiquement les événements extraits).
    col : str
        Colonne d'UID.

    Returns
    -------
    list[str]
        UID distincts, triés (requêtes déterministes).
    """
    return df.get_column(col).drop_nulls().unique().sort().to_list()
