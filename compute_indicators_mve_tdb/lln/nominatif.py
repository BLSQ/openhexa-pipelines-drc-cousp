"""LLN nominatives : la LLN partagée + tous les data elements et attributs TEI.

Les LLN nominatives (notifications et contacts) reprennent les colonnes des LLN
partagées, au même grain, puis y ajoutent **tous** les autres data elements et
attributs d'entité suivie présents dans la source, données personnelles
comprises (noms, téléphones, coordonnées GPS). Ces colonnes supplémentaires
sont publiées telles qu'en base (valeur décodée ``value_norm``, en texte),
sous le libellé DHIS2 normalisé : leur usage est laissé au consommateur.
"""

from __future__ import annotations

import re
import unicodedata

import polars as pl
from openhexa.sdk import current_run


def slugifier(texte: str) -> str:
    """Normalise un libellé DHIS2 en nom de colonne (minuscules ASCII, ``_``).

    Args:
        texte: Libellé source.

    Returns:
        Le nom normalisé, ex. « MVE - Nom, post nom et prénom du cas » ->
        ``mve_nom_post_nom_et_prenom_du_cas``.
    """
    ascii_texte = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_texte.lower()).strip("_")


def nommer_colonnes(
    sources: dict[str, str],
    existantes: set[str],
    suffixe: dict[str, str],
) -> dict[str, str]:
    """Attribue un nom de colonne unique à chaque source.

    Le nom est le libellé normalisé (cf. slugifier) ; en cas de collision avec
    une colonne existante ou déjà attribuée, on ajoute le suffixe de la source
    (UID du data element, ou ``tei`` pour un attribut). Les sources sont
    traitées par libellé puis identifiant, pour un résultat déterministe.

    Args:
        sources: ``{identifiant_source: libellé}``.
        existantes: Noms déjà pris (colonnes de la LLN partagée).
        suffixe: ``{identifiant_source: suffixe}`` utilisé en cas de collision.

    Returns:
        ``{identifiant_source: nom_de_colonne}``.
    """
    pris = set(existantes)
    noms: dict[str, str] = {}
    for source, libelle in sorted(sources.items(), key=lambda kv: (kv[1], kv[0])):
        nom = slugifier(libelle) or slugifier(source)
        if nom in pris:
            nom = f"{nom}_{slugifier(suffixe[source])}"
        rang = 2
        base = nom
        while nom in pris:
            nom = f"{base}_{rang}"
            rang += 1
        pris.add(nom)
        noms[source] = nom
    return noms


def ajouter_colonnes_nominatives(
    lln: pl.DataFrame,
    valeurs_de: pl.DataFrame,
    cle_de: str,
    de_names: dict[str, str],
    tei: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, dict[str, str]]]:
    """Ajoute à une LLN partagée ses data elements et attributs TEI manquants.

    Args:
        lln: LLN partagée (schéma publié, grain inchangé).
        valeurs_de: ``cle_de`` + une colonne par data element supplémentaire
            (nom = UID), au même grain que ``lln``.
        cle_de: Clé de jointure des data elements (``enrollment_id`` ou
            ``event_id``).
        de_names: ``{data_element_id: libellé}`` (cf. load_data_element_names).
        tei: ``tracked_entity_id`` + une colonne par attribut supplémentaire
            (nom = libellé DHIS2).

    Returns:
        La LLN nominative (colonnes de ``lln``, puis data elements, puis
        attributs, chaque bloc trié par nom) et le dictionnaire des colonnes
        ajoutées ``{colonne: {"source", "uid"?, "libelle"}}``.
    """
    uids = [col for col in valeurs_de.columns if col != cle_de]
    attributs = [col for col in tei.columns if col != "tracked_entity_id"]
    sources = {uid: de_names.get(uid, uid) for uid in uids} | {
        f"tei:{att}": att for att in attributs
    }
    suffixes = {uid: uid for uid in uids} | {f"tei:{att}": "tei" for att in attributs}
    noms = nommer_colonnes(sources, set(lln.columns), suffixes)

    noms_de = {uid: noms[uid] for uid in uids}
    noms_tei = {att: noms[f"tei:{att}"] for att in attributs}
    nominatif = lln.join(valeurs_de.rename(noms_de), on=cle_de, how="left").join(
        tei.rename(noms_tei), on="tracked_entity_id", how="left"
    )
    if nominatif.height != lln.height:
        raise ValueError(
            f"LLN nominative : {nominatif.height} lignes au lieu de {lln.height} "
            f"(clé {cle_de} ou tracked_entity_id dupliquée côté valeurs)."
        )
    nominatif = nominatif.select(
        [*lln.columns, *sorted(noms_de.values()), *sorted(noms_tei.values())]
    )

    dictionnaire = {
        nom: {"source": "data_element", "uid": uid, "libelle": de_names.get(uid, uid)}
        for uid, nom in noms_de.items()
    } | {nom: {"source": "attribut_tei", "libelle": att} for att, nom in noms_tei.items()}
    current_run.log_info(
        f"LLN nominative : {nominatif.height} lignes, {lln.width} colonnes partagées "
        f"+ {len(noms_de)} data element(s) + {len(noms_tei)} attribut(s) TEI."
    )
    return nominatif, dict(sorted(dictionnaire.items()))


def pivot_data_elements(events: pl.DataFrame, cle: str, exclus: list[str]) -> pl.DataFrame:
    """Pivote les data elements non publiés par la LLN partagée, au grain ``cle``.

    Même règle que les LLN partagées : dernière valeur décodée (``value_norm``)
    par data element, dans l'ordre de saisie.

    Args:
        events: Événements au format long (``data_element_id``, ``value_norm``).
        cle: Grain du pivot (``event_id`` pour les contacts).
        exclus: Data elements déjà publiés par la LLN partagée.

    Returns:
        ``cle`` + une colonne par data element supplémentaire (nom = UID).
    """
    supplementaires = events.filter(
        pl.col("data_element_id").is_not_null() & ~pl.col("data_element_id").is_in(exclus)
    )
    if supplementaires.is_empty():
        return events.select(cle).unique()
    return supplementaires.sort([cle, "created_at"], nulls_last=True).pivot(
        on="data_element_id",
        index=cle,
        values="value_norm",
        aggregate_function="last",
    )
