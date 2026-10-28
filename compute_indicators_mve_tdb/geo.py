"""Unités d'organisation : géométries par niveau et référentiel géographique."""

from __future__ import annotations

import json
from typing import Literal

import pandas as pd
import polars as pl
from openhexa.sdk import current_run
from utils import (
    parse_geo,
)


def prepare_org_units(
    org_units: pl.DataFrame,
    niveau: Literal["province", "zone_sante", "aire_sante"],
) -> pl.DataFrame:
    """Prépare les unités d'organisation d'un niveau donné.

    Filtre sur le niveau hiérarchique, reconstruit la hiérarchie géographique et
    extrait l'anneau extérieur du polygone, sérialisé en JSON (jointure carto).

    Pour « aire_sante », contrairement aux 2 autres niveaux, les lignes sans
    géométrie sont conservées (`coordinates` à None) plutôt que filtrées : la
    géométrie n'y est pas systématique (~92-99% selon la province, vérifié sur
    le référentiel DHIS2), l'écarter fausserait un comptage par province.

    Args:
        org_units: Unités d'organisation issues de la toolbox DHIS2.
        niveau: « province » (level 2), « zone_sante » (level 3) ou
            « aire_sante » (level 4).

    Returns:
        Les unités du niveau demandé, avec geo_hierarchie et coordinates (JSON).
    """
    if niveau == "aire_sante":
        level, cols_geo = 4, ["level_1_name", "level_2_name", "level_3_name", "level_4_name"]
    elif niveau == "zone_sante":
        level, cols_geo = 3, ["level_1_name", "level_2_name", "level_3_name"]
    else:
        level, cols_geo = 2, ["level_1_name", "level_2_name"]

    def _anneau_exterieur(geom: object) -> object:
        """Anneau extérieur du polygone (ZS/AS imbriquées d'un niveau de plus).

        Returns:
            La liste de coordonnées de l'anneau extérieur, ou None si absente.
        """
        if not isinstance(geom, str):
            return None
        coords = json.loads(geom)["coordinates"][0]
        return coords[0] if niveau in ("zone_sante", "aire_sante") else coords

    filtre = pl.col("level") == level
    if niveau != "aire_sante":
        filtre = filtre & pl.col("geometry").is_not_null()

    base = org_units.filter(filtre).with_columns(
        pl.concat_str(cols_geo, separator=" / ").alias("geo_hierarchie"),
        pl.col("geometry")
        .map_elements(_anneau_exterieur, return_dtype=pl.Object)
        .alias("coordinates"),
    )
    prepared = base.with_columns(
        pl.col("coordinates").map_elements(json.dumps, return_dtype=pl.Utf8).alias("coordinates")
    )
    if niveau == "aire_sante":
        n_avec_geometrie = base.select(pl.col("coordinates").is_not_null().sum()).item()
        current_run.log_info(
            f"Unités d'organisation « aire_sante » préparées : {prepared.height} "
            f"(dont {n_avec_geometrie} avec géométrie)."
        )
    else:
        current_run.log_info(
            f"Unités d'organisation « {niveau} » préparées : {prepared.height} géométries."
        )
    return prepared


def build_geo_referentiel(ou_aire_sante: pl.DataFrame) -> pd.DataFrame:
    """Référentiel géographique complet (province, zone_sante, aire_sante).

    Une ligne par aire de santé du référentiel DHIS2, y compris celles sans
    aucune activité tracker — indépendant du grain daté des 4 tables agrégées
    (aucune colonne date/sexe/tranche d'âge), pour qu'un consommateur (ex.
    sitrep_v2) puisse calculer un total par province ou par zone de santé sans
    dépendre de la présence de cas dans les données.

    Args:
        ou_aire_sante: Unités d'organisation aire de santé (référentiel complet).

    Returns:
        Une ligne par aire de santé, colonnes (province, zone_sante, aire_sante).
    """
    geo = ou_aire_sante["geo_hierarchie"].to_pandas().apply(parse_geo).apply(pd.Series)
    referentiel = geo[["province", "zone_sante", "aire_sante"]].reset_index(drop=True)
    current_run.log_info(f"Référentiel géographique préparé : {len(referentiel)} aires de santé.")
    return referentiel
