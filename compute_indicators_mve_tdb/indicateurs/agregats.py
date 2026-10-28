"""Agrégation des indicateurs par date, géographie, sexe et tranche d'âge."""

from __future__ import annotations

import config
import pandas as pd
import polars as pl
from openhexa.sdk import current_run
from utils import (
    compter_oui,
)

from indicateurs.cas import reconstruct_date_deces, to_naive_datetime

_AGG_FUNCS: dict[str, object] = {"sum": "sum", "nunique": "nunique", "compter_oui": compter_oui}


INDICATEURS_AGG_SPEC: dict[str, tuple[str, object]] = {
    nom: (col, _AGG_FUNCS[fn]) for nom, (col, fn) in config.INDICATEURS_AGG_SPEC.items()
}


def aggregate_rapportage(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
) -> pd.DataFrame:
    """Agrège les indicateurs pour l'axe « date_rapportage », une date par indicateur.

    Contrairement aux 3 autres axes (une seule colonne de date partagée par
    tous les indicateurs), chaque indicateur est ici rattaché à la date de
    l'événement qui le détermine (config.RAPPORTAGE_DATE_SOURCE) : ex.
    date_confirmation pour n_confirmes, date_deces (reconstruite) pour
    n_deces, date_sortie_cte pour n_gueri. Les indicateurs partageant la même
    date source sont agrégés ensemble en une seule passe, puis les résultats
    par date source sont fusionnés (jointure externe) sur (date_rapportage,
    aire_sante, zone_sante, province, sexe_norm, tranche_age, geo_hierarchie),
    les valeurs manquantes étant complétées à 0. Grain le plus bas (aire de
    santé) avec les colonnes parentes (zone_sante, province) pour permettre
    des agrégations à des niveaux plus élevés côté consommateurs (ex.
    sitrep_v2). Rattache enfin les coordonnées ZS et province, comme
    aggregate_indicators() (au niveau zone de santé : aucune géométrie par
    aire de santé n'est nécessaire pour l'instant).

    Args:
        indicators: Liste de ligne enrichie issue de compute_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).

    Returns:
        Les agrégats, une ligne par (date_rapportage, aire de santé, ZS,
        province, sexe, tranche d'âge).
    """
    df = indicators.copy()
    for col in (
        "date_notif",
        "date_heure_investigation",
        "date_prelevement",
        "date_reception_labo",
        "date_analyse_labo",
        "date_dernier_test",
        "date_confirmation",
        "date_sortie_cte",
        "date_debut_signes_invest",
    ):
        df[col] = to_naive_datetime(df[col])
    df["date_preleves_calc"] = df["date_prelevement"].fillna(df["date_reception_labo"])
    df["date_deces"] = to_naive_datetime(reconstruct_date_deces(df))

    dims = ["aire_sante", "zone_sante", "province", "sexe_norm", "tranche_age", "geo_hierarchie"]

    par_date: dict[str, dict[str, tuple[str, object]]] = {}
    for indicateur, date_col in config.RAPPORTAGE_DATE_SOURCE.items():
        par_date.setdefault(date_col, {})[indicateur] = INDICATEURS_AGG_SPEC[indicateur]

    merged: pd.DataFrame | None = None
    for date_col, agg_spec in par_date.items():
        sub = df[df["is_deces"]] if date_col == "date_deces" else df
        sub = sub[sub[date_col].notna()]
        grouped = (
            sub.groupby([date_col, *dims], dropna=False)
            .agg(**agg_spec)
            .reset_index()
            .rename(columns={date_col: "date_rapportage"})
        )
        merged = (
            grouped
            if merged is None
            else merged.merge(grouped, on=["date_rapportage", *dims], how="outer")
        )

    valeurs = [c for c in merged.columns if c not in ("date_rapportage", *dims)]
    merged[valeurs] = merged[valeurs].fillna(0).astype(int)
    merged = merged.sort_values(["date_rapportage", "province", "zone_sante"]).reset_index(
        drop=True
    )

    merged = merged.merge(
        ou_zone_sante.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_zs"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )
    merged["geo_hierarchie"] = merged["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:2])
    )
    merged = merged.merge(
        ou_provinces.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_province"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    ).drop(columns="geo_hierarchie")

    current_run.log_info(f"Agrégation sur « date_rapportage » : {len(merged)} lignes.")
    return merged


def aggregate_indicators(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
    colonne_date: str,
) -> pd.DataFrame:
    """Agrège les indicateurs par date, géographie, sexe et tranche d'âge.

    Pour l'axe « date_deces », reconstruit d'abord une date de décès unique
    (date finale, sinon notification, sinon proxy prélèvement/notification) et
    restreint aux décès. Rattache enfin les coordonnées ZS et province.

    Args:
        indicators: Liste de ligne enrichie issue de compute_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).
        colonne_date: Axe temporel d'agrégation (date_notif, date_debut_symptomes
            ou date_deces).

    Returns:
        Les agrégats, une ligne par (date, ZS, province, sexe, tranche d'âge).
    """
    if colonne_date == "date_rapportage":
        return aggregate_rapportage(indicators, ou_zone_sante, ou_provinces)

    if colonne_date == "date_deces":
        indicators = indicators.copy()
        indicators["date_deces"] = to_naive_datetime(reconstruct_date_deces(indicators))
        indicators = indicators[indicators["is_deces"]]

    group_keys = [
        colonne_date,
        "zone_sante",
        "province",
        "sexe_norm",
        "tranche_age",
        "geo_hierarchie",
    ]

    aggregated = (
        indicators.groupby(group_keys, dropna=False)
        .agg(**INDICATEURS_AGG_SPEC)
        .reset_index()
        .sort_values([colonne_date, "province", "zone_sante"])
        .reset_index(drop=True)
    )

    aggregated = aggregated.merge(
        ou_zone_sante.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_zs"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )
    # Réduit la hiérarchie ZS → province pour rattacher les coordonnées province
    aggregated["geo_hierarchie"] = aggregated["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:2])
    )
    aggregated = aggregated.merge(
        ou_provinces.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_province"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    ).drop(columns="geo_hierarchie")
    current_run.log_info(f"Agrégation sur « {colonne_date} » : {len(aggregated)} lignes.")
    return aggregated
