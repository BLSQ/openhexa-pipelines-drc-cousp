"""Structure de sortie de la couche données : ``SitRepData``."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import polars as pl


@dataclass(frozen=True)
class SitRepData:
    """Tous les indicateurs calculés du SitRep (consommés par reporting.*)."""

    # Le SitRep couvre une fenêtre de rapportage de plusieurs jours (2 par
    # défaut, ex. 17-18 mai), publiée à une date d'élaboration ultérieure.
    reporting_start: date
    reporting_end: date
    reporting_label: str  # ex. "17-18 mai 2026"
    publication_date: date
    sitrep_number: str
    provinces_touchees: list[str]
    zones_by_province: dict[str, list[str]]
    kpi: dict[str, object]
    province_summary: list[dict]  # lignes + ligne Total
    # Faits saillants « à date » (situation de la période, non cumulée).
    nouveaux_par_zone: list[dict]  # {province, zone, n} confirmés sur la période
    nouvelles_zones: list[dict]  # {province, zone} nouvellement touchées
    zones_atteintes: dict  # {province: {"touchees": k, "total": N}}
    # Distribution spatiale par province (style PDF 3.1) + ligne Total.
    distribution_spatiale: list[dict]
    tableau1: list[dict]
    tableau1_total: dict
    tableau2: list[dict]
    tableau2_total: dict
    agesex_pyramid: dict  # {"Masculin": [..par age..], "Feminin": [...]}
    agesex_crosstab: list[dict]
    epi_curve: list[tuple[date, int]]
    # Tableaux « par pilier » (section 4) — indicateurs « à date » / période.
    surveillance_indics: dict  # gestion des alertes (Tableau III)
    labo_indics: dict  # indicateurs laboratoire (Tableau V)
    prise_en_charge_indics: dict  # indicateurs de prise en charge (Tableau VI)
    raw: pl.DataFrame | None = field(repr=False, default=None)
