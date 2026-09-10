"""Structure de sortie de la couche données : ``SitRepData``.

Simplifiée par rapport à v1 (``sitrep/code/generate_sitrep/data/model.py``) :
ne garde que les champs alimentant les marqueurs du nouveau template —
retirés : ``surveillance_indics``, ``labo_indics``, ``prise_en_charge_indics``,
``mouvement_indics``, ``agesex_crosstab`` (tableaux absents du nouveau
template).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import polars as pl


@dataclass(frozen=True)
class SitRepData:
    """Tous les indicateurs calculés du SitRep (consommés par reporting.*)."""

    # Le SitRep couvre une fenêtre de rapportage de plusieurs jours (2 par
    # défaut), publiée à une date d'élaboration ultérieure.
    reporting_start: date
    reporting_end: date
    reporting_label: str  # ex. "17-18 mai 2026"
    publication_date: date
    sitrep_number: str
    provinces_touchees: list[str]
    zones_by_province: dict[str, list[str]]
    zones_atteintes: dict  # {province: {"touchees": k, "total": N}}
    aires_atteintes: dict  # {"touchees": k, "total": N} — national (cf. B.4.1)
    kpi: dict[str, object]
    # Tableau 1 (par province) : cumul (cas/décès/CFR) + jour (nouveaux cas) +
    # ZS touchées/total. Une ligne par province touchée + ligne "Total".
    tableau1: list[dict]
    tableau1_total: dict
    # Tableau 2 (par province > zone de santé) : une ligne « province »
    # (résumé, ``is_province=True``) suivie de ses ZS (ordre alphabétique) —
    # cumul (cas/décès/CFR) + jour (nouveaux cas, décès communautaires/
    # intra-CTE, total décès), provinces triées par cas cumulés décroissants.
    tableau2: list[dict]
    tableau2_total: dict
    # [[ACTIONS_LABORATOIRE]] : par province (jour), triées par nb d'analyses
    # décroissant — {province, vivants, deces, positifs, analyses, positivite}.
    labo_par_province: list[dict]
    # Provinces touchées sans aucune analyse labo sur la fenêtre (jour) —
    # dernière puce de [[ACTIONS_LABORATOIRE]].
    provinces_sans_labo: list[str]
    # [[ACTIONS_SURVEILLANCE]] : alertes/suspects (cumul, national).
    surveillance_stats: dict
    # Faits saillants « à date » (situation de la période, non cumulée).
    nouveaux_par_zone: list[dict]  # {province, zone, n} confirmés sur la période
    nouvelles_zones: list[dict]  # {province, zone} nouvellement touchées
    agesex_pyramid: dict  # {"Masculin": [..par age..], "Féminin": [...]} (cumul)
    agesex_pyramid_jour: dict  # idem, jour (24h)
    # Courbe épi (DDS_Agg, par date de début des symptômes) : split vivant/décédé.
    epi_curve: list[tuple[date, int, int]]  # (date, n_vivants, n_deces)
    # Portée du rapport : ``None`` = national ; sinon libellé de la/les
    # province(s) filtrée(s) (ex. « Province : Ituri »), ajouté au titre par
    # ``render``.
    scope_label: str | None = None
    raw: pl.DataFrame | None = field(repr=False, default=None)  # Rapportage, cumul filtré
    raw_day: pl.DataFrame | None = field(repr=False, default=None)  # Rapportage, jour (24h)
    raw_dds: pl.DataFrame | None = field(repr=False, default=None)  # DDS_Agg, cumul filtré
