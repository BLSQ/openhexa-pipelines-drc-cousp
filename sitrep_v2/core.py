from __future__ import annotations

import re
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import config
from data.loader import filter_provinces, load_dataset
from data.metrics import compute
from data.model import SitRepData
from reporting import charts, render, zone_map
from reporting.narrative import load_narrative


def _default_output(reporting_end: date, slug: str = "") -> Path:
    suffix = f"_{slug}" if slug else ""
    return config.DATA_DIR / f"SitRep_MVE_RDC_{reporting_end:%Y%m%d}{suffix}.docx"


def _province_scope(provinces: list[str] | None) -> tuple[str | None, str]:
    """Construit le libellé de portée et le slug de fichier depuis les provinces choisies.

    Remplace ``core._zone_scope`` de v1 (filtre ``zone_sante``) par un filtre
    ``province`` — les provinces sont déjà canonisées à la source, aucune
    canonisation supplémentaire nécessaire ici.

    Returns:
        tuple[str | None, str]: ``(scope_label, slug)`` — ``(None, "")`` si
        aucun choix (rapport national).
    """
    chosen = sorted({p.strip() for p in (provinces or []) if p and p.strip()})
    if not chosen:
        return None, ""
    intitule = "Province" if len(chosen) == 1 else "Provinces"
    scope_label = f"{intitule} : {', '.join(chosen)}"
    slug = "-".join(re.sub(r"[^\w]+", "_", p).strip("_") for p in chosen)
    return scope_label, slug


def build_sitrep(
    *,
    template_path: str | Path = config.DEFAULT_TEMPLATE,
    output_path: str | Path | None = None,
    reporting_end: date | None = None,
    period_days: int = config.REPORTING_PERIOD_DAYS,
    publication_date: date | None = None,
    sitrep_number: str = config.SITREP_NUMBER,
    provinces: list[str] | None = None,
    narrative_path: str | Path | None = None,
    assets_dir: str | Path | None = None,
    logger: Callable[[str], None] = print,
) -> tuple[Path, SitRepData]:
    """Construit le SitRep et renvoie (chemin_docx, indicateurs).

    Le rapport couvre la fenêtre de ``period_days`` jours se terminant le
    ``reporting_end``. Voir ``data.metrics.compute``.

    ``provinces`` (vide/``None`` → rapport national) restreint l'ensemble du
    rapport aux provinces demandées : tous les indicateurs, tableaux, visuels
    et faits saillants sont recalculés sur ce sous-ensemble (remplace le
    filtre ``zone_sante`` de v1).

    Returns:
        tuple[Path, SitRepData]: Le chemin du ``.docx`` généré et les
        indicateurs calculés.
    """
    logger(f"Chargement du dataset « {config.DATASET_SLUG} » (workspace « {config.DATASET_SOURCE_WORKSPACE} »)…")
    rapportage, dds_agg = load_dataset()

    scope_label, slug = _province_scope(provinces)
    if scope_label:
        rapportage = filter_provinces(rapportage, provinces)
        dds_agg = filter_provinces(dds_agg, provinces)
        logger(f"Filtrage par {scope_label} → {rapportage.height} ligne(s) retenue(s).")
        if rapportage.height == 0:
            logger("AVERTISSEMENT : aucune ligne pour les provinces demandées (rapport vide).")

    data = compute(
        rapportage,
        dds_agg,
        reporting_end=reporting_end,
        period_days=period_days,
        publication_date=publication_date,
        sitrep_number=sitrep_number,
        scope_label=scope_label,
    )
    logger(
        f"Période de rapportage : {data.reporting_label} "
        f"(publication {data.publication_date:%Y-%m-%d}) | "
        f"cumul confirmés : {data.kpi['cumul_confirmes']} | "
        f"nouveaux sur la période : {data.kpi['nouveaux_confirmes_periode']}"
    )

    template_path = Path(template_path)
    if not template_path.exists():
        raise FileNotFoundError(f"Template introuvable : {template_path}")

    assets = Path(assets_dir) if assets_dir else Path(tempfile.mkdtemp(prefix="sitrep_v2_"))
    logger("Génération des visuels (courbe épi, pyramide, carte)…")
    chart_paths = charts.build_all(data, assets)
    chart_paths["zone_situation_map"] = zone_map.zone_situation_maps(data, assets)
    if chart_paths.get("zone_situation_map") is None:
        logger("AVERTISSEMENT : shapefile indisponible, carte omise.")

    narrative = load_narrative(narrative_path)
    output_path = Path(output_path) if output_path else _default_output(data.reporting_end, slug)
    logger(f"Rendu du document : {output_path}")
    render.render(data, chart_paths, template_path, output_path, narrative)
    logger("SitRep généré avec succès.")
    return output_path, data
