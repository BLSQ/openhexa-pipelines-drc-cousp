from __future__ import annotations

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import config
import polars as pl
from data.loader import date_anomalies, load_raw
from data.metrics import compute
from data.model import SitRepData
from reporting import build_template, charts, render, zone_map
from reporting.narrative import load_narrative


def _default_output(reporting_end: date) -> Path:
    return config.DATA_DIR / f"SitRep_MVE_RDC_{reporting_end:%Y%m%d}.docx"


def build_sitrep(
    *,
    df: pl.DataFrame | None = None,
    csv_path: str | Path | None = None,
    template_path: str | Path = config.DEFAULT_TEMPLATE,
    output_path: str | Path | None = None,
    reporting_end: date | None = None,
    period_days: int = config.REPORTING_PERIOD_DAYS,
    publication_date: date | None = None,
    sitrep_number: str = config.SITREP_NUMBER,
    narrative_path: str | Path | None = None,
    assets_dir: str | Path | None = None,
    logger: Callable[[str], None] = print,
) -> tuple[Path, SitRepData]:
    """Construit le SitRep et renvoie (chemin_docx, indicateurs).

    Fournir soit ``df`` (DataFrame déjà nettoyé, p.ex. issu de DHIS2), soit
    ``csv_path`` (première itération sur l'extraction agrégée).

    Le rapport couvre la fenêtre de ``period_days`` jours se terminant le
    ``reporting_end`` (ex. 17-18 mai). Voir ``data.compute``.

    Returns:
        tuple[Path, SitRepData]: Le chemin du ``.docx`` généré et les
        indicateurs calculés.
    """
    if df is None:
        if csv_path is None:
            csv_path = config.DEFAULT_CSV
        logger(f"Chargement de l'extraction : {csv_path}")
        df = load_raw(csv_path)

    for col, info in date_anomalies(df).items():
        logger(
            f"⚠️ {info['count']} date(s) {col} hors plage "
            f"[{info['lo']}..{info['hi']}] : {', '.join(info['examples'])}"
        )

    data = compute(
        df,
        reporting_end=reporting_end,
        period_days=period_days,
        publication_date=publication_date,
        sitrep_number=sitrep_number,
    )
    logger(
        f"Période de rapportage : {data.reporting_label} "
        f"(publication {data.publication_date:%Y-%m-%d}) | "
        f"cumul confirmés : {data.kpi['cumul_confirmes']} | "
        f"nouveaux sur la période : {data.kpi['nouveaux_confirmes_periode']}"
    )

    prov_sum = sum(r["confirmes"] for r in data.province_summary[:-1])
    total = data.province_summary[-1]["confirmes"]
    if prov_sum != total:
        logger(f"AVERTISSEMENT : somme provinces ({prov_sum}) != total ({total})")

    template_path = Path(template_path)
    if not template_path.exists():
        logger(f"Template absent, génération : {template_path}")
        build_template.build(template_path)

    assets = Path(assets_dir) if assets_dir else Path(tempfile.mkdtemp(prefix="sitrep_"))
    logger("Génération des visuels (courbe épi, pyramide, carte)…")
    chart_paths = charts.build_all(data, assets)
    chart_paths["province_situation_map"] = zone_map.province_situation_map(data, assets)
    chart_paths["zone_situation_map"] = zone_map.zone_situation_map(data, assets)
    if chart_paths.get("zone_situation_map") is None:
        logger("AVERTISSEMENT : shapefile indisponible, carte omise.")

    narrative = load_narrative(narrative_path)
    output_path = Path(output_path) if output_path else _default_output(data.reporting_end)
    logger(f"Rendu du document : {output_path}")
    render.render(data, chart_paths, template_path, output_path, narrative)
    logger("SitRep généré avec succès.")
    return output_path, data
