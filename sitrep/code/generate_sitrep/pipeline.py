"""Pipeline OpenHexa : génération du SitRep MVE RDC.

Source : table SQL agrégée du workspace (``config.AGG_TABLE``, grain événement).
L'utilisateur ne choisit que la fenêtre de rapportage (fin + nombre de jours),
et, optionnellement, le fichier et le dataset de sortie.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import config
from core import build_sitrep
from openhexa.sdk import (
    Dataset,
    current_run,
    parameter,
    pipeline,
    workspace,
)

from data import build_definitive_data


@pipeline("Generate SitRep MVE")
@parameter(
    "reporting_end",
    type=str,
    name="Fin de la fenêtre de rapportage (YYYY-MM-DD)",
    help="Dernier jour couvert (champ date_report). Défaut = date max de la table.",
    required=False,
)
@parameter(
    "period_days",
    type=int,
    name="Nombre de jours du rapport",
    help="Largeur de la fenêtre de rapportage (en jours).",
    default=2,
    required=False,
)
@parameter(
    "dst_file",
    type=str,
    name="Fichier de sortie (.docx)",
    help="Chemin du SitRep dans le workspace. Calculé par défaut si vide.",
    required=False,
)
@parameter(
    "dst_dataset",
    type=Dataset,
    name="Dataset de sortie",
    help="Dataset OpenHexa où publier le SitRep (optionnel).",
    required=False,
)
def generate_sitrep_pipeline(
    reporting_end: str | None = None,
    period_days: int = config.REPORTING_PERIOD_DAYS,
    dst_file: str | None = None,
    dst_dataset: Dataset | None = None,
) -> None:
    """Génère le SitRep depuis la table SQL et le publie dans le workspace / dataset."""
    rep_end = datetime.strptime(reporting_end, "%Y-%m-%d").date() if reporting_end else None

    current_run.log_info(f"Lecture de la table « {config.AGG_TABLE} » (base du workspace)…")
    df = build_definitive_data()

    output_path = Path(dst_file) if dst_file else None
    out, data = build_sitrep(
        df=df,
        template_path=config.DEFAULT_TEMPLATE,
        output_path=output_path,
        reporting_end=rep_end,
        period_days=period_days,
        sitrep_number=config.SITREP_NUMBER,
        assets_dir=Path(workspace.files_path) / "pipelines/sitrep/assets",
        logger=current_run.log_info,
    )

    current_run.add_file_output(str(out))
    current_run.log_info(
        f"SitRep généré : {out.name} "
        f"(cumul confirmés = {data.kpi['cumul_confirmes']}, "
        f"provinces touchées = {', '.join(data.provinces_touchees)})"
    )

    if dst_dataset is not None:
        _publish_to_dataset(dst_dataset, out)


def _publish_to_dataset(dataset: Dataset, doc_path: Path) -> None:
    version = dataset.create_version(f"SitRep {datetime.now().strftime('%Y-%m-%d_%H:%M')}")
    version.add_file(str(doc_path), filename=doc_path.name)
    current_run.log_info(f"SitRep publié dans le dataset {dataset.name}.")


if __name__ == "__main__":
    generate_sitrep_pipeline()
