"""Pipeline OpenHexa : génération du SitRep MVE RDC (v2, nouveau template).

Source : dataset OpenHexa ``config.DATASET_SLUG`` (2 fichiers déjà agrégés,
``COD_MVE_Tracker_Rapportage``/``COD_MVE_Tracker_DDS_Agg``, publiés par le
pipeline ``compute_indicators_mve_tdb`` du workspace
``config.DATASET_SOURCE_WORKSPACE``), lu en cross-workspace via
``data/loader.py`` (pas de paramètre ``Dataset`` ici). Contrairement à v1
(``sitrep/code/generate_sitrep``), aucune connexion DHIS2/base SQL directe :
ce pipeline tourne dans le workspace ``drc-sgi-mve-17``, distinct de celui où
vit ``compute_indicators_mve_tdb``.
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


@pipeline(name="MVE17 - Génération du Sitrep")
@parameter(
    "reporting_end",
    type=str,
    name="Fin de la fenêtre de rapportage (YYYY-MM-DD)",
    help="Dernier jour couvert (champ date_rapportage). Défaut = date max de la table.",
    required=False,
)
@parameter(
    "period_days",
    type=int,
    name="Nombre de jours du rapport",
    help="Largeur de la fenêtre de rapportage (en jours).",
    default=1,
    required=False,
)
@parameter(
    "province",
    type=str,
    name="Province(s)",
    help="Restreint le SitRep aux provinces choisies. Vide = rapport national.",
    choices=["Ituri", "Nord-Kivu", "Sud-Kivu", "Tshopo", "Haut-Uélé", "Bas-Uélé"],
    multiple=True,
    required=False,
)
@parameter(
    "template_file",
    type=str,
    name="Nom du fichier template (.docx)",
    help=(
        "Nom du fichier dans pipelines/sitrep/template/ du workspace. "
        "Vide = valeur par défaut (config.DEFAULT_TEMPLATE)."
    ),
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
def sitrep_v2(
    reporting_end: str | None = None,
    period_days: int = config.REPORTING_PERIOD_DAYS,
    province: list[str] | None = None,
    template_file: str | None = None,
    dst_file: str | None = None,
    dst_dataset: Dataset | None = None,
) -> None:
    """Génère le SitRep depuis le dataset et le publie dans le workspace / dataset."""
    rep_end = datetime.strptime(reporting_end, "%Y-%m-%d").date() if reporting_end else None

    if province:
        current_run.log_info(f"Portée demandée : {', '.join(province)}")

    template_path = (
        Path(workspace.files_path) / "pipelines/sitrep/template" / template_file
        if template_file
        else config.DEFAULT_TEMPLATE
    )
    if template_file:
        current_run.log_info(f"Template : {template_file} (paramètre)")

    output_path = Path(dst_file) if dst_file else None
    out, data = build_sitrep(
        template_path=template_path,
        output_path=output_path,
        reporting_end=rep_end,
        period_days=period_days,
        sitrep_number=config.SITREP_NUMBER,
        provinces=province,
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
    sitrep_v2()
