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

import anthropic
import config
from core import build_sitrep
from openhexa.sdk import (
    CustomConnection,
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
    "use_ai_narrative",
    type=bool,
    name="Rédaction par IA (résumé + conclusion)",
    help=(
        "Si activé (et une connexion IA renseignée), [[RESUME_POINTS_CLES]] et "
        "[[CONCLUSION]] sont rédigés par un modèle Claude à partir des indicateurs "
        "déjà calculés (jamais recalculés), avec repli automatique sur le texte "
        "habituel en cas d'échec ou de chiffre non reconnu."
    ),
    default=False,
    required=False,
)
@parameter(
    "ai_connection",
    type=CustomConnection,
    name="Connexion IA (clé API Anthropic)",
    help="Connexion personnalisée portant un champ « api_key ». Requise si l'option ci-dessus est activée.",
    required=False,
)
def sitrep_v2(
    reporting_end: str | None = None,
    period_days: int = config.REPORTING_PERIOD_DAYS,
    province: list[str] | None = None,
    use_ai_narrative: bool = False,
    ai_connection: CustomConnection | None = None,
) -> None:
    """Génère le SitRep depuis le dataset et le publie dans le workspace."""
    rep_end = datetime.strptime(reporting_end, "%Y-%m-%d").date() if reporting_end else None

    if province:
        current_run.log_info(f"Portée demandée : {', '.join(province)}")

    ai_client = None
    if use_ai_narrative:
        if ai_connection is None:
            current_run.log_info(
                "AVERTISSEMENT : rédaction par IA demandée mais aucune connexion "
                "fournie — texte habituel utilisé."
            )
        else:
            ai_client = anthropic.Anthropic(api_key=ai_connection.api_key)

    out, data = build_sitrep(
        template_path=config.DEFAULT_TEMPLATE,
        output_path=None,
        reporting_end=rep_end,
        period_days=period_days,
        sitrep_number=config.SITREP_NUMBER,
        provinces=province,
        assets_dir=Path(workspace.files_path) / "pipelines/sitrep/assets",
        ai_client=ai_client,
        logger=current_run.log_info,
    )

    current_run.add_file_output(str(out))
    current_run.log_info(
        f"SitRep généré : {out.name} "
        f"(cumul confirmés = {data.kpi['cumul_confirmes']}, "
        f"provinces touchées = {', '.join(data.provinces_touchees)})"
    )


if __name__ == "__main__":
    sitrep_v2()
