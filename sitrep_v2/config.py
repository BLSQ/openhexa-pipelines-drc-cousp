from __future__ import annotations

from contextlib import suppress
from datetime import date as _date
from pathlib import Path

from openhexa.sdk import workspace


def _resolve_layout() -> tuple[Path, Path, Path, str]:
    """Détecte le layout des fichiers : OpenHexa (workspace monté) ou dépôt local.

    - **OpenHexa** : ``<workspace>/pipelines/sitrep/generated_files`` pour
      les sorties, comme ``<workspace>/pipelines/sitrep/geometry`` pour la
      géométrie et ``<workspace>/pipelines/sitrep/template`` pour le
      template — tout partagé avec v1 (`sitrep/code/generate_sitrep`), pas
      de dossier `sitrep_v2` distinct.
    - **Local** : ``./workspace/pipelines/sitrep/generated_files`` et
      ``./workspace/pipelines/sitrep/geometry`` (même convention que
      ci-dessus), pour les tests hors OpenHexa avec un `workspace.yaml`
      local (`files: path: ./workspace`).

    Returns:
        tuple: ``(repo_root, output_dir, geom_dir, template_name)``.
    """
    root = Path(workspace.files_path) if _files_path_available() else Path(__file__).resolve().parent / "workspace"
    return (
        root,
        root / "pipelines/sitrep/generated_files",
        root / "pipelines/sitrep/geometry",
        "Template_SitRep_MVE_nouveau_v2.docx",
    )


def _files_path_available() -> bool:
    with suppress(Exception):
        return Path(workspace.files_path).exists()
    return False


(REPO_ROOT, DATA_DIR, _GEOM_DIR, _TEMPLATE_NAME) = _resolve_layout()

TEMPLATE_DIR = REPO_ROOT / "pipelines/sitrep/template"
DEFAULT_TEMPLATE = TEMPLATE_DIR / _TEMPLATE_NAME

DEFAULT_SHAPEFILE = _GEOM_DIR / "zone_sante.parquet"
DEFAULT_SHAPEFILE_FALLBACK = _GEOM_DIR / "zone_sante.geojson"
DEFAULT_PROVINCES_SHAPEFILE = _GEOM_DIR / "provinces.parquet"
PROVINCES_NAME_SUFFIX = " Province"
SHAPE_NAME_SUFFIX = " Zone de Santé"

DATASET_SLUG = "sgi-mve-17"
DATASET_SOURCE_WORKSPACE = "drc-cousp-e26493"
RAPPORTAGE_FILE = "COD_MVE_Tracker_Rapportage.parquet"
DDS_AGG_FILE = "COD_MVE_Tracker_DDS_Agg.parquet"

NARRATIVE_YAML = REPO_ROOT / "pipelines/sitrep/template" / "narrative.yaml"

ND = "ND"

PROVINCE_CANONICAL = {
    "ituri": "Ituri",
    "nord kivu": "Nord-Kivu",
    "nord-kivu": "Nord-Kivu",
    "sud kivu": "Sud-Kivu",
    "sud-kivu": "Sud-Kivu",
    "kinshasa": "Kinshasa",
    "tshopo": "Tshopo",
    "haut uele": "Haut-Uélé",
    "haut-uele": "Haut-Uélé",
    "bas uele": "Bas-Uélé",
    "bas-uele": "Bas-Uélé",
}

SHAPE_PREFIX_TO_PROVINCE = {
    "it": "Ituri",
    "nk": "Nord-Kivu",
    "sk": "Sud-Kivu",
    "tp": "Tshopo",
    "hu": "Haut-Uélé",
    "bu": "Bas-Uélé",
}

EPIDEMIC_PROVINCES = ("Ituri", "Nord-Kivu")

PROVINCE_TOTAL_ZONES = {
    "Ituri": 36,
    "Nord-Kivu": 34,
    "Sud-Kivu": 34,
    "Tshopo": 23,
    "Haut-Uélé": 13,
    "Bas-Uélé": 11,
}
TOTAL_ZONES_SANTE = sum(PROVINCE_TOTAL_ZONES.values())

TOTAL_AIRES_SANTE = 3104

AGE_ORDER = [
    "1. 0-4 ans",
    "2. 5-14 ans",
    "3. 15-24 ans",
    "4. 25-44 ans",
    "5. 45-64 ans",
    "6. 65+ ans",
]
SEX_ORDER = ["Masculin", "Féminin"]
SEXE_INCONNU = "Inconnu"
# Variantes connues de sexe_norm (accent/casse) -> libellé canonique ci-dessus,
# appliqué défensivement en lecture (cf. utils/geo.py::canonical_sexe_expr) au
# cas où la source produirait une variante non accentuée (ex. « Feminin »).
SEXE_CANONICAL = {
    "masculin": "Masculin",
    "feminin": "Féminin",
    "féminin": "Féminin",  # noqa: RUF001
    "inconnu": SEXE_INCONNU,
}

SITREP_NUMBER = "1"
# Code incident affiché dans le titre : « SitRep N°{num}/{INCIDENT}_{date} ».
INCIDENT = "MVB"

DATE_PLAUSIBLE_MIN = _date(2026, 5, 1)
DATE_PLAUSIBLE_MAX = _date(2026, 12, 31)

REPORTING_PERIOD_DAYS = 1

ACCENT_RED = "EE0000"
ACCENT_DARK_BLUE = "002060"
