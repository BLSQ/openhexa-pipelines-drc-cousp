from __future__ import annotations

from contextlib import suppress
from pathlib import Path

from openhexa.sdk import workspace


def _resolve_layout() -> tuple[Path, Path, Path, Path, Path, Path, str]:
    """Détecte le layout des fichiers : OpenHexa (workspace monté) ou dépôt local.

    - **OpenHexa** : ``<workspace>/pipelines/sitrep/{generated_files, geometry,
      template_docx}`` ; le CSV et les sorties partagent ``generated_files``.
    - **Local** : dépôt réorganisé sous ``data/`` en sous-dossiers dédiés
      (``extract_data_openhexa/`` pour le CSV, ``geometry/``, ``templates/``,
      ``generate_files/`` pour les sorties).

    Returns:
        tuple: ``(repo_root, output_dir, csv_dir, geom_dir, template_dir,
        narrative_dir, template_name)``.
    """
    with suppress(Exception):
        root = Path(workspace.files_path) / "pipelines/sitrep"
        if root.exists():
            gen = root / "generated_files"
            return (
                root,
                gen,  # output_dir
                gen,  # csv_dir
                root / "geometry",
                root / "template_docx",
                root / "narrative",
                "Template_SitRep.docx",
            )
    # Mode local : racine du dépôt (…/RDC SiteRep), arborescence ``data/``
    # pour les tests locaux du pipeline.
    local = Path(__file__).resolve().parents[2]
    data = local / "data"
    return (
        local,
        data / "generate_files",  # output_dir
        data / "extract_data_openhexa",  # csv_dir
        data / "geometry",
        data / "templates",
        data / "narrative",
        "Template_SitRep_v3.docx",
    )


(
    REPO_ROOT,
    DATA_DIR,
    _CSV_DIR,
    _GEOM_DIR,
    _TEMPLATE_DIR,
    _NARRATIVE_DIR,
    _TEMPLATE_NAME,
) = _resolve_layout()

AGG_TABLE = "mve_notification_events"
AGG_DB_SCHEMA: str | None = None  # schéma SQL (None = défaut/public)
DEFAULT_CSV = _CSV_DIR / "mve_tracker_events_notifications.csv"
DEFAULT_TEMPLATE = _TEMPLATE_DIR / _TEMPLATE_NAME
ORIGINAL_TEMPLATE = _TEMPLATE_DIR / _TEMPLATE_NAME
DEFAULT_SHAPEFILE = _GEOM_DIR / "zone_sante.parquet"
DEFAULT_SHAPEFILE_FALLBACK = _GEOM_DIR / "zone_sante.geojson"
DEFAULT_PROVINCES_SHAPEFILE = _GEOM_DIR / "provinces.parquet"
PROVINCES_NAME_SUFFIX = " Province"

ASSET_DIR = DATA_DIR.parent / "assets"
IMAGE_CHRONOLOGIE = ASSET_DIR / "chronologie des faits.png"
IMAGE_CONTACTS = ASSET_DIR / "info-contacts.png"

NARRATIVE_YAML = _NARRATIVE_DIR / "narrative.yaml"

ND = "ND"

PROVINCE_PREFIXES = (
    "nk",
    "sk",
    "kn",
    "it",
    "bu",
    "hk",
    "hl",
    "hu",
    "kc",
    "ke",
    "kg",
    "kl",
    "kr",
    "ks",
    "ll",
    "lm",
    "md",
    "mg",
    "mn",
    "nu",
    "sn",
    "su",
    "tn",
    "tp",
    "tu",
    "eq",
)

PROVINCE_CANONICAL = {
    "ituri": "Ituri",
    "nord kivu": "Nord-Kivu",
    "nord-kivu": "Nord-Kivu",
    "sud kivu": "Sud-Kivu",
    "sud-kivu": "Sud-Kivu",
    "kinshasa": "Kinshasa",
}

SHAPE_PREFIX_TO_PROVINCE = {
    "it": "Ituri",
    "nk": "Nord-Kivu",
    "sk": "Sud-Kivu",
    "kn": "Kinshasa",
}

SHAPE_NAME_SUFFIX = " Zone de Santé"

EPIDEMIC_PROVINCES = ("Ituri", "Nord-Kivu")

AGE_ORDER = ["0-4 ans", "5-17 ans", "18-29 ans", "30-49 ans", "50 ans et plus"]

AGE_BUCKETS = [
    (0, 4, "0-4 ans"),
    (5, 17, "5-17 ans"),
    (18, 29, "18-29 ans"),
    (30, 49, "30-49 ans"),
    (50, float("inf"), "50 ans et plus"),
]

SEX_ORDER = ["Masculin", "Feminin"]

SEXE_CANONICAL = {
    "m": "Masculin",
    "h": "Masculin",
    "homme": "Masculin",
    "masculin": "Masculin",
    "male": "Masculin",
    "f": "Feminin",
    "femme": "Feminin",
    "feminin": "Feminin",
    "féminin": "Feminin",
    "female": "Feminin",
}
SEXE_INCONNU = "Inconnu"

PROVINCE_TOTAL_ZONES = {
    "Ituri": 36,
    "Nord-Kivu": 34,
    "Sud-Kivu": 34,
    "Kinshasa": 35,
}


SITREP_NUMBER = "37"
# Code incident affiché dans le titre : « SitRep N°{num}/{INCIDENT}_{date} ».
INCIDENT = "MVB"
YEAR = 2026

from datetime import date as _date  # noqa: E402

DATE_PLAUSIBLE_MIN = _date(2026, 5, 1)
DATE_PLAUSIBLE_MAX = _date(2026, 12, 31)

REPORTING_PERIOD_DAYS = 2

ACCENT_RED = "C00000"

DE_UUID_SYMPTOMES = {
    "g2QJ4LWuq1C": "signe_fatigue",
    "uW3XFH8TQGE": "signe_fievre",
    "xATq2Gnt48G": "signe_nausees_vomissements",
    "Pjk2zRsdLEv": "signe_diarrhees",
    "ZwlwHsvxPA3": "signe_cephalees",
    "HrFOPwqKxoV": "signe_saignements",
    "vwS0SsOqCz9": "signe_coma",
    "N50wDaI6H1r": "signe_epistaxis",
    "BYkTKut1D8V": "signe_melenas",
    "fjXyHX02I8c": "signe_confusion",
    "pwNocbwvO0o": "signe_saignement_gencives",
    "Gutl308P6Pl": "signe_hematemeses",
    "f0yTueLYdns": "signe_hematomes_petechies",
    "jieNzfUp3E8": "signes_hemorragiques_maladie",
}

SIGNES = {
    "signe_fatigue": "Fatigue sévère",
    "signe_fievre": "Fièvre",
    "signe_nausees_vomissements": "Nausées/Vomissements",
    "signe_diarrhees": "Diarrhées",
    "signe_cephalees": "Céphalées",
    "signe_saignements": "Saignements",
    "signe_coma": "Coma",
    "signe_epistaxis": "Épistaxis",
    "signe_melenas": "Mélénas",
    "signe_confusion": "Confusion",
    "signe_saignement_gencives": "Saign. gencives",
    "signes_hemorragiques_maladie": "Hémorragique (global)",
}
