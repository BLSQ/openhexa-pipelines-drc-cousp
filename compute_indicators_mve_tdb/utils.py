import re

import config
import pandas as pd


def parse_geo(geo_str: object) -> dict[str, str | None]:
    """Extrait province, zone_sante et aire_sante depuis la hiérarchie DHIS2.

    Returns:
        Un dict {province, zone_sante, aire_sante} ; valeur None si le niveau
        est absent de la hiérarchie.
    """
    result: dict[str, str | None] = {"province": None, "zone_sante": None, "aire_sante": None}
    if pd.isna(geo_str):  # type: ignore
        return result
    parts = [re.sub(r"^it\s+", "", p.strip()) for p in str(geo_str).split("/")]
    suffix = r"\s+(Province|Zone de Santé|Zone_de_sante|Aire de Santé)$"
    for i, label in enumerate(["province", "zone_sante", "aire_sante"]):
        idx = i + 1  # parts[0] = pays
        if idx < len(parts):
            result[label] = re.sub(suffix, "", parts[idx], flags=re.IGNORECASE).strip() or None
    return result


def tranche_age(
    row: pd.Series,
    age_bins: list[float] = config.AGE_BINS,
    age_labels: list[str] = config.AGE_LABELS,
) -> str:
    """Classe un cas dans sa tranche d'âge (priorité aux années, sinon mois).

    Returns:
        Le libellé de tranche d'âge, ou « Inconnu » si l'âge est absent.
    """
    ans = row.get("age_ans")
    mois = row.get("age_mois")
    if pd.notna(ans):
        age = float(ans)
    elif pd.notna(mois):
        age = float(mois) / 12
    else:
        return "Inconnu"
    for i, borne in enumerate(age_bins[1:]):
        if age < borne:
            return age_labels[i]
    return age_labels[-1]


def compter_oui(serie: pd.Series) -> int:
    """Compte les réponses « Oui » d'une série (les autres valeurs sont ignorées).

    Returns:
        Le nombre de valeurs égales à « Oui ».
    """
    return int((serie == "Oui").sum())
