import hashlib
import re
from pathlib import Path

import config
import pandas as pd
import polars as pl
from openhexa.sdk.datasets.dataset import DatasetVersion


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


def canoniser_geo_expr(col: str) -> pl.Expr:
    """Canonise un libellé géographique DHIS2 (« it Ituri Province » -> « Ituri »).

    Retire le préfixe province à deux lettres (it/nk/sk/kn…) puis le suffixe de
    niveau (« Province », « Zone de Santé », « Aire de Santé »), et harmonise les
    noms de province composés (« Nord Kivu » -> « Nord-Kivu ») pour permettre une
    jointure avec les géométries.

    Returns:
        L'expression Polars renvoyant le libellé canonique de ``col``.
    """
    nettoye = (
        pl.col(col)
        .str.replace(config.GEO_PREFIX_RE, "")
        .str.replace(config.GEO_SUFFIX_RE, "")
        .str.strip_chars()
    )
    return nettoye.replace(config.PROVINCE_CANONICAL).alias(col)


def sha256_of_file(file_path: Path) -> str:
    """Calcule l'empreinte SHA-256 d'un fichier (lecture par blocs).

    Args:
        file_path: Chemin du fichier à hacher.

    Returns:
        L'empreinte SHA-256 du contenu du fichier.
    """
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def in_dataset_version(file_path: Path, dataset_version: DatasetVersion) -> bool:
    """Indique si un fichier de contenu identique est déjà dans la version.

    La comparaison porte sur l'empreinte du contenu, pas sur le nom : une
    nouvelle version n'est créée que si les données ont réellement changé.

    Args:
        file_path: Chemin du fichier local.
        dataset_version: Version de dataset à inspecter.

    Returns:
        True si un fichier de la version a le même contenu, False sinon.
    """
    file_hash = sha256_of_file(file_path)
    for file in dataset_version.files:
        remote_hash = hashlib.sha256()
        remote_hash.update(file.read())
        if file_hash == remote_hash.hexdigest():
            return True
    return False


def nom_prochaine_version(derniere: DatasetVersion | None, horodatage: str) -> str:
    """Détermine le nom de la prochaine version de dataset.

    Incrémente la numérotation « vN » quand elle est reconnue ; sinon (version
    nommée à la main, format inattendu) se replie sur un nom horodaté afin de ne
    jamais échouer ni écraser une version existante.

    Args:
        derniere: Dernière version connue du dataset, ou None si aucune.
        horodatage: Horodatage utilisé pour le nom de repli (UTC).

    Returns:
        Le nom de version à créer.
    """
    if derniere is None:
        return "v1"
    correspondance = re.fullmatch(r"v(\d+)", derniere.name.strip())
    if correspondance:
        return f"v{int(correspondance.group(1)) + 1}"
    return f"v{horodatage}"
