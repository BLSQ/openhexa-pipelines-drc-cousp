import hashlib
import re
from pathlib import Path
import tempfile
import shutil
from datetime import datetime

import config
import pandas as pd
import polars as pl
from openhexa.sdk.datasets.dataset import DatasetVersion
from openhexa.sdk import current_run, workspace


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


def files_unchanged_in_version(file_paths: list[Path], dataset_version: DatasetVersion) -> bool:
    """Indique si tous les fichiers locaux ont un contenu déjà présent dans la version.

    Contrairement à un appel à in_dataset_version() par fichier local, les
    fichiers distants ne sont téléchargés et hachés qu'une seule fois, quel que
    soit le nombre de fichiers locaux à vérifier.

    Args:
        file_paths: Fichiers locaux à vérifier.
        dataset_version: Version de dataset à inspecter.

    Returns:
        True si chaque fichier local a une empreinte présente dans la version.
    """
    remote_hashes = set()
    for file in dataset_version.files:
        remote_hash = hashlib.sha256()
        remote_hash.update(file.read())
        remote_hashes.add(remote_hash.hexdigest())

    return all(sha256_of_file(fp) in remote_hashes for fp in file_paths)


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


def add_files_to_dataset(
    dataset_id: str,
    file_paths: list[Path],
    ds_version_prefix: str = "DS",
    ds_desc: str = "Dataset version created by pipeline",
) -> bool:
    """Add files to a new dataset version.

    Args:
        dataset_id: The ID of the dataset to which files will be added.
        file_paths: A list of file paths to be added to the dataset.
        ds_version_prefix: The prefix for the dataset version name. Defaults to "DS".
        ds_desc: The description for the dataset version. Defaults to "Dataset version created by pipeline".

    Returns:
        True if at least one file was added successfully, False otherwise.

    Raises:
        ValueError: If `dataset_id` is not specified.
    """
    if not dataset_id:
        raise ValueError("Dataset ID is not specified.")

    supported_extensions = {".parquet", ".csv", ".geojson", ".json"}
    added_any = False
    new_version = None

    for src in file_paths:
        if not src.exists():
            current_run.log_warning(f"File not found: {src}")
            continue

        ext = src.suffix.lower()
        if ext not in supported_extensions:
            current_run.log_warning(f"Unsupported file format: {src.name}")
            continue

        try:
            new_version = _copy_and_add_file(src, new_version, dataset_id, ds_version_prefix, ds_desc)
            current_run.log_info(f"File {src.name} added to dataset version: {new_version.name}")
            added_any = True
        except Exception as e:
            current_run.log_warning(f"File {src.name} cannot be added: {e}")

    if not added_any:
        current_run.log_warning("No valid files found. Dataset version was not created.")
        return False

    return True


def _copy_and_add_file(
    src: Path, new_version: DatasetVersion | None, dataset_id: str, prefix: str, desc: str
) -> DatasetVersion:
    """Copy src to a temp file, then add it to a lazily created dataset version.

    Args:
        src: Path of the file to copy and add to the dataset.
        new_version: The dataset version created so far in this run, or None if none has been created yet.
        dataset_id: The ID of the dataset for which a new version will be created if needed.
        prefix: Prefix for the dataset version name, used only if a new version needs to be created.
        desc: Description used if the dataset itself has to be created, used only if a new version
            needs to be created.

    Returns:
        The dataset version the file was added to (same object as `new_version` if it was already set).
    """
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=src.suffix.lower(), delete=False) as tmp:
            tmp_path = Path(tmp.name)
        shutil.copy2(src, tmp_path)
        new_version = _ensure_dataset_version(new_version, dataset_id, prefix, desc)
        new_version.add_file(str(tmp_path), filename=src.name)
        return new_version
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()


def _ensure_dataset_version(
    current: DatasetVersion | None, dataset_id: str, prefix: str, desc: str
) -> DatasetVersion:
    """Return the current dataset version, creating one lazily on first use.

    Args:
        current: The dataset version created so far in this run, or None if none has been created yet.
        dataset_id: The ID of the dataset for which a new version will be created if needed.
        prefix: Prefix for the dataset version name, used only if a new version needs to be created.
        desc: Description used if the dataset itself has to be created, used only if a new version
            needs to be created.

    Returns:
        `current` if it was already set, otherwise a newly created dataset version.
    """
    if current is not None:
        return current
    version = get_new_dataset_version(ds_id=dataset_id, prefix=prefix, ds_desc=desc)
    current_run.log_info(f"New dataset version created: {version.name}")
    return version


def get_new_dataset_version(ds_id: str, prefix: str = "DS", ds_desc: str = "Dataset") -> DatasetVersion:
    """Create and return a new dataset version.

    Args:
        ds_id: The ID of the dataset for which a new version will be created.
        prefix: Prefix for the dataset version name. Defaults to "DS".
        ds_desc: Description used when the dataset itself has to be created (i.e. `ds_id` doesn't
            exist yet). Not used when a version is added to an existing dataset. Defaults to "Dataset".

    Returns:
        The newly created dataset version.

    Raises:
        Exception: If an error occurs while creating the new dataset version.
    """
    try:
        dataset = workspace.get_dataset(ds_id)
    except Exception as e:
        current_run.log_warning(f"Error retrieving dataset: {ds_id}: {e}")
        dataset = None

    if dataset is None:
        current_run.log_warning(f"Creating new Dataset with ID: {ds_id}")
        dataset = workspace.create_dataset(name=ds_id.replace("-", "_").upper(), description=ds_desc)

    version_name = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}"

    try:
        return dataset.create_version(version_name)
    except Exception as e:
        raise Exception("An error occurred while creating the new dataset version.") from e
