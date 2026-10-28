"""Publication de fichiers dans les datasets OpenHexa."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from openhexa.sdk import Dataset, current_run, workspace
from utils import (
    add_files_to_dataset,
    files_unchanged_in_version,
    nom_prochaine_version,
)


def save_data_for_dataset(df: pd.DataFrame, name: str, output_path: Path) -> list[Path]:
    """Sérialise la table en parquet et CSV pour publication dans le dataset.

    Returns:
        Les chemins des fichiers sérialisés (parquet et CSV) pour publication.
    """
    if len(df) == 0:
        current_run.log_warning(f"Table « {name} » vide : aucun fichier exporté.")
        return []

    output_path.mkdir(parents=True, exist_ok=True)
    file_paths = []
    df.to_parquet(output_path / f"{name}.parquet")
    df.to_csv(output_path / f"{name}.csv", index=False)
    file_paths.append(output_path / f"{name}.parquet")
    file_paths.append(output_path / f"{name}.csv")
    return file_paths


def publish_data_to_dataset(file_paths: list[Path], dataset_id: str) -> None:
    """Publie les tables dans le dataset, sauf si leur contenu est inchangé."""
    try:
        dataset = workspace.get_dataset(dataset_id)
    except Exception as e:
        current_run.log_warning(f"Error retrieving dataset: {dataset_id}: {e}")
        dataset = None

    derniere = dataset.latest_version if dataset is not None else None
    if derniere is not None and files_unchanged_in_version(file_paths, derniere):
        current_run.log_info(
            f"Tables inchangées depuis la version « {derniere.name} » du dataset "
            f"« {dataset_id} » : aucune nouvelle version créée."
        )
        for file in file_paths:
            file.unlink(missing_ok=True)
        return

    try:
        add_files_to_dataset(
            dataset_id=dataset_id, file_paths=file_paths, ds_version_prefix="MVE_TRACKER"
        )
    finally:
        for file in file_paths:
            file.unlink(missing_ok=True)


def publier_version(
    dataset: Dataset,
    chemins: list[Path],
    chemins_meta: list[Path],
    horodatage: str,
) -> None:
    """Crée une version du dataset et y dépose les LLN, si leur contenu a changé.

    Seuls les parquets entrent dans la comparaison : les métadonnées portent
    l'horodatage de génération et changeraient à chaque exécution.

    Args:
        dataset: Dataset cible.
        chemins: Parquets des LLN.
        chemins_meta: JSON de métadonnées accompagnant les LLN.
        horodatage: Horodatage de repli pour le nom de version.
    """
    derniere = dataset.latest_version
    if derniere is not None and files_unchanged_in_version(chemins, derniere):
        current_run.log_info(
            f"LLN inchangées depuis la version « {derniere.name} » du dataset "
            f"« {dataset.name} » : aucune nouvelle version créée."
        )
        return

    version = dataset.create_version(nom_prochaine_version(derniere, horodatage))
    for chemin in (*chemins, *chemins_meta):
        version.add_file(chemin, chemin.name)
    current_run.log_info(
        f"LLN publiées dans le dataset « {dataset.name} », version « {version.name} » "
        f"({', '.join(c.name for c in (*chemins, *chemins_meta))})."
    )
