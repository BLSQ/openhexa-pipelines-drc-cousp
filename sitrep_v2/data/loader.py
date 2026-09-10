from __future__ import annotations

import tempfile
from pathlib import Path

import config
import polars as pl
import requests
from openhexa.sdk import Dataset, workspace
from utils import geo


def _get_source_dataset() -> Dataset:
    """Résout le dataset source **sans** exiger qu'il soit lié à ce workspace.

    ``workspace.get_dataset()`` accepte un ``source_workspace_slug`` optionnel
    qui interroge directement le workspace d'origine, sans passer par un lien
    dataset↔workspace créé dans l'UI OpenHexa. D'où l'absence de paramètre
    ``Dataset`` dans ``pipeline.py`` : le dataset et son workspace source sont
    des constantes ``config.py``.

    Returns:
        Dataset: Le dataset ``config.DATASET_SLUG`` du workspace
        ``config.DATASET_SOURCE_WORKSPACE``.
    """
    return workspace.get_dataset(config.DATASET_SLUG, source_workspace_slug=config.DATASET_SOURCE_WORKSPACE)


def _download_dataset_file(dataset: Dataset, filename: str) -> Path:
    """Télécharge un fichier de la dernière version d'un dataset OpenHexa.

    Reprend le pattern de ``senes_table_update/utils.py::get_dataset_content``
    (même dépôt, aucun import cross-pipeline).

    Returns:
        Path: Le chemin du fichier téléchargé (fichier temporaire).
    """
    version = dataset.latest_version
    if not version:
        raise ValueError(f"Aucune version trouvée pour le dataset « {dataset.name} ».")
    file_ref = version.get_file(filename)
    if not file_ref:
        raise ValueError(f"Fichier « {filename} » absent du dataset « {dataset.name} ».")
    r = requests.get(file_ref.download_url, timeout=60)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=Path(filename).suffix, delete=False) as tfile:
        tfile.write(r.content)
        return Path(tfile.name)


def _clean_geo(df: pl.DataFrame) -> pl.DataFrame:
    """Nettoie les colonnes géo/démographie (variantes résiduelles amont).

    En principe déjà canonisées par ``compute_indicators_mve_tdb``, mais
    certaines provinces (ex. Bas-Uélé) remontent encore préfixées (« bu Bas
    Uele », « bu Buta »), et ``sexe_norm`` pourrait remonter sous une
    variante d'accent/casse (ex. « Feminin ») — filet de sécurité en
    attendant la correction amont, avec les mêmes helpers que v1
    (``utils/geo.py``).

    Returns:
        pl.DataFrame: Le DataFrame avec ``province``/``zone_sante``/
        ``aire_sante``/``sexe_norm`` nettoyés (colonnes absentes laissées
        telles quelles).
    """
    if "province" in df.columns:
        df = df.with_columns(geo.canonical_province_expr("province").alias("province"))
    for col in ("zone_sante", "aire_sante"):
        if col in df.columns:
            df = df.with_columns(geo.strip_prefix_expr(col).str.strip_chars().alias(col))
    if "sexe_norm" in df.columns:
        df = df.with_columns(geo.canonical_sexe_expr("sexe_norm").alias("sexe_norm"))
    return df


def load_dataset() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Charge les 2 tables déjà agrégées du dataset source.

    Contrairement à v1 (``data/loader.py::load_from_db``), pas de renommage à
    faire ici : ``compute_indicators_mve_tdb`` a déjà normalisé ``sexe_norm``/
    ``tranche_age``. Un nettoyage géographique défensif reste appliqué (cf.
    ``_clean_geo``).

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: ``(rapportage, dds_agg)``.
    """
    dataset = _get_source_dataset()
    rapportage_path = _download_dataset_file(dataset, config.RAPPORTAGE_FILE)
    dds_agg_path = _download_dataset_file(dataset, config.DDS_AGG_FILE)
    rapportage = _clean_geo(pl.read_parquet(rapportage_path))
    dds_agg = _clean_geo(pl.read_parquet(dds_agg_path))
    return rapportage, dds_agg


def filter_provinces(df: pl.DataFrame, provinces: list[str] | None) -> pl.DataFrame:
    """Restreint une table aux provinces demandées (remplace ``zone_sante`` v1).

    ``provinces`` vide/``None`` → DataFrame inchangé (rapport national). Les
    provinces sont déjà canonisées à la source (``compute_indicators_mve_tdb``) :
    aucune canonisation supplémentaire nécessaire (à la différence de
    ``data/loader.py::filter_zones_sante`` en v1).

    Returns:
        pl.DataFrame: Le sous-ensemble filtré (ou ``df`` intact si aucun choix).
    """
    if not provinces or "province" not in df.columns:
        return df
    return df.filter(pl.col("province").is_in(provinces))


def date_anomalies(df: pl.DataFrame, date_col: str) -> dict:
    """Repère (sans filtrer) les dates hors plage plausible, pour les signaler.

    Returns:
        dict: ``{count, examples, lo, hi}``, vide si aucune anomalie ou si
        ``date_col`` est absente.
    """
    if date_col not in df.columns:
        return {}
    lo, hi = config.DATE_PLAUSIBLE_MIN, config.DATE_PLAUSIBLE_MAX
    bad = df.filter((pl.col(date_col) < lo) | (pl.col(date_col) > hi))
    if not bad.height:
        return {}
    ex = sorted({str(d) for d in bad[date_col].drop_nulls().unique().to_list()})
    return {"count": bad.height, "examples": ex[:5], "lo": lo, "hi": hi}
