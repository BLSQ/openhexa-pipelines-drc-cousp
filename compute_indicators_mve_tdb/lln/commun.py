"""Briques communes aux listes linéaires publiées dans le dataset."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import config
import polars as pl
from openhexa.sdk import current_run, workspace
from openhexa.toolbox.dhis2 import dataframe
from utils import (
    canoniser_geo_expr,
)

# level_* DHIS2 -> noms publiés dans la LLN partagée
GEO_RENAME = {
    "level_2_id": "province_id",
    "level_2_name": "province",
    "level_3_id": "zone_sante_id",
    "level_3_name": "zone_sante",
    "level_4_id": "aire_sante_id",
    "level_4_name": "aire_sante",
}


def joindre_geographie(df: pl.DataFrame, org_units: pl.DataFrame) -> pl.DataFrame:
    """Joint la hiérarchie géographique et canonise les libellés.

    « it Ituri Province » -> « Ituri » ; les level_* DHIS2 sont renommés
    selon GEO_RENAME (province, zone_sante, aire_sante et leurs identifiants).

    Args:
        df: Table portant une colonne organisation_unit_id.
        org_units: Unités d'organisation (hiérarchie géographique).

    Returns:
        La table enrichie de la géographie canonisée.
    """
    df = dataframe.join_object_names(df=df, organisation_units=org_units)
    df = df.rename({brut: nom for brut, nom in GEO_RENAME.items() if brut in df.columns})
    return df.with_columns(
        [
            canoniser_geo_expr(niveau)
            for niveau in ("province", "zone_sante", "aire_sante")
            if niveau in df.columns
        ]
    )


def apply_dataset_schema(df: pl.DataFrame, colonnes: list[str]) -> pl.DataFrame:
    """Applique le schéma publié : colonnes manquantes créées à NULL, ordre figé.

    Args:
        df: LLN construite depuis la source.
        colonnes: Schéma cible (config.DATASET_LLN_COLS).

    Returns:
        Le DataFrame restreint et ordonné selon ``colonnes``.
    """
    manquantes = [col for col in colonnes if col not in df.columns]
    if manquantes:
        current_run.log_warning(
            f"LLN : {len(manquantes)} colonne(s) absente(s) de la source, créée(s) à NULL "
            f"({', '.join(manquantes)})."
        )
        df = df.with_columns([pl.lit(None, dtype=pl.String).alias(col) for col in manquantes])
    return df.select(colonnes)


def write_lln_parquet(lln: pl.DataFrame, nom_fichier: str = config.LLN_DATASET_FILE) -> Path:
    """Écrit la LLN dans le dossier de travail du workspace, sous un nom stable.

    Args:
        lln: LLN au schéma publié.
        nom_fichier: Nom du parquet (LLN notifications par défaut).

    Returns:
        Le chemin du parquet écrit.
    """
    dossier = Path(workspace.files_path, *config.LLN_EXPORT_DIR.split("/"))
    dossier.mkdir(parents=True, exist_ok=True)
    chemin = dossier / nom_fichier
    lln.write_parquet(chemin)
    current_run.log_info(f"LLN écrite dans « {chemin} » ({chemin.stat().st_size / 1e6:.1f} Mo).")
    return chemin


def build_lln_metadata(
    lln: pl.DataFrame,
    date_min: date | None,
    date_max: date | None,
    horodatage: datetime,
    source: str = f"public.{config.EVENTS_TABLE} (événements du tracker MVE, format long)",
    grain: str = "un enrôlement (enrollment_id) par ligne",
    confidentialite: str = (
        "Liste linéaire nominative. Contient des quasi-identifiants (numéro Epid, "
        "identifiant labo, âge, sexe, profession, aire de santé) et des coordonnées "
        "GPS du domicile : diffusion restreinte aux personnes habilitées."
    ),
) -> dict[str, object]:
    """Décrit la LLN publiée : périmètre, volumétrie, remplissage, confidentialité.

    Ce dictionnaire est publié en JSON à côté du parquet : sans lui, le workspace
    consommateur ne peut savoir ni quelle fenêtre est couverte, ni quelles
    colonnes sont vides faute de collecte.

    Args:
        lln: LLN au schéma publié.
        date_min: Borne basse de la fenêtre sur enrolled_at, ou None si
            l'extraction est complète (aucune fenêtre publiée).
        date_max: Borne haute de la fenêtre, ou None.
        horodatage: Instant de génération (UTC).
        source: Table source décrite dans les métadonnées.
        grain: Grain d'une ligne de la LLN.
        confidentialite: Mention de confidentialité.

    Returns:
        Les métadonnées du fichier, sérialisables en JSON.
    """
    total = lln.height
    non_nuls = {col: total - lln[col].null_count() for col in lln.columns}
    remplissage = {col: round(100 * n / total, 1) for col, n in non_nuls.items()} if total else {}
    # Compté sur les valeurs, pas sur le pourcentage arrondi : une colonne
    # remplie à 0,04 % arrondit à 0,0 sans être vide pour autant.
    vides = [col for col, n in non_nuls.items() if n == 0]
    if vides:
        current_run.log_warning(
            f"LLN : {len(vides)} colonne(s) entièrement vide(s) sur la période ({', '.join(vides)})."
        )
    return {
        "pipeline": "compute_indicators_mve_tdb",
        "genere_le": horodatage.isoformat(timespec="seconds"),
        "source": source,
        "grain": grain,
        "fenetre_enrolled_at": (
            {
                "min": date_min.isoformat(),
                "max": date_max.isoformat() if date_max else None,
            }
            if date_min
            else None
        ),
        "lignes": total,
        "colonnes": lln.width,
        "entites_suivies": lln["tracked_entity_id"].n_unique(),
        "taux_remplissage_pct": remplissage,
        "colonnes_vides": vides,
        "confidentialite": confidentialite,
    }
