"""LLN du suivi des contacts (un suivi journalier par ligne)."""

from __future__ import annotations

import config
import polars as pl
from openhexa.sdk import current_run

from lln.commun import apply_dataset_schema, joindre_geographie


def build_contacts_line_list(
    events: pl.DataFrame,
    tei: pl.DataFrame,
    org_units: pl.DataFrame,
) -> pl.DataFrame:
    """Construit la LLN du suivi des contacts (un suivi journalier par ligne).

    Le programme ne compte qu'un program stage : chaque event_id porte un suivi
    complet d'un contact. Pivote les data elements au grain event, joint les
    attributs TEI, canonise la géographie, type les dates / booléens /
    température puis applique le schéma config.DATASET_LLN_CONTACTS_COLS.

    Args:
        events: Événements bruts issus de load_contacts_events().
        tei: Attributs d'entité suivie des contacts.
        org_units: Unités d'organisation (hiérarchie géographique).

    Returns:
        La LLN contacts au schéma publié, triée de façon déterministe.
    """
    cles = ["event_id", "enrollment_id", "tracked_entity_id", "enrolled_at"]
    contexte = events.group_by(cles).agg(
        pl.col("enrollment_org_unit")
        .sort_by(["occurred_at", "created_at"], nulls_last=True)
        .last()
        .alias("organisation_unit_id")
    )
    valeurs = events.sort(
        ["event_id", "created_at"], nulls_last=True
    ).pivot(
        on="data_element_id",
        index="event_id",
        values="value_norm",
        aggregate_function="last",
    )
    lln = contexte.join(valeurs, on="event_id", how="left")
    lln = lln.rename(
        {de: nom for de, nom in config.DICO_DE_MAPPING_CONTACTS.items() if de in lln.columns}
    )
    lln = lln.join(tei, on="tracked_entity_id", how="inner")

    # Data elements jamais collectés sur la période : colonnes vides typées
    # texte, pour que les conversions ci-dessous s'appliquent sans erreur.
    lln = lln.with_columns(
        pl.lit(None, dtype=pl.String).alias(nom)
        for nom in config.DICO_DE_MAPPING_CONTACTS.values()
        if nom not in lln.columns
    )

    cols_texte = [
        col for col in lln.columns if col.startswith("date_") and lln.schema[col] == pl.String
    ]
    lln = lln.with_columns(
        pl.col(cols_texte).str.strip_chars().str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False),
        pl.col(["dernier_jour_de_suivi", "contact_vu"]).replace_strict(
            {"true": True, "false": False}, default=None, return_dtype=pl.Boolean
        ),
        pl.col("temperature")
        .str.strip_chars()
        .str.replace(",", ".", literal=True)
        .cast(pl.Float32, strict=False),
    ).with_columns(
        # Semaine ISO, même format que semaine_epidemio (« 2026-S05 »)
        (
            pl.col("date_de_suivi").dt.iso_year().cast(pl.String)
            + "-S"
            + pl.col("date_de_suivi").dt.week().cast(pl.String).str.zfill(2)
        ).alias("semaine_de_suivi")
    )

    # Géographie : libellés canonisés (« it Ituri Province » -> « Ituri »)
    lln = joindre_geographie(lln, org_units)

    lln = apply_dataset_schema(lln, config.DATASET_LLN_CONTACTS_COLS)
    current_run.log_info(
        f"LLN contacts : {lln.height} suivis journaliers, "
        f"{lln['tracked_entity_id'].n_unique()} contacts, {lln.width} colonnes."
    )
    # Tri déterministe (parquet reproductible, cf. build_line_list)
    return lln.sort(["date_de_suivi", "event_id"], nulls_last=True)
