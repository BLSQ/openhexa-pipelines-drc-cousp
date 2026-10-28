"""LLN des notifications (un enrôlement par ligne), drapeaux is_* compris."""

from __future__ import annotations

import config
import polars as pl
from openhexa.sdk import current_run

from lln.commun import apply_dataset_schema, joindre_geographie

# Sources des drapeaux is_* (compute_lln_flags), avec leur type de repli si le
# data element n'a pas été collecté sur la fenêtre demandée (colonne alors
# absente de la LLN avant apply_dataset_schema).
FLAG_SOURCE_DTYPES: dict[str, pl.DataType] = {
    "conclusion_alerte": pl.String,
    "nature_alerte": pl.String,
    "statut_final_patient": pl.String,
    "date_deces_final": pl.Date,
    "statut_patient_prelevement": pl.String,
    "date_deces_pci": pl.Date,
    "modalite_sortie_cte": pl.String,
    "etat_patient_investigation": pl.String,
    "date_prelevement": pl.Date,
    "date_reception_labo": pl.Date,
    "date_analyse_labo": pl.Date,
    "date_notification": pl.Datetime,
    "date_deces_notification": pl.Date,
    "classification_finale_cas": pl.String,
}  # type: ignore


def compute_lln_flags(lln: pl.DataFrame) -> pl.DataFrame:
    """Ajoute à la LLN les drapeaux is_* et la date de décès reconstruite.

    Réplique, au grain enrôlement de la LLN, la même méthodologie que
    compute_indicators()/reconstruct_date_deces() (grain cas, table
    COD_MVE_Tracker_Individu) : des drapeaux booléens combinant plusieurs data
    elements (ex. is_confirme = lab_confirme ET conclusion_alerte == « Validée »
    — une définition qui peut différer de classification_finale_cas, DHIS2).
    is_confirme et is_suspect restent mutuellement exclusifs même si
    lab_confirme (positif un jour, sur tout l'historique labo) diverge de
    is_resultat_valide (valeur courante de resultat_labo) : is_suspect exclut
    explicitement is_confirme. Réplique aussi une date de décès reconstruite
    par cascade de priorité (date finale → date
    notifiée → proxy PCI → proxy prélèvement si décès au prélèvement → proxy
    date de notification). Colonnes ajoutées en fin de schéma
    (config.COLS_LLN_FLAGS) : aucune ne recouvre un nom déjà publié par
    ailleurs dans la LLN (cf. le garde-fou de doublons dans config.py).

    Args:
        lln: LLN après jointures TEI/événements/labo et normalisation des dates
            (colonnes ``date_*`` déjà castées en pl.Date).

    Returns:
        La LLN enrichie des colonnes is_* et date_deces.
    """
    manquantes = [c for c in FLAG_SOURCE_DTYPES if c not in lln.columns]
    if manquantes:
        current_run.log_warning(
            f"LLN : {len(manquantes)} data element(s) absent(s) sur la période, "
            f"traité(s) comme non renseigné(s) pour les drapeaux is_* "
            f"({', '.join(manquantes)})."
        )
        lln = lln.with_columns(
            pl.lit(None, dtype=FLAG_SOURCE_DTYPES[col]).alias(col) for col in manquantes
        )

    # fill_null(False) après chaque comparaison : Polars applique la logique de
    # Kleene (une comparaison à une valeur nulle donne « inconnu », pas faux),
    # alors que la méthodologie de référence (pandas, compute_indicators) traite
    # un data element non renseigné comme une absence de correspondance (False).
    # Sans ce garde-fou, un simple None ferait basculer un OU/ET entier à null.
    is_alerte_valide = pl.col("conclusion_alerte").eq("Validée").fill_null(False)
    is_confirme = (pl.col("n_pos").fill_null(0) >= 1) & is_alerte_valide
    is_non_cas = (pl.col("n_neg").fill_null(0) >= 1) & is_alerte_valide & ~is_confirme
    is_suspect = is_alerte_valide & ~is_confirme & ~is_non_cas
    is_resultat_valide = is_confirme | is_non_cas
    # La date de décès saisie au stage Statut final (date_deces_final) n'entre
    # plus dans la définition : elle est parfois renseignée sur des cas qui ne
    # sont pas déclarés décédés par ailleurs. Le décès est désormais établi par
    # un statut explicite (notification, statut final, prélèvement,
    # investigation, sortie du CTE) ou par la date de décès saisie au PCI.
    is_deces = (
        pl.col("nature_alerte").eq("Décès").fill_null(False)
        | pl.col("statut_final_patient").eq("Décédé").fill_null(False)
        | pl.col("statut_patient_prelevement").eq("Décédé").fill_null(False)
        | pl.col("etat_patient_investigation").eq("Décès").fill_null(False)
        | pl.col("modalite_sortie_cte").eq("Décédé(e)").fill_null(False)
        | pl.col("date_deces_pci").is_not_null()
    )
    is_gueri = pl.col("modalite_sortie_cte").eq("Guéri(e)").fill_null(False)

    lln = lln.with_columns(
        pl.lit(True).alias("is_alerte"),
        is_alerte_valide.alias("is_alerte_valide"),
        # Un numéro de prélèvement peut être attribué sans qu'un prélèvement ait
        # eu lieu : seules les dates (prélèvement, réception labo) l'attestent.
        (
            pl.col("date_prelevement").is_not_null() | pl.col("date_reception_labo").is_not_null()
        ).alias("is_preleve"),
        pl.col("date_reception_labo").is_not_null().alias("is_recu"),
        pl.col("date_analyse_labo").is_not_null().alias("is_analyse"),
        is_resultat_valide.alias("is_resultat_valide"),
        is_suspect.alias("is_suspect"),
        is_non_cas.alias("is_non_cas"),
        is_confirme.alias("is_confirme"),
        is_deces.alias("is_deces"),
        is_gueri.alias("is_gueri"),
    )
    lln = lln.with_columns(
        (pl.col("is_deces") & pl.col("is_confirme")).alias("is_deces_confirme"),
        (pl.col("is_deces") & pl.col("is_suspect")).alias("is_deces_suspect"),
        (pl.col("is_confirme") & pl.col("is_gueri")).alias("is_confirme_gueri"),
        (pl.col("is_confirme") & ~pl.col("is_deces")).alias("is_confirme_vivant"),
    )

    # Cascade de priorité : date finale saisie -> date notifiée -> proxy PCI ->
    # proxy prélèvement (si décès au prélèvement) -> proxy date de notification.
    date_deces = pl.col("date_deces_final").fill_null(pl.col("date_deces_notification"))
    proxy_pci = date_deces.is_null() & pl.col("is_deces") & pl.col("date_deces_pci").is_not_null()
    date_deces = pl.when(proxy_pci).then(pl.col("date_deces_pci")).otherwise(date_deces)
    proxy_prelev = (
        date_deces.is_null()
        & pl.col("is_deces")
        & pl.col("statut_patient_prelevement").eq("Décédé").fill_null(False)
        & pl.col("date_prelevement").is_not_null()
    )
    date_deces = pl.when(proxy_prelev).then(pl.col("date_prelevement")).otherwise(date_deces)
    proxy_notif = (
        date_deces.is_null() & pl.col("is_deces") & pl.col("date_notification").is_not_null()
    )
    date_deces = (
        pl.when(proxy_notif).then(pl.col("date_notification").cast(pl.Date)).otherwise(date_deces)
    )
    # Publiée uniquement pour les décès avérés : depuis que date_deces_final
    # n'établit plus le décès à elle seule, elle renseignerait sinon une date de
    # décès sur un cas dont is_deces vaut False.
    lln = lln.with_columns(
        pl.when(pl.col("is_deces"))
        .then(date_deces)
        .otherwise(pl.lit(None, dtype=pl.Date))
        .alias("date_deces")
    )

    n_confirme = int(lln.get_column("is_confirme").sum())
    n_classification = int(
        (lln.get_column("classification_finale_cas") == "Cas confirmé").fill_null(False).sum()
    )
    if n_confirme != n_classification:
        current_run.log_warning(
            f"LLN : écart de confirmation : {n_confirme} cas via labo + alerte validée contre "
            f"{n_classification} « Cas confirmé » selon la classification finale DHIS2."
        )
    current_run.log_info(
        f"LLN : drapeaux calculés — {n_confirme} confirmés, "
        f"{int(lln.get_column('is_deces').sum())} décès, "
        f"{int(lln.get_column('is_gueri').sum())} guéris."
    )
    return lln


def build_line_list(
    enrollments: pl.DataFrame,
    tei: pl.DataFrame,
    org_units: pl.DataFrame,
    lab_summary: pl.DataFrame,
    event_dates: pl.DataFrame,
) -> pl.DataFrame:
    """Construit la LLN publiée dans le dataset (un enrôlement par ligne).

    Renomme les data elements (config.DATASET_LLN_MAPPING), joint les attributs
    TEI, la fenêtre d'événements et le résumé labo, calcule les drapeaux is_* et
    la date de décès reconstruite (compute_lln_flags, même méthodologie que
    COD_MVE_Tracker_Individu), canonise la géographie puis applique le schéma
    publié config.DATASET_LLN_COLS.

    Args:
        enrollments: Pivot au grain enrôlement.
        tei: Attributs d'entité suivie.
        org_units: Unités d'organisation (hiérarchie géographique).
        lab_summary: Résumé de l'historique laboratoire.
        event_dates: Fenêtre d'événements par enrôlement.

    Returns:
        La LLN au schéma config.DATASET_LLN_COLS, triée de façon déterministe.
    """
    de_columns = [de for de in config.DATASET_LLN_MAPPING if de in enrollments.columns]
    lln = enrollments.select(
        [
            "enrollment_id",
            "tracked_entity_id",
            pl.col("enrollment_org_unit").alias("organisation_unit_id"),
            "enrolled_at",
            *de_columns,
        ]
    ).rename({de: nom for de, nom in config.DATASET_LLN_MAPPING.items() if de in de_columns})

    lln = lln.join(tei, on="tracked_entity_id", how="inner")
    lln = lln.join(event_dates, on="enrollment_id", how="left")

    lln = lln.join(
        lab_summary.drop("enrolled_at", "tracked_entity_id", "lab_resultat_courant"),
        on="enrollment_id",
        how="left",
    )

    cols_texte = [
        col for col in lln.columns if col.startswith("date_") and lln.schema[col] == pl.String
    ]
    lln = lln.with_columns(
        pl.col(cols_texte).str.strip_chars().str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False)
    ).with_columns(pl.col(["date_confirmation", "date_dernier_test"]).cast(pl.Date, strict=False))

    lln = compute_lln_flags(lln)

    # Géographie : libellés canonisés (« it Ituri Province » -> « Ituri »)
    lln = joindre_geographie(lln, org_units)

    lln = apply_dataset_schema(lln, config.DATASET_LLN_COLS)
    current_run.log_info(
        f"LLN partagée : {lln.height} enrôlements, "
        f"{lln['tracked_entity_id'].n_unique()} entités suivies, {lln.width} colonnes."
    )
    # Tri déterministe : garantit un parquet reproductible octet à octet, donc
    # une nouvelle version de dataset uniquement quand les données changent.
    return lln.sort(["enrolled_at", "enrollment_id"], nulls_last=True)
