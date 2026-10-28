"""Indicateurs au grain cas et liste de ligne individuelle du tableau de bord."""

from __future__ import annotations

import config
import numpy as np
import pandas as pd
import polars as pl
from openhexa.sdk import current_run
from utils import (
    parse_geo,
    tranche_age,
)


def consolidate_line_list(
    enrollments: pl.DataFrame,
    tei: pl.DataFrame,
    lab_summary: pl.DataFrame,
) -> pd.DataFrame:
    """Consolide la liste de ligne nominative (une ligne par cas).

    Joint le pivot enrôlement aux attributs TEI puis au résumé labo, renomme les
    data elements (identifiants DHIS2 → noms lisibles), reconstruit la
    hiérarchie géographique et crée à None les data elements jamais collectés.

    Returns:
        La liste de ligne consolidée (pandas), entrée du calcul d'indicateurs.
    """
    line_list = enrollments.join(tei, on="tracked_entity_id", how="left")
    line_list = line_list.with_columns(
        pl.concat_str(
            ["level_1_name", "level_2_name", "level_3_name", "level_4_name"],
            separator=" / ",
        ).alias("geo_hierarchie"),
    )

    # Identifiants DHIS2 → noms lisibles (uniquement les colonnes présentes)
    de_vers_nom = {
        de_id: nom for nom, de_id in config.DICO_DE_MAPPING.items() if de_id in line_list.columns
    }
    line_list = line_list.rename(de_vers_nom)

    # Data elements jamais collectés sur la période → colonnes vides
    de_absents = [nom for nom in config.DICO_DE_MAPPING if nom not in line_list.columns]
    line_list = line_list.with_columns(pl.lit(None).alias(nom) for nom in de_absents)

    line_list = line_list.select(["tracked_entity_id", *config.RENAME_MAP.values()])
    line_list = line_list.join(
        lab_summary.join(tei, on="tracked_entity_id", how="left").select(config.COLS_PRELEV),
        on="tracked_entity_id",
        how="left",
    )
    if de_absents:
        current_run.log_debug(
            f"Data elements absents de la période (créés vides) : {len(de_absents)}."
        )
    current_run.log_info(f"Liste de ligne consolidée : {line_list.height} cas.")
    return line_list.drop("tracked_entity_id").to_pandas()


def compute_indicators(line_list: pd.DataFrame) -> pd.DataFrame:
    """Calcule les indicateurs au grain cas à partir de la liste de ligne.

    Découpe la hiérarchie géographique, normalise les dates, dérive la semaine
    épidémiologique, la tranche d'âge et le sexe, puis les drapeaux booléens
    (alerte, suspect, confirmé, décès, guéri…) qui seront sommés à l'agrégation.

    Returns:
        La liste de ligne enrichie des colonnes dérivées et des drapeaux is_*.
    """
    geo = line_list["geo_hierarchie"].apply(parse_geo).apply(pd.Series)
    line_list = pd.concat([line_list, geo], axis=1)
    line_list["geo_hierarchie"] = line_list["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:3])
    )
    for cible, source in config.DATE_COLS.items():
        dates = pd.Series(pd.to_datetime(line_list[source], errors="coerce"))
        line_list[cible] = dates.where(dates.dt.year.between(1900, 2100))

    # Semaine épidémiologique ISO (recalculée depuis date_notif propre)
    iso = line_list["date_notif"].dt.isocalendar()
    line_list["semaine_epidemio"] = iso.year.astype(str) + "-S" + iso.week.astype(str).str.zfill(2)
    line_list["tranche_age"] = line_list.apply(tranche_age, axis=1)

    line_list["sexe_norm"] = (
        line_list["sexe"]
        .astype(str)
        .str.strip()
        .str.capitalize()
        .replace({"Nan": "Inconnu", "None": "Inconnu"})
    )

    line_list["is_alerte"] = True
    line_list["is_alerte_valide"] = line_list["conclusion_alerte"] == "Validée"
    line_list["is_preleve"] = (
        line_list["date_prelevement"].notna() | line_list["date_reception_labo"].notna()
    )
    line_list["is_recu"] = line_list["date_reception_labo"].notna()
    line_list["is_analyse"] = line_list["date_analyse_labo"].notna()
    line_list["is_confirme"] = (line_list["n_pos"].fillna(0) >= 1) & line_list["is_alerte_valide"]
    line_list["is_non_cas"] = (
        (line_list["n_neg"].fillna(0) >= 1)
        & line_list["is_alerte_valide"]
        & ~line_list["is_confirme"]
    )
    line_list["is_suspect"] = (
        line_list["is_alerte_valide"] & ~line_list["is_confirme"] & ~line_list["is_non_cas"]
    )
    line_list["is_resultat_valide"] = line_list["is_confirme"] | line_list["is_non_cas"]
    line_list["n_echantillons_valides"] = line_list["n_pos"].fillna(0) + line_list["n_neg"].fillna(
        0
    )

    # Définition alignée sur compute_lln_flags : la date de décès du statut
    # final (date_deces_final) n'établit plus le décès à elle seule.
    line_list["is_deces"] = (
        (line_list["nature_alerte"] == "Décès")
        | (line_list["statut_final_patient"] == "Décédé")
        | (line_list["statut_patient_prelevement"] == "Décédé")
        | (line_list["etat_patient_investigation"] == "Décès")
        | (line_list["modalite_sortie_cte"] == "Décédé(e)")
        | (line_list["date_deces_pci"].notna())
    )
    line_list["is_deces_confirme"] = line_list["is_deces"] & line_list["is_confirme"]
    line_list["is_deces_suspect"] = line_list["is_deces"] & line_list["is_suspect"]
    # Alertes validées (suspects, non-cas et confirmés confondus) avec lien
    # épidémiologique connu — le nom du drapeau reste is_suspect_lien_epi
    # (indicateur historique n_suspects_lien_epi) mais n'est plus restreint aux
    # seuls cas suspects.
    line_list["is_suspect_lien_epi"] = line_list["is_alerte_valide"] & (
        line_list["lien_epidemiologique"] == "Oui"
    )

    # Guérison : modalité de sortie du CTE (DE « Statut au moment de la sortie »).
    modalite_sortie = line_list.get("modalite_sortie_cte")
    line_list["is_gueri"] = modalite_sortie.eq("Guéri(e)") if modalite_sortie is not None else False
    line_list["is_confirme_gueri"] = line_list["is_confirme"] & line_list["is_gueri"]
    line_list["is_confirme_vivant"] = line_list["is_confirme"] & ~line_list["is_deces"]

    current_run.log_info(
        f"Indicateurs au grain cas : {len(line_list)} cas, "
        f"{int(line_list['is_confirme'].sum())} confirmés, "
        f"{int(line_list['is_non_cas'].sum())} non-cas, "
        f"{int(line_list['is_deces_confirme'].sum())} décès confirmés, "
        f"{int(line_list['is_gueri'].sum())} guéris."
    )

    n_derive = int(line_list["is_confirme"].sum())
    n_classification = int((line_list["classification_finale_cas"] == "Cas confirmé").sum())
    if n_derive != n_classification:
        current_run.log_warning(
            f"Écart de confirmation : {n_derive} cas via labo + alerte validée contre "
            f"{n_classification} « Cas confirmé » selon la classification finale DHIS2."
        )
    return line_list


def to_naive_datetime(series: pd.Series) -> pd.Series:
    """Parse en date, normalisée à minuit, toujours en datetime64 naïf (sans fuseau).

    Certaines colonnes de dates arrivent tz-aware (UTC, ex. ``date_notification``
    en ``pl.Datetime`` côté LLN) et d'autres naïves (``pl.Date``) — un merge
    ultérieur sur des colonnes ``date_rapportage`` de fuseaux différents échoue
    sinon (pandas refuse de fusionner ``datetime64[us]`` et
    ``datetime64[ns, UTC]``). ``utc=True`` uniformise d'abord tout en UTC
    (aware) avant de retirer le fuseau — la date obtenue après normalisation
    est donc identique à celle qu'aurait donnée l'ancien code sur une colonne
    déjà tz-aware (seule l'étiquette de fuseau est retirée, pas la valeur).

    Returns:
        pd.Series: La série, toujours en ``datetime64[ns]`` naïf, à minuit.
    """
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()


def reconstruct_date_deces(df: pd.DataFrame) -> pd.Series:
    """Reconstruit une date de décès unique par cas.

    Priorité aux dates réellement saisies : date de décès finale, sinon date
    notifiée du décès, sinon date de décès saisie en PCI. À défaut, proxys
    (date de prélèvement si patient décédé au prélèvement, sinon date de
    notification) lorsque le décès est avéré (is_deces).

    Args:
        df: Liste de ligne contenant les dates et drapeaux de décès.

    Returns:
        La série des dates de décès (NaT si non décédé ou date inconnue).
    """
    date_deces = df["date_deces_final"].fillna(df["date_deces_notification"])

    proxy_pci = date_deces.isna() & df["is_deces"] & df["date_deces_pci"].notna()
    date_deces = date_deces.mask(proxy_pci, df["date_deces_pci"])

    proxy_prelev = (
        date_deces.isna()
        & df["is_deces"]
        & (df["statut_patient_prelevement"] == "Décédé")
        & df["date_prelevement"].notna()
    )
    date_deces = date_deces.mask(proxy_prelev, df["date_prelevement"])

    proxy_notif = date_deces.isna() & df["is_deces"] & df["date_notif"].notna()
    date_deces = date_deces.mask(proxy_notif, df["date_notif"])

    # Publiée uniquement pour les décès avérés : date_deces_final, première de la
    # cascade, n'établit plus le décès à elle seule (cf. compute_indicators).
    return date_deces.where(df["is_deces"])


def build_line_list_individu(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
) -> pd.DataFrame:
    """Construit la liste de ligne nominative (grain cas) pour le tableau de bord.

    Reconstruit la date de décès, rattache les coordonnées carto (ZS et
    province), dérive les délais (en jours, bornés), le statut vital et les
    variables labo (résultat, valeurs Ct des positifs, classe Ct), puis restreint
    au schéma publié (config.LLN_COLS).

    Args:
        indicators: Liste de ligne enrichie issue de compute_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).

    Returns:
        La liste de ligne individuelle, une ligne par cas, colonnes LLN_COLS.
    """
    line_list = indicators.copy()
    line_list["date_deces"] = reconstruct_date_deces(line_list)

    # ── Coordonnées carto (ZS puis province) ─────────────────────────────────
    # NB : redondant (géométrie répétée par ligne) ; à externaliser plus tard.
    line_list = line_list.merge(
        ou_zone_sante.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_zs"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )
    line_list["geo_hierarchie"] = line_list["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:2])
    )
    line_list = line_list.merge(
        ou_provinces.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_province"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )

    # ── Délais (jours, float - bornés aux valeurs plausibles) ────────────────
    for nom, (col_fin, col_debut) in config.DELAI_DEFS.items():
        delai = (line_list[col_fin] - line_list[col_debut]).dt.total_seconds() / 86_400
        min_, max_ = config.DELAI_BORNES[nom]
        line_list[nom] = delai.where(delai.between(min_, max_), other=np.nan)

    # Statut vital (Décédé → Guéri → Vivant)
    line_list["statut_vital"] = np.where(
        line_list["is_deces"], "Décédé", np.where(line_list["is_gueri"], "Guéri", "Vivant")
    )

    # ── Labo : résultat + valeurs Ct (positifs uniquement) + classe Ct ───────
    line_list["resultat_labo"] = line_list["resultat_final_mve"]
    positif = line_list["is_confirme"].astype(bool)
    for col in ("valeur_ct_ebov", "valeur_ct_hec"):
        line_list[col] = pd.to_numeric(line_list[col], errors="coerce").where(positif)
    line_list["ct_ebov_classe"] = pd.cut(
        line_list["valeur_ct_ebov"],
        bins=[-np.inf, 18, 21, 24, 27, 30, 33, np.inf],
        labels=["<18", "18–21", "21–24", "24–27", "27–30", "30–33", ">33"],  # ruff: ignore[ambiguous-unicode-character-string]
        right=False,
    ).astype("object")

    # Schéma publié - reindex pour tolérer une colonne absente (créée à NULL
    # plutôt que de lever KeyError).
    manquantes = [c for c in config.LLN_COLS if c not in line_list.columns]
    if manquantes:
        current_run.log_warning(f"LLN : colonnes absentes créées à NULL : {manquantes}.")
    current_run.log_info(f"Liste de ligne individuelle : {len(line_list)} cas.")
    return line_list.reindex(columns=config.LLN_COLS)
