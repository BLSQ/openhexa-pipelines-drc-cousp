from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

import config
import numpy as np
import pandas as pd
import polars as pl
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2, dataframe
from utils import compter_oui, parse_geo, tranche_age


@pipeline("compute_indicators_mve_tdb")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    name="Connexion DHIS2",
    help="Connexion à l'instance tracker MVE",
    required=True,
)
@parameter(
    "date_min",
    type=str,
    name="Date de début (incluse)",
    help="Borne basse sur enrolled_at, au format YYYY-MM-DD.",
    default="2026-05-01",
)
@parameter(
    "date_max",
    type=str,
    name="Date de fin (incluse)",
    help="Borne haute sur enrolled_at (YYYY-MM-DD). Laisser vide pour aucun plafond.",
    required=False,
)
def compute_indicators_mve_tdb(
    dhis_con: DHIS2Connection,
    date_min: str,
    date_max: str | None = None,
) -> None:
    """Calcule et publie les indicateurs du tableau de bord MVE."""
    fenetre_min = date.fromisoformat(date_min)
    fenetre_max = date.fromisoformat(date_max) if date_max else None

    tracker = DHIS2(dhis_con, Path(workspace.files_path) / ".cache")

    borne_max = fenetre_max.isoformat() if fenetre_max else "aucune"
    current_run.log_info(
        f"Fenêtre d'analyse sur enrolled_at : {fenetre_min.isoformat()} → {borne_max}."
    )

    db_url = workspace.database_url
    org_units = get_organisation_units(tracker)

    indicators = build_indicators(org_units, fenetre_min, fenetre_max)
    ou_zone_sante = build_org_units(org_units, "zone_sante")
    ou_provinces = build_org_units(org_units, "province")

    for colonne_date, table_name in config.AXES_EXPORT:
        export_aggregate(indicators, ou_zone_sante, ou_provinces, colonne_date, table_name, db_url)

    export_individu(indicators, ou_zone_sante, ou_provinces, db_url)


@compute_indicators_mve_tdb.task
def get_organisation_units(tracker: DHIS2) -> pl.DataFrame:
    """Récupère les unités d'organisation DHIS2 (métadonnées + géométries).

    Args:
        tracker: instance DHIS2 utilisée pour accéder aux métadonnées.

    Returns:
        DataFrame contenant les unités d'organisation.
    """
    return dataframe.get_organisation_units(tracker)


@compute_indicators_mve_tdb.task
def build_indicators(
    org_units: pl.DataFrame,
    date_min: date,
    date_max: date | None,
) -> pd.DataFrame:
    """Construit la liste de ligne enrichie des indicateurs, au grain cas.

    Args:
        org_units: Unités d'organisation (jointure géographique du pivot).
        date_min: Borne basse incluse sur enrolled_at.
        date_max: Borne haute incluse sur enrolled_at, ou None.

    Returns:
        La liste de ligne (pandas) avec colonnes dérivées et drapeaux is_*.
    """
    events = load_notification_events()
    enrollments = pivot_enrollments(events, org_units, date_min, date_max)
    tei = extract_tei_attributes(events)
    lab_summary = build_lab_summary(events, tei, date_min, date_max)
    line_list = consolidate_line_list(enrollments, tei, lab_summary)
    return compute_indicators(line_list)


@compute_indicators_mve_tdb.task
def export_aggregate(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
    colonne_date: str,
    table_name: str,
    db_url: str,
) -> None:
    """Agrège un axe temporel et publie sa table de staging (branche DAG isolée).

    Args:
        indicators: Liste de ligne enrichie issue de build_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).
        colonne_date: Axe temporel d'agrégation (cf. config.AXES_EXPORT).
        table_name: Table de staging cible.
        db_url: URI de connexion à la base du workspace.
    """
    agg = aggregate_indicators(indicators, ou_zone_sante, ou_provinces, colonne_date)
    export_to_database(agg, table_name, db_url)


@compute_indicators_mve_tdb.task
def export_individu(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
    db_url: str,
) -> None:
    """Construit et publie la liste de ligne nominative (branche DAG isolée).

    Args:
        indicators: Liste de ligne enrichie issue de build_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).
        db_url: URI de connexion à la base du workspace.
    """
    individu = build_line_list_individu(indicators, ou_zone_sante, ou_provinces)
    export_to_database(individu, config.LLN_TABLE, db_url)


def load_notification_events() -> pl.DataFrame:
    """Charge la table d'événements de notification MVE depuis le workspace.

    Renomme les attributs d'entité suivie (config.DICO_TEI) et type les dates
    de notification et de début des symptômes.

    Returns:
        Les événements bruts (grain événement DHIS2), prêts à être pivotés.
    """
    df = pl.read_database_uri(
        'SELECT * FROM "public"."mve_notification_events"',
        uri=workspace.database_url,
    )
    # df = df.filter(pl.col("data_element_id").is_in(list(config.DICO_DE_MAPPING.values())))
    df = df.rename(config.DICO_TEI).with_columns(
        pl.col("date_notification").cast(pl.Datetime, strict=False),  # .dt.date(),
        pl.col("date_debut_symptomes").cast(pl.Date, strict=False),
    )
    current_run.log_info(f"Événements de notification chargés : {df.height} lignes.")
    return df


def pivot_enrollments(
    events: pl.DataFrame,
    org_units: pl.DataFrame,
    date_min: date,
    date_max: date | None,
) -> pl.DataFrame:
    """Pivote les événements tracker au grain ENROLLMENT (une ligne par enrôlement).

    Chaque data element devient une colonne portant sa DERNIÈRE valeur connue
    (tri par created_at) — équivalent Polars d'un program indicator DHIS2 de
    type ENROLLMENT. Le filtre temporel s'applique sur enrolled_at ; le plafond
    date_max (optionnel) écarte les dates aberrantes dans le futur. La hiérarchie
    géographique est rattachée depuis org_units.

    Args:
        events: Événements bruts issus de load_notification_events().
        org_units: Unités d'organisation (noms de niveaux 1 à 4).
        date_min: Borne basse incluse sur enrolled_at.
        date_max: Borne haute incluse sur enrolled_at, ou None.

    Returns:
        Un DataFrame au grain enrôlement, une colonne par data element.
    """
    fenetre = pl.col("data_element_id").is_not_null() & (
        pl.col("enrolled_at") >= pl.datetime(date_min.year, date_min.month, date_min.day)
    )
    if date_max is not None:
        fenetre = fenetre & (
            pl.col("enrolled_at") <= pl.datetime(date_max.year, date_max.month, date_max.day)
        )

    enrollments = (
        events.filter(fenetre)  # noqa: PD010 — .pivot() Polars, pas pandas
        .sort(["tracked_entity_id", "enrollment_id", "created_at"])
        .pivot(
            on="data_element_id",
            index=[
                "enrollment_id",
                "tracked_entity_id",
                "enrollment_org_unit",
                "enrolled_at",
            ],
            values="value_norm",
            aggregate_function="last",
        )
    )
    enrollments = enrollments.join(
        org_units.select(
            ["id", "level_1_name", "level_2_name", "level_3_name", "level_4_name"]
        ).rename({"id": "enrollment_org_unit"}),
        on="enrollment_org_unit",
        how="left",
    )
    current_run.log_info(f"Pivot au grain enrôlement : {enrollments.height} enrôlements retenus.")
    return enrollments


def extract_tei_attributes(
    events: pl.DataFrame,
    colonnes: list[str] = config.EXPR_TEI,
) -> pl.DataFrame:
    """Extrait les attributs d'entité suivie (TEI), un enregistrement par TEI.

    Returns:
        Les attributs TEI dédoublonnés (dernière occurrence par tracked_entity_id).
    """
    tei = (
        events.select(colonnes)
        .sort("tracked_entity_id")
        .unique(subset=["tracked_entity_id"], keep="last")
    )
    current_run.log_info(f"Attributs TEI extraits : {tei.height} entités suivies.")
    return tei


def build_lab_summary(
    events: pl.DataFrame,
    tei: pl.DataFrame,
    date_min: date,
    date_max: date | None,
) -> pl.DataFrame:
    """Résume l'historique de laboratoire par enrôlement (data element « Résultat final MVE »).

    Agrège les tests successifs (Positif/Négatif/Invalide) : confirmation, dates
    clés, compteurs et drapeaux de réversion (positif puis négatif / invalide).

    Returns:
        Un résumé labo par enrôlement, enrichi des attributs TEI.
    """
    de_resultat = config.DICO_DE_MAPPING["resultat_final_mve"]
    fenetre = (pl.col("data_element_id") == de_resultat) & (
        pl.col("enrolled_at") >= pl.datetime(date_min.year, date_min.month, date_min.day)
    )
    if date_max is not None:
        fenetre = fenetre & (
            pl.col("enrolled_at") <= pl.datetime(date_max.year, date_max.month, date_max.day)
        )

    lab_summary = (
        events.filter(fenetre)
        .with_columns(
            pl.col("occurred_at").alias("event_dt"),
            (pl.col("value_norm") == "Positif").alias("is_pos"),
            (pl.col("value_norm") == "Négatif").alias("is_neg"),
            (pl.col("value_norm") == "Invalide").alias("is_inv"),
        )
        .group_by(["enrollment_id", "enrolled_at", "tracked_entity_id"])
        .agg(
            # A déjà été positif au moins une fois
            pl.col("is_pos").any().alias("lab_confirme"),
            # Date du premier test positif
            pl.col("event_dt").filter(pl.col("is_pos")).min().alias("date_confirmation"),
            # Statut du dernier test connu
            pl.col("value_norm").sort_by("event_dt").last().alias("lab_resultat_courant"),
            # Date du dernier test
            pl.col("event_dt").max().alias("date_dernier_test"),
            # Compteurs de tests
            pl.len().alias("n_tests_labo"),
            pl.col("is_pos").sum().alias("n_pos"),
            pl.col("is_neg").sum().alias("n_neg"),
            pl.col("is_inv").sum().alias("n_inv"),
            # Positif puis négatif / invalide (réversion du statut)
            (
                pl.col("is_pos").any()
                & (pl.col("value_norm").sort_by("event_dt").last() == "Négatif")
            ).alias("flag_pos_puis_neg"),
            (
                pl.col("is_pos").any()
                & (pl.col("value_norm").sort_by("event_dt").last() == "Invalide")
            ).alias("flag_pos_puis_inv"),
        )
    )
    n_confirmes = int(lab_summary.get_column("lab_confirme").sum())
    current_run.log_info(
        f"Résumé labo : {lab_summary.height} enrôlements testés, dont {n_confirmes} confirmés."
    )
    return lab_summary.join(tei, on="tracked_entity_id", how="left")


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
    line_list = enrollments.join(tei, on="tracked_entity_id", how="left").with_columns(
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
        lab_summary.select(config.COLS_PRELEV),
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
        line_list[cible] = pd.to_datetime(line_list[source], errors="coerce")  # .dt.normalize()

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
    line_list["is_valide"] = line_list["resultat_final_mve"].isin(["Positif", "Négatif"])
    line_list["is_suspect"] = line_list["is_alerte_valide"] & ~line_list["is_valide"]
    line_list["is_confirme"] = (line_list["lab_confirme"] == True) & (  # noqa: E712
        line_list["conclusion_alerte"] == "Validée"
    )

    line_list["is_deces"] = (
        (line_list["nature_alerte"] == "Décès")
        | (line_list["statut_final_patient"] == "Décédé")
        | (line_list["date_deces_final"].notna())
        | (line_list["statut_patient_prelevement"] == "Décédé")
    )
    line_list["is_deces_confirme"] = line_list["is_deces"] & line_list["is_confirme"]
    line_list["is_deces_suspect"] = line_list["is_deces"] & line_list["is_suspect"]
    line_list["is_suspect_lien_epi"] = line_list["is_suspect"] & (
        line_list["lien_epidemiologique"] == "Oui"
    )

    # Stage PEC désactivé : la modalité de sortie n'est pas collectée → guéri = Faux
    modalite_sortie = line_list.get("modalite_sortie_cte")
    line_list["is_gueri"] = modalite_sortie.eq("Guéri(e)") if modalite_sortie is not None else False
    line_list["is_confirme_gueri"] = line_list["is_confirme"] & line_list["is_gueri"]
    line_list["is_confirme_vivant"] = (
        line_list["is_confirme"] & ~line_list["is_deces"] & ~line_list["is_gueri"]
    )

    current_run.log_info(
        f"Indicateurs au grain cas : {len(line_list)} cas, "
        f"{int(line_list['is_confirme'].sum())} confirmés, "
        f"{int(line_list['is_deces_confirme'].sum())} décès confirmés."
    )
    return line_list


@compute_indicators_mve_tdb.task
def build_org_units(
    org_units: pl.DataFrame,
    niveau: Literal["province", "zone_sante"],
) -> pl.DataFrame:
    """Prépare les unités d'organisation d'un niveau donné (province ou zone de santé).

    Filtre sur le niveau hiérarchique, reconstruit la hiérarchie géographique et
    extrait l'anneau extérieur du polygone, sérialisé en JSON (jointure carto).

    Args:
        org_units: Unités d'organisation issues de la toolbox DHIS2.
        niveau: « province » (level 2) ou « zone_sante » (level 3).

    Returns:
        Les unités du niveau demandé, avec geo_hierarchie et coordinates (JSON).
    """
    if niveau == "zone_sante":
        level, cols_geo = 3, ["level_1_name", "level_2_name", "level_3_name"]
    else:
        level, cols_geo = 2, ["level_1_name", "level_2_name"]

    def _anneau_exterieur(geom: object) -> object:
        """Anneau extérieur du polygone (les ZS sont imbriquées d'un niveau de plus).

        Returns:
            La liste de coordonnées de l'anneau extérieur, ou None si absente.
        """
        if not isinstance(geom, str):
            return None
        coords = json.loads(geom)["coordinates"][0]
        return coords[0] if niveau == "zone_sante" else coords

    prepared = (
        org_units.filter((pl.col("level") == level) & pl.col("geometry").is_not_null())
        .with_columns(
            pl.concat_str(cols_geo, separator=" / ").alias("geo_hierarchie"),
            pl.col("geometry")
            .map_elements(_anneau_exterieur, return_dtype=pl.Object)
            .alias("coordinates"),
        )
        .with_columns(
            pl.col("coordinates")
            .map_elements(json.dumps, return_dtype=pl.Utf8)
            .alias("coordinates")
        )
    )
    current_run.log_info(
        f"Unités d'organisation « {niveau} » préparées : {prepared.height} géométries."
    )
    return prepared


def reconstruct_date_deces(df: pd.DataFrame) -> pd.Series:
    """Reconstruit une date de décès unique par cas.

    Priorité : date de décès finale, sinon date notifiée du décès, sinon proxys
    (date de prélèvement si patient décédé au prélèvement, sinon date de
    notification) lorsque le décès est avéré (is_deces).

    Args:
        df: Liste de ligne contenant les dates et drapeaux de décès.

    Returns:
        La série des dates de décès (NaT si non décédé ou date inconnue).
    """
    date_deces = df["date_deces_final"].fillna(df["date_deces_notification"])

    proxy_prelev = (
        date_deces.isna()
        & df["is_deces"]
        & (df["statut_patient_prelevement"] == "Décédé")
        & df["date_prelevement"].notna()
    )
    date_deces = date_deces.mask(proxy_prelev, df["date_prelevement"])

    proxy_notif = date_deces.isna() & df["is_deces"] & df["date_notif"].notna()
    return date_deces.mask(proxy_notif, df["date_notif"])


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

    # ── Délais (jours, float — bornés aux valeurs plausibles) ────────────────
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
        labels=["<18", "18–21", "21–24", "24–27", "27–30", "30–33", ">33"],  # noqa: RUF001
        right=False,
    ).astype("object")

    # Schéma publié — reindex pour tolérer une colonne absente (créée à NULL
    # plutôt que de lever KeyError).
    manquantes = [c for c in config.LLN_COLS if c not in line_list.columns]
    if manquantes:
        current_run.log_warning(f"LLN : colonnes absentes créées à NULL : {manquantes}.")
    current_run.log_info(f"Liste de ligne individuelle : {len(line_list)} cas.")
    return line_list.reindex(columns=config.LLN_COLS)


def aggregate_indicators(
    indicators: pd.DataFrame,
    ou_zone_sante: pl.DataFrame,
    ou_provinces: pl.DataFrame,
    colonne_date: str,
) -> pd.DataFrame:
    """Agrège les indicateurs par date, géographie, sexe et tranche d'âge.

    Pour l'axe « date_deces », reconstruit d'abord une date de décès unique
    (date finale, sinon notification, sinon proxy prélèvement/notification) et
    restreint aux décès. Rattache enfin les coordonnées ZS et province.

    Args:
        indicators: Liste de ligne enrichie issue de compute_indicators().
        ou_zone_sante: Unités d'organisation zone de santé (coordonnées).
        ou_provinces: Unités d'organisation province (coordonnées).
        colonne_date: Axe temporel d'agrégation (date_notif, date_debut_symptomes
            ou date_deces).

    Returns:
        Les agrégats, une ligne par (date, ZS, province, sexe, tranche d'âge).
    """
    if colonne_date == "date_deces":
        indicators = indicators.copy()
        indicators["date_deces"] = reconstruct_date_deces(indicators)
        indicators = indicators[indicators["is_deces"]]

    group_keys = [
        colonne_date,
        "zone_sante",
        "province",
        "sexe_norm",
        "tranche_age",
        "geo_hierarchie",
    ]

    aggregated = (
        indicators.groupby(group_keys, dropna=False)
        .agg(
            n_alertes=("numero_epid", "nunique"),
            n_alertes_valides=("is_alerte_valide", "sum"),
            n_suspects=("is_suspect", "sum"),
            n_suspects_lien_epi=("is_suspect_lien_epi", "sum"),
            n_preleves=("is_preleve", "sum"),
            n_recus=("is_recu", "sum"),
            n_analyses=("is_analyse", "sum"),
            n_echantillons_valides=("is_valide", "sum"),
            n_confirmes=("is_confirme", "sum"),
            n_deces=("is_deces", "sum"),
            n_deces_suspects=("is_deces_suspect", "sum"),
            n_deces_confirmes=("is_deces_confirme", "sum"),
            n_gueri=("is_gueri", "sum"),
            # n_confirmes_deces == n_deces_confirmes (même drapeau, conservé pour le TDB)
            n_confirmes_deces=("is_deces_confirme", "sum"),
            n_confirmes_gueri=("is_confirme_gueri", "sum"),
            n_confirmes_vivants=("is_confirme_vivant", "sum"),
            # ── Signes cliniques ────────────────────────────────────────────────
            n_signe_fievre=("signe_fievre", compter_oui),
            n_signe_vomissements=("signe_nausees_vomissements", compter_oui),
            n_signe_diarrhees=("signe_diarrhees", compter_oui),
            n_signe_fatigue=("signe_fatigue", compter_oui),
            n_signe_cephalees=("signe_cephalees", compter_oui),
            n_signe_coma=("signe_coma", compter_oui),
            n_signe_confusion=("signe_confusion", compter_oui),
            n_signe_saignements=("signe_saignements", compter_oui),
            n_signe_saignement_gencives=("signe_saignement_gencives", compter_oui),
            n_signe_epistaxis=("signe_epistaxis", compter_oui),
            n_signe_melenas=("signe_melenas", compter_oui),
            n_signe_hemorragique=("signes_hemorragiques_maladie", compter_oui),
        )
        .reset_index()
        .sort_values([colonne_date, "province", "zone_sante"])
        .reset_index(drop=True)
    )

    aggregated = aggregated.merge(
        ou_zone_sante.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_zs"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    )
    # Réduit la hiérarchie ZS → province pour rattacher les coordonnées province
    aggregated["geo_hierarchie"] = aggregated["geo_hierarchie"].apply(
        lambda col: " / ".join(col.split(" / ")[:2])
    )
    aggregated = aggregated.merge(
        ou_provinces.select(["geo_hierarchie", "coordinates"])
        .rename({"coordinates": "coordinates_province"})
        .to_pandas(),
        on="geo_hierarchie",
        how="left",
    ).drop(columns="geo_hierarchie")
    current_run.log_info(f"Agrégation sur « {colonne_date} » : {len(aggregated)} lignes.")
    return aggregated


def export_to_database(
    df: pd.DataFrame,
    table_name: str,
    db_url: str,
    mode: Literal["append", "replace", "fail"] = "replace",
) -> None:
    """Écrit un DataFrame dans une table de la base du workspace (remplace par défaut).

    Args:
        df: Agrégats à publier.
        table_name: Nom de la table de staging cible.
        db_url: URI de connexion à la base du workspace (workspace.database_url).
        mode: Stratégie si la table existe (replace par défaut).
    """
    current_run.log_info(f"Export des données vers la table `{table_name}` de la base de données.")
    pl.DataFrame(df).write_database(
        table_name, connection=db_url, if_table_exists=mode, engine="adbc"
    )
    current_run.log_info(f"Table « {table_name} » écrite ({mode}) : {len(df)} lignes.")
    current_run.add_database_output(table_name)


if __name__ == "__main__":
    compute_indicators_mve_tdb()
