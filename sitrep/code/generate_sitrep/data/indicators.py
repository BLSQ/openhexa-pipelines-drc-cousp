"""Calcul des indicateurs MVE — pivot au niveau ENROLLMENT.
=========================================================

Principes de conception
------------------------
- Pivot enrollment-level (1 ligne / enrollment), aggregate_function="last".
  Aucune condition ne croise des DE de stages différents sans passer par le pivot.
- Distinction explicite CUMUL vs STOCK dans le nom de chaque indicateur.
- Sémantique des codes vérifiée sur l'optionSet réel (pas supposée).
- Données manquantes -> ND, jamais zéro implicite. Les indicateurs de STOCK
  dépendent du stage "Statut final" ; s'il est trop peu rempli, ils sont
  automatiquement neutralisés (mis à None) par le garde-fou de complétude.

Sémantique confirmée des data elements
--------------------------------------
  KhsBtTYkFZd  Conclusion alerte     (stage Notification)  : VAL / INV / Enc / Ninv
  D6kduc7OZnS  Classification cas    (stage Labo)           : CC / CP / CS / NC
  j6xabrRDJuo  Résultat labo final   (stage Labo)           : POS / NEG / INV
  nniQIfMGBDC  Statut AU PRÉLÈVEMENT (stage Prélèvement)    : V / D   (figé, != devenir)
  Za0cx3pmcWW  Statut FINAL          (stage Statut final)   : D / V   (devenir, ~7% rempli)
  W2u38gg9Jy8  Date sortie isolement (stage Statut final)   : DATE
  jHaeHsB6JbW  Devenir cas suspect   (stage Notification)   : CTE / TCTE / PREL / RTP
  USnTDONKNN8  Type de prélèvement   (stage Prélèvement)
  HBw0c2Cg8GU  Date réception labo   (stage Labo)
  F0gpBf9R11P  Date investigation    (stage Notification)

Usage
-----
    from indicateurs_mve import construire_pivot, calculer_indicateurs, agreger_par_zone

    piv = construire_pivot(df_mve_notif, date_min=date(2026, 5, 1))
    df_ind = calculer_indicateurs(piv)            # 1 ligne / enrollment + colonnes indicateurs
    df_zone = agreger_par_zone(df_ind, org_units) # agrégat géographique pour le SitRep
"""  # noqa: D205

from __future__ import annotations

from datetime import date

import polars as pl
from data.loader import _clean, _read_db  # noqa: PLC2701

# DE de référence (pour documentation / validation de présence des colonnes)
DE_CONCLUSION_ALERTE = "KhsBtTYkFZd"
DE_CLASSIFICATION = "D6kduc7OZnS"
DE_RESULTAT_LABO = "j6xabrRDJuo"
DE_STATUT_PRELEVEMENT = "nniQIfMGBDC"
DE_STATUT_FINAL = "Za0cx3pmcWW"
DE_DATE_SORTIE_ISOLEMENT = "W2u38gg9Jy8"
DE_DEVENIR_SUSPECT = "jHaeHsB6JbW"
DE_TYPE_PRELEVEMENT = "USnTDONKNN8"
DE_DATE_RECEPTION_LABO = "HBw0c2Cg8GU"
DE_DATE_INVESTIGATION = "F0gpBf9R11P"

# --- Stage PRISE EN CHARGE (PEC) — devenir réel du patient ---
# Non encore présent dans l'extraction actuelle ; le module bascule en mode
# dégradé (ND) tant que ces DE sont absents.
DE_PEC_DATE_ADMISSION = "KGsTJ4jV7Fb"  # MVE - PEC - Date d'admission
DE_PEC_DATE_SORTIE = "Xy5J5MGpaZ7"  # MVE - PEC - Date de sortie
DE_PEC_ISSUE = "WKZu0kp6wWu"  # Statut au moment de la sortie (optionSet eCWs0ZcUuRq)
# Codes de l'optionSet "MVE - Issue du patient" (eCWs0ZcUuRq) :
ISSUE_GUERI = "GR"  # Guéri(e)
ISSUE_DECEDE = "DCD"  # Décédé(e)
ISSUE_EVADE = "EVD"  # Évadé(e)
ISSUE_TRANSFERE = "TRF"  # Transféré(e)
ISSUE_NON_CAS = "NC"  # Non cas
# Sorties = ne sont plus en isolement (toute issue documentée vide le lit)
ISSUES_SORTIE = [ISSUE_GUERI, ISSUE_DECEDE, ISSUE_EVADE, ISSUE_TRANSFERE, ISSUE_NON_CAS]

# Codes d'isolement comptés comme "orienté vers une structure d'isolement"
CODES_ISOLEMENT = ["CTE", "TCTE"]  # CTE = isolé au CTE, TCTE = transféré vers CTE

# Seuil de complétude du stage "Statut final" en dessous duquel les
# indicateurs de STOCK ne sont pas fiables et sont neutralisés (-> None / ND).
SEUIL_COMPLETUDE_STATUT_FINAL = 0.50

# Indicateurs de STOCK soumis au garde-fou de complétude.
INDICATEURS_STOCK = ["n_isole_stock", "n_cas_actifs_stock"]

# Indicateurs basés sur la PEC (devenir réel). Fiables uniquement quand le
# stage PEC est présent ET suffisamment rempli ; sinon -> ND.
INDICATEURS_PEC = [
    "n_gueris",
    "n_deces_pec",
    "n_evades",
    "n_transferes",
    "n_isole_stock_pec",
]

# Indicateurs de FLUX/CUMUL toujours fiables (ne dépendent pas du devenir).
INDICATEURS_CUMUL = [
    "n_confirmes",
    "n_probables",
    "n_suspects_en_cours",
    "n_deces",
    "n_deces_confirmes",
    "n_deces_suspect",
    "n_orientes_isolement_cumul",
    "n_confirme_isole",
    "n_suspect_isole",
    "n_alertes",
    "n_alertes_investiguees",
    "n_alertes_validees",
    "n_alertes_invalidees",
    "n_echantillons_collectes",
    "n_echantillons_positifs",
    "n_echantillons_en_cours",
    "n_echantillons_invalides",
    "n_positifs_suspect_decedes",
]

TOUS_INDICATEURS = INDICATEURS_CUMUL + INDICATEURS_STOCK + INDICATEURS_PEC


def build_pivot(
    df_mve_notif: pl.DataFrame,
    date_min: date = date(2026, 5, 1),
    date_max: date | None = None,
) -> pl.DataFrame:
    """Pivote l'extraction tracker au niveau ENROLLMENT.

    Une ligne par enrollment ; chaque DE devient une colonne dont la valeur
    est la DERNIÈRE valeur connue (tri par created_at) — équivalent Polars
    d'un program indicator DHIS2 de type ENROLLMENT.

    Le filtre temporel s'applique sur enrolled_at (ancrage correct pour les
    bornes de période). Un plafond date_max permet d'écarter d'éventuelles
    dates aberrantes dans le futur.
    """  # noqa: DOC201
    flt = pl.col("data_element_id").is_not_null() & (
        pl.col("enrolled_at") >= pl.datetime(date_min.year, date_min.month, date_min.day)
    )
    if date_max is not None:
        flt = flt & (
            pl.col("enrolled_at") <= pl.datetime(date_max.year, date_max.month, date_max.day)
        )

    org_units = df_mve_notif.select(
        [
            "enrollment_org_unit",
            "level_1_name",
            "level_2_name",
            "level_3_name",
            "level_4_name",
        ]
    ).unique(subset="enrollment_org_unit", keep="last")

    df_pivot = (
        df_mve_notif.filter(flt)
        .sort(["tracked_entity_id", "enrollment_id", "created_at"])
        .pivot(
            on="data_element_id",
            index=[
                "enrollment_id",
                "tracked_entity_id",
                "enrollment_org_unit",
                "enrolled_at",
            ],
            values="value",
            aggregate_function="last",
        )
        .with_columns(pl.col("enrolled_at").dt.date())
    )

    return df_pivot.join(org_units, on="enrollment_org_unit", how="left")


def _col(df: pl.DataFrame, de_id: str) -> pl.Expr:
    """Renvoie l'expression de colonne pour un DE, ou une colonne de None typée.

    Si le DE est absent du pivot (évite un KeyError quand un stage est vide
    sur la période). Permet au module de tourner sur des extractions partielles.

    Returns:
        Une expression Polars pour le DE, ou une colonne None typée.
    """
    if de_id in df.columns:
        return pl.col(de_id)
    return pl.lit(None, dtype=pl.Utf8).alias(de_id)


def compute_indicators_mve_notifications(
    df_pivot: pl.DataFrame,
    appliquer_garde_fou_stock: bool = True,
) -> pl.DataFrame:
    """Ajoute les colonnes indicateurs (0/1 par enrollment) au pivot.

    Si appliquer_garde_fou_stock=True (défaut), les indicateurs de STOCK
    sont mis à None lorsque la complétude du stage "Statut final" est
    inférieure à SEUIL_COMPLETUDE_STATUT_FINAL — auquel cas ils devront
    être affichés "ND" dans le SitRep plutôt qu'un chiffre trompeur.
    """  # noqa: DOC201
    c_concl = _col(df_pivot, DE_CONCLUSION_ALERTE)
    c_class = _col(df_pivot, DE_CLASSIFICATION)
    c_labo = _col(df_pivot, DE_RESULTAT_LABO)
    c_prel = _col(df_pivot, DE_STATUT_PRELEVEMENT)
    c_final = _col(df_pivot, DE_STATUT_FINAL)
    c_sortie = _col(df_pivot, DE_DATE_SORTIE_ISOLEMENT)
    c_devenir = _col(df_pivot, DE_DEVENIR_SUSPECT)
    c_type_prel = _col(df_pivot, DE_TYPE_PRELEVEMENT)
    c_recep = _col(df_pivot, DE_DATE_RECEPTION_LABO)
    c_invest = _col(df_pivot, DE_DATE_INVESTIGATION)
    c_pec_issue = _col(df_pivot, DE_PEC_ISSUE)
    c_pec_adm = _col(df_pivot, DE_PEC_DATE_ADMISSION)
    c_pec_sortie = _col(df_pivot, DE_PEC_DATE_SORTIE)

    est_decede = (c_final == "D") | (c_prel == "D")
    est_oriente_isolement = c_devenir.is_in(CODES_ISOLEMENT)
    echantillon_collecte = c_type_prel.is_not_null() & c_recep.is_not_null()

    df = df_pivot.with_columns(
        # ---- CAS (cumul) ----
        pl.when(c_class == "CC").then(1).otherwise(0).alias("n_confirmes"),
        pl.when(c_class == "CP").then(1).otherwise(0).alias("n_probables"),
        pl.when((c_concl == "VAL") & (c_class.is_in(["CS"]) | c_class.is_null()))
        .then(1)
        .otherwise(0)
        .alias("n_suspects_en_cours"),
        pl.when(
            ((c_concl == "VAL") & (c_class.is_in(["CS"]) | c_class.is_null()))
            & (c_invest.is_not_null() | (c_concl == "Enc"))
        )
        .then(1)
        .otherwise(0)
        .alias("n_suspects_en_cours_investigation"),
        # ---- DÉCÈS (cumul, 2 sources combinées) ----
        pl.when(est_decede).then(1).otherwise(0).alias("n_deces"),
        pl.when((c_class == "CC") & est_decede).then(1).otherwise(0).alias("n_deces_confirmes"),
        pl.when((c_class == "CS") & est_decede).then(1).otherwise(0).alias("n_deces_suspect"),
        # ---- DEVENIR via PRISE EN CHARGE (PEC) — source dédiée ----
        # Issue du patient (optionSet eCWs0ZcUuRq). GR = vrai code "guéri".
        pl.when(c_pec_issue == ISSUE_GUERI).then(1).otherwise(0).alias("n_gueris"),
        pl.when(c_pec_issue == ISSUE_DECEDE).then(1).otherwise(0).alias("n_deces_pec"),
        pl.when(c_pec_issue == ISSUE_EVADE).then(1).otherwise(0).alias("n_evades"),
        pl.when(c_pec_issue == ISSUE_TRANSFERE).then(1).otherwise(0).alias("n_transferes"),
        # STOCK isolés fiable via PEC : admis (date d'admission) sans issue de sortie.
        pl.when(c_pec_adm.is_not_null() & c_pec_issue.is_null() & c_pec_sortie.is_null())
        .then(1)
        .otherwise(0)
        .alias("n_isole_stock_pec"),
        # ---- ISOLEMENT ----
        pl.when(est_oriente_isolement).then(1).otherwise(0).alias("n_orientes_isolement_cumul"),
        pl.when(
            est_oriente_isolement
            & c_sortie.is_null()
            & (c_final != "D").fill_null(True)
            & (c_prel != "D").fill_null(True)
        )
        .then(1)
        .otherwise(0)
        .alias("n_isole_stock"),
        pl.when((c_class == "CC") & est_oriente_isolement)
        .then(1)
        .otherwise(0)
        .alias("n_confirme_isole"),
        pl.when((c_class == "CS") & est_oriente_isolement)
        .then(1)
        .otherwise(0)
        .alias("n_suspect_isole"),
        # ---- CAS CONFIRMÉS ACTIFS (stock) ----
        pl.when(
            (c_class == "CC")
            & (c_final != "D").fill_null(True)
            & (c_prel != "D").fill_null(True)
            & c_sortie.is_null()
        )
        .then(1)
        .otherwise(0)
        .alias("n_cas_actifs_stock"),
        # ---- ALERTES ----
        pl.lit(1).alias("n_alertes"),
        pl.when(c_invest.is_not_null() | (c_concl == "Enc"))
        .then(1)
        .otherwise(0)
        .alias("n_alertes_investiguees"),
        pl.when(c_concl == "VAL").then(1).otherwise(0).alias("n_alertes_validees"),
        pl.when(c_concl == "INV").then(1).otherwise(0).alias("n_alertes_invalidees"),
        # ---- ÉCHANTILLONS ----
        pl.when(echantillon_collecte).then(1).otherwise(0).alias("n_echantillons_collectes"),
        pl.when(echantillon_collecte & (c_labo == "POS"))
        .then(1)
        .otherwise(0)
        .alias("n_echantillons_positifs"),
        pl.when(echantillon_collecte & c_labo.is_null())
        .then(1)
        .otherwise(0)
        .alias("n_echantillons_en_cours"),
        pl.when(echantillon_collecte & (c_labo == "INV"))
        .then(1)
        .otherwise(0)
        .alias("n_echantillons_invalides"),
        pl.when((c_class == "CC") & est_decede & echantillon_collecte & (c_labo == "POS"))
        .then(1)
        .otherwise(0)
        .alias("n_positifs_suspect_decedes"),
    )

    if appliquer_garde_fou_stock:
        df = _neutraliser_stock_si_incomplet(df, df_pivot)
        df = _neutraliser_pec_si_incomplet(df, df_pivot)

    return df


def _neutraliser_pec_si_incomplet(df: pl.DataFrame, df_pivot: pl.DataFrame) -> pl.DataFrame:
    """Neutralise les indicateurs PEC (-> None / ND) si le stage Prise en charge
    est absent de l'extraction ou rempli sous le seuil. Tant que la PEC n'est
    pas accessible, tous les indicateurs de devenir restent ND.
    """  # noqa: D205, DOC201
    if DE_PEC_ISSUE in df_pivot.columns:
        completude = df_pivot[DE_PEC_ISSUE].is_not_null().mean() or 0.0
    else:
        completude = 0.0

    if completude < SEUIL_COMPLETUDE_STATUT_FINAL:  # type: ignore
        df = df.with_columns([pl.lit(None, dtype=pl.Int32).alias(c) for c in INDICATEURS_PEC])
    return df


def _neutraliser_stock_si_incomplet(df: pl.DataFrame, df_pivot: pl.DataFrame) -> pl.DataFrame:
    """Si la complétude du stage Statut final < seuil, met les indicateurs de
    STOCK à None (ils seront affichés ND). Les indicateurs de cumul restent.
    """  # noqa: D205, DOC201
    if DE_STATUT_FINAL in df_pivot.columns:
        completude = df_pivot[DE_STATUT_FINAL].is_not_null().mean() or 0.0
    else:
        completude = 0.0

    if completude < SEUIL_COMPLETUDE_STATUT_FINAL:  # type: ignore
        df = df.with_columns([pl.lit(None, dtype=pl.Int32).alias(c) for c in INDICATEURS_STOCK])
    return df


def diagnostic_completude(df_pivot: pl.DataFrame) -> pl.DataFrame:
    """Renvoie le taux de remplissage des DE clés (aide à décider des ND)."""  # noqa: DOC201
    des = [
        DE_CONCLUSION_ALERTE,
        DE_CLASSIFICATION,
        DE_RESULTAT_LABO,
        DE_STATUT_PRELEVEMENT,
        DE_STATUT_FINAL,
        DE_DATE_SORTIE_ISOLEMENT,
        DE_DEVENIR_SUSPECT,
        DE_TYPE_PRELEVEMENT,
        DE_DATE_RECEPTION_LABO,
        DE_DATE_INVESTIGATION,
        DE_PEC_DATE_ADMISSION,
        DE_PEC_DATE_SORTIE,
        DE_PEC_ISSUE,
    ]
    n = df_pivot.height
    rows = []
    for de in des:
        renseigne = df_pivot[de].is_not_null().sum() if de in df_pivot.columns else 0
        rows.append(
            {
                "data_element": de,
                "renseigne": renseigne,
                "total": n,
                "completude": (renseigne / n) if n else 0.0,
            }
        )
    return pl.DataFrame(rows).sort("completude")


def build_definitive_from_raw(df_mve: pl.DataFrame) -> pl.DataFrame:
    """Transforme une extraction tracker brute (format long) en schéma interne.

    Returns:
        pl.DataFrame: Le DataFrame nettoyé au schéma interne (grain enrollment,
        drapeaux ``n_*``, attributs TEI, géo canonisée).
    """
    df_tei = (
        df_mve.select(
            [
                pl.col("tracked_entity_id"),
                pl.col("MVE - Numéro Epid - Alerte MVE").alias("num_epid"),
                pl.col("MPOX-N-Date et heure de notification de l'alerte").alias(
                    "date_notification"
                ),
                pl.col("MVE - DDS (Date de début des symptômes)").alias("date_debut_symptomes"),
                pl.col("MVE-N-Sexe").alias("sexe"),
                pl.col("MVE - Age(ans)").alias("age"),
                pl.col("MVE-N-Age < 1 an ?").alias("age_<1an"),
            ]
        )
        .unique()
        .with_columns(
            pl.col("date_notification")
            .cast(pl.Datetime, strict=False)
            .dt.date()
            .alias("date_notification"),
            pl.col("date_debut_symptomes").cast(pl.Date).alias("date_debut_symptomes"),
        )
    )

    df_ind = compute_indicators_mve_notifications(
        build_pivot(df_mve), appliquer_garde_fou_stock=False
    )

    df_comp = df_ind.join(df_tei, on="tracked_entity_id", how="left")

    return _clean(
        df_comp.with_columns(
            pl.col("enrolled_at").cast(pl.Datetime, strict=False).dt.date().alias("enrolled_at")
        )
    )


def build_definitive_data(table: str | None = None, schema: str | None = None) -> pl.DataFrame:
    """Renvoie le dataframe définitif depuis la table SQL du workspace (OpenHexa).

    Returns:
        pl.DataFrame: Le DataFrame nettoyé au schéma interne.
    """
    return build_definitive_from_raw(_read_db(table, schema))
