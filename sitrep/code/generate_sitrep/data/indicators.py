"""Calcul des indicateurs MVE — pivot au niveau ENROLLMENT.
=========================================================

Principes de conception
------------------------
- Pivot enrollment-level (1 ligne / enrollment), aggregate_function="last".
  Aucune condition ne croise des DE de stages différents sans passer par le pivot.
- Distinction explicite CUMUL vs STOCK dans le nom de chaque indicateur.
- Sémantique des codes vérifiée sur l'optionSet réel (pas supposée).
- **Restitution fidèle à la source** : aucun indicateur n'est masqué parce que son
  stage est peu rempli. Un stage partiellement saisi produit des compteurs
  faibles, ce qui est l'information réelle. Le ND est réservé aux champs
  réellement absents de l'extraction (suivi des contacts, lits CTE) et décidé
  côté ``reporting``, jamais ici.

CODES et non libellés
------------------------
``build_pivot`` pivote ``values="value"`` : toutes les conditions de ce module
comparent donc des **codes** d'optionSet (``D``, ``CC``, ``VAL``, ``GR``, ``POS``).
Le pipeline de tableau de bord ``compute_indicators_mve_tdb`` pivote au contraire
``value_norm``, soit les **libellés** décodés (« Décédé », « Cas confirmé »,
« Guéri(e) », « Positif »). Une condition transposée d'un pipeline à l'autre sans
traduction ne lève aucune erreur : elle ne matche simplement jamais. Exemple
vérifié : le code de la nature d'alerte est ``DCD``, pas « Décès » — comparé au
libellé, l'indicateur vaut 0 en silence. Ne pas copier de code entre les deux
modules ; retraduire.

Sémantique confirmée des data elements
--------------------------------------
  kdOYmDgoyAA  Nature de l'alerte    (stage Notification)  : Cas / DCD (entrée décédé)
  KhsBtTYkFZd  Conclusion alerte     (stage Notification)  : VAL / INV / Enc / Ninv
  D6kduc7OZnS  Classification cas    (stage Labo)           : CC / CP / CS / NC
  j6xabrRDJuo  Résultat labo final   (stage Labo)           : POS / NEG / INV
  nniQIfMGBDC  Statut AU PRÉLÈVEMENT (stage Prélèvement)    : V / D   (figé, != devenir)
  Za0cx3pmcWW  Statut FINAL          (stage Statut final)   : D / V   (devenir, ~5% rempli)
  x1aazi4fgKO  Date de décès         (stage Statut final)   : DATE
  fWBGDpJezOX  Date de décès PCI     (stage PCI)            : DATE
  W2u38gg9Jy8  Date sortie isolement (stage Statut final)   : DATE
  jHaeHsB6JbW  Devenir cas suspect   (stage Notification)   : CTE / TCTE / PREL / RTP
  USnTDONKNN8  Type de prélèvement   (stage Prélèvement)
  CxQAC5LkMtn  Date du prélèvement   (stage Prélèvement)    : DATE
  HBw0c2Cg8GU  Date réception labo   (stage Labo)
  F0gpBf9R11P  Date investigation    (stage Notification)
  WKZu0kp6wWu  Issue de la PEC       (stage Prise en charge): GR / DCD / EVD / TRF /
                                                              NC / RMCAR / Abandon / Référé
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
DE_DATE_PRELEVEMENT = "CxQAC5LkMtn"  # 183 - MVE - S5 - Date du prélèvement
DE_DATE_RECEPTION_LABO = "HBw0c2Cg8GU"
DE_DATE_INVESTIGATION = "F0gpBf9R11P"

# --- Nature de l'alerte : le patient entre-t-il dans le programme décédé ? ---
DE_NATURE_ALERTE = "kdOYmDgoyAA"  # MVE-N Nature de l'Alerte (stage Notification)
NATURE_ALERTE_DECES = "DCD"  # code du décès ; value_norm dirait « Décès »

# --- Dates de décès réellement saisies (hors statuts) ---
DE_DATE_DECES_S6 = "x1aazi4fgKO"  # 208 - MVE - S6 - Date de décès (stage Statut final)
DE_PCI_DATE_DECES = "fWBGDpJezOX"  # MVE - PCI9 - Date de décès (stage PCI)

# --- Stage PRISE EN CHARGE (PEC) — devenir réel du patient ---
DE_PEC_DATE_ADMISSION = "KGsTJ4jV7Fb"  # MVE - PEC - Date d'admission
DE_PEC_DATE_SORTIE = "Xy5J5MGpaZ7"  # MVE - PEC - Date de sortie
DE_PEC_ISSUE = "WKZu0kp6wWu"  # Statut au moment de la sortie (optionSet eCWs0ZcUuRq)
ISSUE_GUERI = "GR"  # Guéri(e)
ISSUE_DECEDE = "DCD"  # Décédé(e)
ISSUE_EVADE = "EVD"  # Évadé(e)
ISSUE_TRANSFERE = "TRF"  # Transféré(e)
ISSUE_NON_CAS = "NC"  # Non cas
ISSUE_RETOUR_MAISON = "RMCAR"  # Retour à la maison
ISSUE_ABANDON = "Abandon"  # Abandon (libellé, pas de code)
ISSUE_REFERE = "Référé"  # Référé (libellé, pas de code)
# Sorties = ne sont plus en isolement (toute issue documentée vide le lit)
ISSUES_SORTIE = [
    ISSUE_GUERI,
    ISSUE_DECEDE,
    ISSUE_EVADE,
    ISSUE_TRANSFERE,
    ISSUE_NON_CAS,
    ISSUE_RETOUR_MAISON,
    ISSUE_ABANDON,
    ISSUE_REFERE,
]

# Codes d'isolement comptés comme "orienté vers une structure d'isolement"
CODES_ISOLEMENT = ["CTE", "TCTE"]  # CTE = isolé au CTE, TCTE = transféré vers CTE

# Indicateurs de STOCK (photo à date, dépendent du devenir du patient).
INDICATEURS_STOCK = ["n_isole_stock", "n_cas_actifs_stock"]

# Indicateurs basés sur la PEC (devenir réel du patient pris en charge).
INDICATEURS_PEC = [
    "n_gueris",
    "n_deces_pec",
    "n_evades",
    "n_transferes",
    "n_isole_stock_pec",
    "n_pec_admis",
    "n_pec_encore_admis",
    "n_pec_sorties",
    "n_pec_non_cas",
    "n_pec_retour_maison",
    "n_pec_abandon",
    "n_pec_refere",
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

    retenus = df_mve_notif.filter(flt)
    cles_tri = [c for c in ("occurred_at", "created_at", "event_id") if c in retenus.columns]

    contexte = retenus.group_by("enrollment_id").agg(
        pl.col("tracked_entity_id").sort_by(cles_tri, nulls_last=True).last(),
        pl.col("enrolled_at").sort_by(cles_tri, nulls_last=True).last(),
        pl.col("enrollment_org_unit").sort_by(cles_tri, nulls_last=True).last(),
    )

    valeurs = retenus.sort(cles_tri, nulls_last=True).pivot(
        on="data_element_id",
        index="enrollment_id",
        values="value",
        aggregate_function="last",
    )

    df_pivot = (
        contexte.join(valeurs, on="enrollment_id", how="left")
        .with_columns(pl.col("enrolled_at").dt.date())
        .join(build_lab_history(retenus), on="enrollment_id", how="left")
    )
    return df_pivot.join(org_units, on="enrollment_org_unit", how="left")


def build_lab_history(df_long: pl.DataFrame) -> pl.DataFrame:
    """Résume l'historique des tests de laboratoire, une ligne par enrôlement.

    Le pivot ne retient que la DERNIÈRE valeur du résultat labo : un cas positif
    puis retesté négatif ou invalide y apparaît non positif, alors qu'il reste un
    échantillon positif dans le décompte du laboratoire. Cette agrégation conserve
    donc l'historique complet, sans changer le grain du pivot.

    Args:
        df_long: Extraction au format long, déjà filtrée sur la fenêtre.

    Returns:
        Une ligne par enrôlement testé : ``lab_confirme`` (au moins un positif),
        ``date_confirmation`` (premier positif), ``n_tests_labo`` et
        ``flag_pos_puis_neg`` (positif dont le dernier résultat ne l'est plus).
    """
    est_pos = pl.col("value") == "POS"
    return (
        df_long.filter(pl.col("data_element_id") == DE_RESULTAT_LABO)
        .sort(["enrollment_id", "created_at"])
        .group_by("enrollment_id", maintain_order=True)
        .agg(
            est_pos.any().alias("lab_confirme"),
            pl.col("created_at").filter(est_pos).min().alias("date_confirmation"),
            pl.len().alias("n_tests_labo"),
            (est_pos.any() & (pl.col("value").last() != "POS")).alias("flag_pos_puis_neg"),
        )
    )


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


def compute_indicators_mve_notifications(df_pivot: pl.DataFrame) -> pl.DataFrame:
    """Ajoute les colonnes indicateurs (0/1 par enrollment) au pivot.

    Chaque indicateur restitue ce que porte la source, sans masquage : un stage
    peu rempli produit des compteurs faibles, pas des « ND ». Le ND est réservé
    aux champs réellement absents de l'extraction, décidé côté ``reporting``.
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
    c_deces_s6 = _col(df_pivot, DE_DATE_DECES_S6)
    c_deces_pci = _col(df_pivot, DE_PCI_DATE_DECES)
    c_nature = _col(df_pivot, DE_NATURE_ALERTE)
    c_date_prel = _col(df_pivot, DE_DATE_PRELEVEMENT)
    c_lab_confirme = (
        pl.col("lab_confirme")
        if "lab_confirme" in df_pivot.columns
        else pl.lit(None, dtype=pl.Boolean)
    )

    est_decede = (
        (c_nature == NATURE_ALERTE_DECES)
        | (c_final == "D")
        | (c_prel == "D")
        | c_deces_s6.is_not_null()
        | c_deces_pci.is_not_null()
    )

    est_vivant = ~est_decede.fill_null(value=False)
    est_gueri = (c_pec_issue == ISSUE_GUERI).fill_null(value=False)
    est_oriente_isolement = c_devenir.is_in(CODES_ISOLEMENT)

    echantillon_collecte = c_date_prel.is_not_null() | (
        c_type_prel.is_not_null() & c_recep.is_not_null()
    )
    # Positivité : l'historique complet des tests, pas le dernier résultat connu.
    echantillon_positif = c_lab_confirme.fill_null(value=False) | (c_labo == "POS")
    est_admis_pec = c_pec_adm.is_not_null()

    return df_pivot.with_columns(
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
        pl.when(est_gueri).then(1).otherwise(0).alias("n_gueris"),
        pl.when(c_pec_issue == ISSUE_DECEDE).then(1).otherwise(0).alias("n_deces_pec"),
        pl.when(c_pec_issue == ISSUE_EVADE).then(1).otherwise(0).alias("n_evades"),
        pl.when(c_pec_issue == ISSUE_TRANSFERE).then(1).otherwise(0).alias("n_transferes"),
        pl.when(c_pec_issue == ISSUE_NON_CAS).then(1).otherwise(0).alias("n_pec_non_cas"),
        pl.when(c_pec_issue == ISSUE_RETOUR_MAISON)
        .then(1)
        .otherwise(0)
        .alias("n_pec_retour_maison"),
        pl.when(c_pec_issue == ISSUE_ABANDON).then(1).otherwise(0).alias("n_pec_abandon"),
        pl.when(c_pec_issue == ISSUE_REFERE).then(1).otherwise(0).alias("n_pec_refere"),
        # Admissions et sorties (alimentent le mouvement des malades, Tableau VII).
        pl.when(est_admis_pec).then(1).otherwise(0).alias("n_pec_admis"),
        pl.when(c_pec_issue.is_in(ISSUES_SORTIE)).then(1).otherwise(0).alias("n_pec_sorties"),
        # STOCK isolés fiable via PEC : admis (date d'admission) sans sortie
        # documentée — ni issue connue, ni date de sortie.
        pl.when(
            est_admis_pec
            & ~c_pec_issue.is_in(ISSUES_SORTIE).fill_null(value=False)
            & c_pec_sortie.is_null()
        )
        .then(1)
        .otherwise(0)
        .alias("n_isole_stock_pec"),
        pl.when(
            est_admis_pec
            & ~c_pec_issue.is_in(ISSUES_SORTIE).fill_null(value=False)
            & c_pec_sortie.is_null()
        )
        .then(1)
        .otherwise(0)
        .alias("n_pec_encore_admis"),
        # ---- ISOLEMENT ----
        pl.when(est_oriente_isolement).then(1).otherwise(0).alias("n_orientes_isolement_cumul"),
        pl.when(est_oriente_isolement & c_sortie.is_null() & est_vivant)
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
        pl.when((c_class == "CC") & est_vivant & ~est_gueri & c_sortie.is_null())
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
        pl.when(echantillon_collecte & echantillon_positif)
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
        pl.when((c_class == "CC") & est_decede & echantillon_collecte & echantillon_positif)
        .then(1)
        .otherwise(0)
        .alias("n_positifs_suspect_decedes"),
    )


def diagnostic_completude(df_pivot: pl.DataFrame) -> pl.DataFrame:
    """Renvoie le taux de remplissage des DE clés.

    Outil de lecture, sans effet sur les indicateurs : il sert à interpréter un
    compteur faible (stage peu saisi) et à repérer un DE absent de l'extraction.
    """  # noqa: DOC201
    des = [
        DE_NATURE_ALERTE,
        DE_CONCLUSION_ALERTE,
        DE_CLASSIFICATION,
        DE_RESULTAT_LABO,
        DE_STATUT_PRELEVEMENT,
        DE_STATUT_FINAL,
        DE_DATE_DECES_S6,
        DE_PCI_DATE_DECES,
        DE_DATE_SORTIE_ISOLEMENT,
        DE_DEVENIR_SUSPECT,
        DE_TYPE_PRELEVEMENT,
        DE_DATE_PRELEVEMENT,
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
    cles_tri = [c for c in ("occurred_at", "created_at", "event_id") if c in df_mve.columns]
    attributs = {
        "num_epid": "MVE - Numéro Epid - Alerte MVE",
        "date_notification": "MPOX-N-Date et heure de notification de l'alerte",
        "date_debut_symptomes": "MVE - DDS (Date de début des symptômes)",
        "sexe": "MVE-N-Sexe",
        "age": "MVE - Age(ans)",
        "age_<1an": "MVE-N-Age < 1 an ?",
    }
    df_tei = (
        df_mve.group_by("tracked_entity_id")
        .agg(
            [
                pl.col(source).sort_by(cles_tri, nulls_last=True).drop_nulls().last().alias(cible)
                for cible, source in attributs.items()
            ]
        )
        .with_columns(
            pl.col("date_notification")
            .cast(pl.Datetime, strict=False)
            .dt.date()
            .alias("date_notification"),
            pl.col("date_debut_symptomes")
            .cast(pl.Date, strict=False)
            .alias("date_debut_symptomes"),
        )
    )

    df_ind = compute_indicators_mve_notifications(build_pivot(df_mve))

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
