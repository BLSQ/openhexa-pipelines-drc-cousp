from __future__ import annotations

from datetime import date, timedelta

import config
import polars as pl
from data.model import SitRepData
from utils.dates import period_label


def _order_provinces(provinces: list[str]) -> list[str]:
    """Ituri puis Nord-Kivu en tête, le reste par ordre alphabétique.

    Returns:
        list[str]: Les provinces ordonnées (épidémiques en tête).
    """
    head = [p for p in config.EPIDEMIC_PROVINCES if p in provinces]
    tail = sorted(p for p in provinces if p not in config.EPIDEMIC_PROVINCES)
    return head + tail


def _cfr(deces: int, confirmes: int) -> float:
    return round(deces / confirmes * 100, 1) if confirmes else 0.0


def compute(
    df: pl.DataFrame,
    reporting_end: date | None = None,
    period_days: int = 2,
    publication_date: date | None = None,
    sitrep_number: str = config.SITREP_NUMBER,
    scope_label: str | None = None,
) -> SitRepData:
    """Calcule l'ensemble des indicateurs du SitRep.

    Le rapport résume une **fenêtre de ``period_days`` jours** se terminant le
    ``reporting_end`` (sur ``date_rapportage``). ``publication_date`` vaut par
    défaut ``reporting_end`` + 1 jour.

    Returns:
        SitRepData: L'ensemble des indicateurs calculés pour le rapport.
    """
    if reporting_end is None:
        reporting_end = df["date_rapportage"].max()  # type: ignore[assignment]
    assert reporting_end is not None, "Aucune date_rapportage dans l'extraction"
    reporting_start = reporting_end - timedelta(days=max(1, period_days) - 1)
    if publication_date is None:
        publication_date = reporting_end + timedelta(days=1)
    reporting_label = period_label(reporting_start, reporting_end)

    cum = df.filter(pl.col("date_rapportage") <= reporting_end)
    day = df.filter(
        (pl.col("date_rapportage") >= reporting_start)
        & (pl.col("date_rapportage") <= reporting_end)
    )

    def s(frame: pl.DataFrame, col: str) -> int:
        return int(frame[col].sum()) if frame.height else 0

    # --- Provinces / zones touchées (>=1 cas confirmé en cumul) ----------
    touched = (
        cum.group_by("province", "zone_sante")
        .agg(pl.col("n_confirmes").sum())
        .filter(pl.col("n_confirmes") > 0)
    )
    provinces_touchees = _order_provinces(touched["province"].unique().to_list())
    zones_by_province = {
        p: sorted(touched.filter(pl.col("province") == p)["zone_sante"].to_list())
        for p in provinces_touchees
    }

    # --- KPI -------------------------------------------------------------
    cum_conf = s(cum, "n_confirmes")
    cum_dec = s(cum, "n_deces_confirmes")
    cum_suspects = s(cum, "n_suspects_en_cours")
    cum_gueris = s(cum, "n_gueris")
    cum_probable = s(cum, "n_probables")
    kpi = {
        "cumul_confirmes": cum_conf,
        "cumul_deces": cum_dec,
        "cumul_suspects": cum_suspects,
        "gueris": cum_gueris,
        "suspects_en_cours_investigation": s(day, "n_suspects_en_cours_investigation"),
        "confirmes_actifs": s(cum, "n_cas_actifs_stock"),
        "confirmes_isolement": s(cum, "n_confirme_isole"),
        "suspects_isolement": s(cum, "n_suspect_isole"),
        "nouveaux_confirmes_periode": s(day, "n_confirmes"),
        "nouvelles_alertes_periode": s(day, "n_alertes"),
        "gueris_periode": s(day, "n_gueris"),
        "cas_probable": cum_probable,
        "contacts": config.ND,
        "taux_suivi_contacts": config.ND,
        # Carte « Cas suspects du jour » (bandeau d'accueil) : suspects en cours
        # sur la période + décès parmi ces suspects. La variable des décès
        # n'existe pas encore dans l'extraction → ND tant qu'elle est absente
        # (un simple ajout de colonne suffira à l'activer ici).
        "suspects_jour": s(day, "n_suspects_en_cours"),
        "deces_suspects_jour": s(cum, "n_deces_suspect"),
    }

    # --- Tableau de synthèse par province (table statique du template) ---
    by_prov_cum = cum.group_by("province").agg(
        pl.col("n_confirmes").sum().alias("confirmes"),
        pl.col("n_deces_confirmes").sum().alias("deces"),
        pl.col("n_suspect_isole").sum().alias("isolement"),
        pl.col("n_cas_actifs_stock").sum().alias("actifs"),
    )
    by_prov_day = day.group_by("province").agg(
        pl.col("n_gueris").sum().alias("gueris"),
        pl.col("n_suspects_en_cours_investigation").sum().alias("suspects"),
    )
    prov_map = {r["province"]: r for r in by_prov_cum.to_dicts()}
    gueris_map = {r["province"]: int(r["gueris"]) for r in by_prov_day.to_dicts()}
    suspects_map = {r["province"]: int(r["suspects"]) for r in by_prov_day.to_dicts()}

    def _prov_row(
        province: str,
        conf: int,
        dec: int,
        suspects: int,
        isole: int,
        actifs: int,
        gueris_jour: int,
    ) -> dict[str, str | int]:
        return {
            "province": province,
            "confirmes": conf,
            "deces": dec,
            "suspects": suspects,  # config.ND,
            "suspects_isolement": isole,
            "actifs": actifs,
            "gueris": gueris_jour,  # guéris du jour (période)
        }

    province_summary = []
    for p in provinces_touchees:
        r = prov_map.get(p, {"confirmes": 0, "deces": 0, "isolement": 0, "actifs": 0})
        province_summary.append(
            _prov_row(
                p,
                int(r["confirmes"]),
                int(r["deces"]),
                suspects_map.get(p, 0),
                int(r["isolement"]),
                int(r["actifs"]),
                gueris_map.get(p, 0),
            )
        )
    province_summary.append(
        _prov_row(
            "Total",
            cum_conf,
            cum_dec,
            s(day, "n_suspects_en_cours_investigation"),
            s(cum, "n_suspect_isole"),
            s(cum, "n_cas_actifs_stock"),
            s(day, "n_gueris"),
        )
    )

    # --- Faits saillants « à date » --------------------------------------
    npz = (
        day.group_by("province", "zone_sante")
        .agg(pl.col("n_confirmes").sum().alias("n"))
        .filter(pl.col("n") > 0)
        .sort("n", descending=True)
    )
    nouveaux_par_zone = [
        {"province": r["province"], "zone": r["zone_sante"], "n": int(r["n"])}
        for r in npz.to_dicts()
    ]

    # Zones nouvellement touchées = 1er cas confirmé dans la fenêtre de rapportage.
    first_conf = (
        cum.filter(pl.col("n_confirmes") > 0)
        .group_by("province", "zone_sante")
        .agg(pl.col("date_rapportage").min().alias("first"))
    )
    nouvelles_zones = [
        {"province": r["province"], "zone": r["zone_sante"]}
        for r in first_conf.filter(pl.col("first") >= reporting_start).to_dicts()
    ]
    nouvelles_zones.sort(key=lambda d: (d["province"], d["zone"]))

    zones_atteintes = {
        p: {
            "touchees": len(zones_by_province.get(p, [])),
            "total": config.PROVINCE_TOTAL_ZONES.get(p),
        }
        for p in provinces_touchees
    }

    # --- Distribution spatiale par province (Tableau I) ------------------
    nouveaux_prov = {
        r["province"]: int(r["n"])
        for r in day.group_by("province").agg(pl.col("n_confirmes").sum().alias("n")).to_dicts()
    }
    distribution_spatiale = []
    for p in provinces_touchees:
        r = prov_map.get(p, {"confirmes": 0, "deces": 0})
        conf, dec = int(r["confirmes"]), int(r["deces"])
        za = zones_atteintes.get(p, {})
        distribution_spatiale.append(
            {
                "province": p,
                "confirmes": conf,
                "deces": dec,
                "cfr": _cfr(dec, conf),
                "zones_touchees": za.get("touchees", 0),
                "zones_total": za.get("total"),
                "nouveaux": nouveaux_prov.get(p, 0),
            }
        )
    tot_conf = sum(d["confirmes"] for d in distribution_spatiale)
    tot_dec = sum(d["deces"] for d in distribution_spatiale)
    distribution_spatiale.append(
        {
            "province": "Total",
            "confirmes": tot_conf,
            "deces": tot_dec,
            "cfr": _cfr(tot_dec, tot_conf),
            "zones_touchees": sum(d["zones_touchees"] for d in distribution_spatiale),
            "zones_total": sum(d["zones_total"] for d in distribution_spatiale if d["zones_total"]),
            "nouveaux": sum(d["nouveaux"] for d in distribution_spatiale),
        }
    )

    # --- Tableau II : par province > zone de santé (zones touchées) ------
    z = (
        cum.group_by("province", "zone_sante")
        .agg(
            pl.col("n_confirmes").sum().alias("confirmes"),
            pl.col("n_deces_confirmes").sum().alias("deces"),
            pl.col("n_cas_actifs_stock").sum().alias("actifs"),
            pl.col("n_gueris").sum().alias("gueris"),
        )
        .filter(pl.col("confirmes") > 0)
    )
    tableau1 = []
    for p in provinces_touchees:
        sub = z.filter(pl.col("province") == p).sort("zone_sante")
        for r in sub.to_dicts():
            tableau1.append(
                {
                    "province": p,
                    "zone": r["zone_sante"],
                    "confirmes": int(r["confirmes"]),
                    "deces": int(r["deces"]),
                    "actifs": int(r["actifs"]),
                    "gueris": int(r["gueris"]),
                }
            )
    tableau1_total = {
        "confirmes": sum(r["confirmes"] for r in tableau1),
        "deces": sum(r["deces"] for r in tableau1),
        "actifs": sum(r["actifs"] for r in tableau1),
        "gueris": sum(r["gueris"] for r in tableau1),
    }

    # --- Alertes de la période par zone (conservé ; non placé en v3) -----
    a = (
        day.group_by("zone_sante")
        .agg(
            pl.col("n_alertes").sum().alias("alertes"),
            pl.col("n_alertes_investiguees").sum().alias("investiguees"),
            pl.col("n_alertes_validees").sum().alias("validees"),
        )
        .filter(pl.col("alertes") > 0)
        .sort("alertes", descending=True)
    )
    tableau2 = [
        {
            "zone": r["zone_sante"],
            "alertes": int(r["alertes"]),
            "investiguees": int(r["investiguees"]),
            "validees": int(r["validees"]),
        }
        for r in a.to_dicts()
    ]
    tableau2_total = {
        "alertes": sum(r["alertes"] for r in tableau2),
        "investiguees": sum(r["investiguees"] for r in tableau2),
        "validees": sum(r["validees"] for r in tableau2),
    }

    # --- Tableau III : gestion des alertes (période, national) -----------
    surveillance_indics = {
        "alertes_remontees": s(day, "n_alertes"),
        "alertes_investiguees": s(day, "n_alertes_investiguees"),
        "alertes_validees": s(day, "n_alertes_validees"),
        "nouvelles_zones": len(nouvelles_zones),
    }

    # --- Tableau V : indicateurs laboratoire (période) -------------------
    collectes = s(day, "n_echantillons_collectes")
    en_cours = s(day, "n_echantillons_en_cours")
    labo_indics = {
        "collectes": collectes,
        "analyses": max(0, collectes - en_cours),
        "positifs": s(day, "n_echantillons_positifs"),
        "en_cours": en_cours,
        "positifs_suspect_decedes": s(day, "n_positifs_suspect_decedes"),
    }

    # --- Tableau VI : indicateurs de prise en charge ---------------------
    prise_en_charge_indics = {
        "suspects_isolement": s(cum, "n_suspect_isole"),
        "confirmes_isolement": s(cum, "n_confirme_isole"),
        "actifs": s(cum, "n_cas_actifs_stock"),
        "gueris_jour": s(day, "n_gueris"),
        "letalite": config.ND,
        "lits": config.ND,
    }

    # --- Pyramide âge x sexe (cas confirmés) -----------------------------
    ag = cum.group_by("sexe_norm", "tranche_age").agg(pl.col("n_confirmes").sum())
    ag_map = {(r["sexe_norm"], r["tranche_age"]): int(r["n_confirmes"]) for r in ag.to_dicts()}
    pyramid = {
        sex: [ag_map.get((sex, age), 0) for age in config.AGE_ORDER] for sex in config.SEX_ORDER
    }

    # --- Tableau croisé sexe x tranche d'âge -----------------------------
    crosstab: list[dict] = []
    for age in config.AGE_ORDER:
        row: dict[str, object] = {"tranche_age": age}
        for sex in config.SEX_ORDER:
            row[sex] = ag_map.get((sex, age), 0)
        row["Total"] = sum(int(row[s_]) for s_ in config.SEX_ORDER)  # type: ignore
        crosstab.append(row)
    total_row: dict[str, object] = {"tranche_age": "Total"}
    for sex in config.SEX_ORDER:
        total_row[sex] = sum(r[sex] for r in crosstab)
    total_row["Total"] = sum(int(r["Total"]) for r in crosstab)
    crosstab.append(total_row)

    # --- Courbe épidémique : confirmés par SEMAINE ÉPIDÉMIOLOGIQUE de début
    # des symptômes (axe borné à [DATE_PLAUSIBLE_MIN .. reporting_end]). Semaine
    # ISO (lundi). Vide si ``date_debut_symptomes`` n'est pas alimentée.
    lo = config.DATE_PLAUSIBLE_MIN
    onset = "date_debut_symptomes"
    if onset in cum.columns:
        ec = (
            cum.filter(
                (pl.col("n_confirmes") > 0)
                & pl.col(onset).is_not_null()
                & (pl.col(onset) >= lo)
                & (pl.col(onset) <= reporting_end)
            )
            .with_columns(pl.col(onset).dt.truncate("1w").alias("semaine"))
            .group_by("semaine")
            .agg(pl.col("n_confirmes").sum())
            .sort("semaine")
        )
        epi_curve = [(r["semaine"], int(r["n_confirmes"])) for r in ec.to_dicts()]
    else:
        epi_curve = []

    return SitRepData(
        reporting_start=reporting_start,
        reporting_end=reporting_end,
        reporting_label=reporting_label,
        publication_date=publication_date,
        sitrep_number=sitrep_number,
        provinces_touchees=provinces_touchees,
        zones_by_province=zones_by_province,
        kpi=kpi,
        province_summary=province_summary,
        nouveaux_par_zone=nouveaux_par_zone,
        nouvelles_zones=nouvelles_zones,
        zones_atteintes=zones_atteintes,
        distribution_spatiale=distribution_spatiale,
        tableau1=tableau1,
        tableau1_total=tableau1_total,
        tableau2=tableau2,
        tableau2_total=tableau2_total,
        agesex_pyramid=pyramid,
        agesex_crosstab=crosstab,
        epi_curve=epi_curve,
        surveillance_indics=surveillance_indics,
        labo_indics=labo_indics,
        prise_en_charge_indics=prise_en_charge_indics,
        scope_label=scope_label,
        raw=cum,
    )
