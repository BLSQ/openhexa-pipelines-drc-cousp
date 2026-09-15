from __future__ import annotations

from datetime import date, timedelta

import config
import polars as pl
from data.model import SitRepData
from utils.dates import period_label


def _order_provinces(provinces: list[str]) -> list[str]:
    """Provinces épidémiques historiques en tête, le reste par ordre alphabétique.

    Returns:
        list[str]: Les provinces ordonnées (épidémiques en tête).
    """
    head = [p for p in config.EPIDEMIC_PROVINCES if p in provinces]
    tail = sorted(p for p in provinces if p not in config.EPIDEMIC_PROVINCES)
    return head + tail


def _cfr(deces: int, confirmes: int) -> float:
    return round(deces / confirmes * 100, 1) if confirmes else 0.0


def _s(frame: pl.DataFrame, col: str) -> int:
    return int(frame[col].sum()) if frame.height and col in frame.columns else 0


def compute(
    rapportage: pl.DataFrame,
    dds_agg: pl.DataFrame,
    reporting_end: date | None = None,
    period_days: int = config.REPORTING_PERIOD_DAYS,
    publication_date: date | None = None,
    sitrep_number: str = config.SITREP_NUMBER,
    scope_label: str | None = None,
) -> SitRepData:
    """Calcule l'ensemble des indicateurs du SitRep.

    Le rapport résume une **fenêtre de ``period_days`` jours** se terminant le
    ``reporting_end`` (sur ``date_rapportage`` de ``rapportage``).
    ``publication_date`` vaut par défaut ``reporting_end`` + 1 jour.

    Contrairement à v1 (``sitrep/code/generate_sitrep/data/metrics.py::compute``),
    les colonnes sommées sont **déjà agrégées** par
    ``compute_indicators_mve_tdb`` (pas de recalcul de drapeaux par cas).

    Returns:
        SitRepData: L'ensemble des indicateurs calculés pour le rapport.
    """
    if reporting_end is None:
        reporting_end = rapportage["date_rapportage"].max()  # type: ignore[assignment]
    assert reporting_end is not None, "Aucune date_rapportage dans l'extraction"
    reporting_start = reporting_end - timedelta(days=max(1, period_days) - 1)
    if publication_date is None:
        publication_date = reporting_end + timedelta(days=1)
    reporting_label = period_label(reporting_start, reporting_end)

    cum = rapportage.filter(pl.col("date_rapportage") <= reporting_end)
    day = rapportage.filter(
        (pl.col("date_rapportage") >= reporting_start)
        & (pl.col("date_rapportage") <= reporting_end)
    )
    # Période précédente (« veille »), de même longueur, immédiatement avant
    # `reporting_start`.
    veille_start = reporting_start - timedelta(days=max(1, period_days))
    veille_end = reporting_start - timedelta(days=1)
    veille = rapportage.filter(
        (pl.col("date_rapportage") >= veille_start) & (pl.col("date_rapportage") <= veille_end)
    )

    # --- Provinces / zones touchées (>=1 cas confirmé en cumul) -------------
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
    zones_atteintes = {
        p: {"touchees": len(zones_by_province.get(p, [])), "total": config.PROVINCE_TOTAL_ZONES.get(p)}
        for p in provinces_touchees
    }

    # --- Aires de santé touchées (national) ---------------------------------
    if "aire_sante" in cum.columns:
        aires_touched = (
            cum.group_by("aire_sante").agg(pl.col("n_confirmes").sum()).filter(pl.col("n_confirmes") > 0)
        )
        aires_atteintes = {"touchees": aires_touched.height, "total": config.TOTAL_AIRES_SANTE}
    else:
        aires_atteintes = {"touchees": config.ND, "total": config.TOTAL_AIRES_SANTE}

    # --- KPI ------------------------------------------------------------
    cum_conf = _s(cum, "n_confirmes")
    cum_dec = _s(cum, "n_deces_confirmes")
    cum_gueris = _s(cum, "n_gueri")
    kpi = {
        "cumul_confirmes": cum_conf,
        "cumul_deces": cum_dec,
        "cumul_gueris": cum_gueris,
        "letalite": _cfr(cum_dec, cum_conf),
        "nouveaux_confirmes_periode": _s(day, "n_confirmes"),
        "gueris_periode": _s(day, "n_gueri"),
        "deces_communautaires_periode": (
            _s(day, "n_deces_communautaires") if "n_deces_communautaires" in day.columns else config.ND
        ),
        "deces_intra_cte_periode": (
            _s(day, "n_deces_intra_cte") if "n_deces_intra_cte" in day.columns else config.ND
        ),
        "deces_total_periode": _s(day, "n_deces_confirmes"),
        "nouveaux_confirmes_veille": _s(veille, "n_confirmes"),
        "deces_communautaires_veille": (
            _s(veille, "n_deces_communautaires") if "n_deces_communautaires" in veille.columns else config.ND
        ),
        "deces_intra_cte_veille": (
            _s(veille, "n_deces_intra_cte") if "n_deces_intra_cte" in veille.columns else config.ND
        ),
        "deces_total_veille": _s(veille, "n_deces_confirmes"),
    }

    # --- Tableau 1 : par province (cumul + jour + veille + ZS touchées) -----
    by_prov_cum = cum.group_by("province").agg(
        pl.col("n_confirmes").sum().alias("confirmes"),
        pl.col("n_deces_confirmes").sum().alias("deces"),
    )
    by_prov_day = day.group_by("province").agg(pl.col("n_confirmes").sum().alias("nouveaux"))
    by_prov_veille = veille.group_by("province").agg(pl.col("n_confirmes").sum().alias("nouveaux"))
    prov_cum_map = {r["province"]: r for r in by_prov_cum.to_dicts()}
    prov_day_map = {r["province"]: int(r["nouveaux"]) for r in by_prov_day.to_dicts()}
    prov_veille_map = {r["province"]: int(r["nouveaux"]) for r in by_prov_veille.to_dicts()}

    tableau1 = []
    for p in provinces_touchees:
        r = prov_cum_map.get(p, {"confirmes": 0, "deces": 0})
        conf, dec = int(r["confirmes"]), int(r["deces"])
        za = zones_atteintes.get(p, {})
        tableau1.append(
            {
                "province": p,
                "confirmes": conf,
                "deces": dec,
                "cfr": _cfr(dec, conf),
                "nouveaux": prov_day_map.get(p, 0),
                "nouveaux_veille": prov_veille_map.get(p, 0),
                "zones_touchees": za.get("touchees", 0),
                "zones_total": za.get("total"),
            }
        )
    tableau1.sort(key=lambda r: r["confirmes"], reverse=True)
    tot_conf = sum(r["confirmes"] for r in tableau1)
    tot_dec = sum(r["deces"] for r in tableau1)
    tableau1_total = {
        "confirmes": tot_conf,
        "deces": tot_dec,
        "cfr": _cfr(tot_dec, tot_conf),
        "nouveaux": sum(r["nouveaux"] for r in tableau1),
        "zones_touchees": sum(r["zones_touchees"] for r in tableau1),
        "zones_total": sum(r["zones_total"] for r in tableau1 if r["zones_total"]),
    }

    # --- Tableau 2 : par province > zone de santé (cumul + jour) ------------
    z_cum = (
        cum.group_by("province", "zone_sante")
        .agg(
            pl.col("n_confirmes").sum().alias("confirmes"),
            pl.col("n_deces_confirmes").sum().alias("deces"),
        )
        .filter(pl.col("confirmes") > 0)
    )
    z_day_cols = ["n_confirmes", "n_deces_confirmes"]
    for extra in ("n_deces_communautaires", "n_deces_intra_cte"):
        if extra in day.columns:
            z_day_cols.append(extra)
    z_day = day.group_by("province", "zone_sante").agg([pl.col(c).sum().alias(c) for c in z_day_cols])
    z_day_map = {(r["province"], r["zone_sante"]): r for r in z_day.to_dicts()}

    def _zone_row(province: str, r: dict) -> dict:
        zday = z_day_map.get((province, r["zone_sante"]), {})
        return {
            "is_province": False,
            "province": province,
            "zone": r["zone_sante"],
            "confirmes": int(r["confirmes"]),
            "deces": int(r["deces"]),
            "cfr": _cfr(int(r["deces"]), int(r["confirmes"])),
            "nouveaux": int(zday.get("n_confirmes", 0)),
            "deces_communautaires": (
                int(zday["n_deces_communautaires"]) if "n_deces_communautaires" in zday else config.ND
            ),
            "deces_intra_cte": int(zday["n_deces_intra_cte"]) if "n_deces_intra_cte" in zday else config.ND,
            "deces_total_jour": int(zday.get("n_deces_confirmes", 0)),
        }

    # Une ligne "province" (résumé, mise en avant) suivie de ses ZS par ordre
    # alphabétique ; provinces triées par cas cumulés décroissants (B.3/render).
    tableau2 = []
    for prov_row in tableau1:
        p = prov_row["province"]
        zones = [_zone_row(p, r) for r in z_cum.filter(pl.col("province") == p).sort("zone_sante").to_dicts()]
        tableau2.append(
            {
                "is_province": True,
                "province": p,
                "zone": None,
                "confirmes": prov_row["confirmes"],
                "deces": prov_row["deces"],
                "cfr": prov_row["cfr"],
                "nouveaux": prov_row["nouveaux"],
                "deces_communautaires": sum(
                    z["deces_communautaires"] for z in zones if isinstance(z["deces_communautaires"], int)
                )
                if any(isinstance(z["deces_communautaires"], int) for z in zones)
                else config.ND,
                "deces_intra_cte": sum(z["deces_intra_cte"] for z in zones if isinstance(z["deces_intra_cte"], int))
                if any(isinstance(z["deces_intra_cte"], int) for z in zones)
                else config.ND,
                "deces_total_jour": sum(z["deces_total_jour"] for z in zones),
            }
        )
        tableau2.extend(zones)
    _zones_only = [r for r in tableau2 if not r["is_province"]]
    tableau2_total = {
        "confirmes": tot_conf,
        "deces": tot_dec,
        "cfr": _cfr(tot_dec, tot_conf),
        "nouveaux": sum(r["nouveaux"] for r in tableau1),
        "deces_communautaires": (
            sum(z["deces_communautaires"] for z in _zones_only if isinstance(z["deces_communautaires"], int))
            if any(isinstance(z["deces_communautaires"], int) for z in _zones_only)
            else config.ND
        ),
        "deces_intra_cte": (
            sum(z["deces_intra_cte"] for z in _zones_only if isinstance(z["deces_intra_cte"], int))
            if any(isinstance(z["deces_intra_cte"], int) for z in _zones_only)
            else config.ND
        ),
        "deces_total_jour": sum(r["deces_total_jour"] for r in _zones_only),
    }

    # --- Laboratoire par province (jour) — [[ACTIONS_LABORATOIRE]] ----------
    labo_par_province = []
    if "n_analyses" in day.columns:
        by_prov_labo = day.group_by("province").agg(
            pl.col("n_confirmes_vivants").sum().alias("vivants"),
            pl.col("n_confirmes_deces").sum().alias("deces"),
            pl.col("n_analyses").sum().alias("analyses"),
        )
        for r in by_prov_labo.sort("analyses", descending=True).to_dicts():
            analyses = int(r["analyses"])
            if not analyses:
                continue
            vivants, deces = int(r["vivants"]), int(r["deces"])
            labo_par_province.append(
                {
                    "province": r["province"],
                    "vivants": vivants,
                    "deces": deces,
                    "positifs": vivants + deces,
                    "analyses": analyses,
                    "positivite": round((vivants + deces) / analyses * 100, 1),
                }
            )
    provinces_sans_labo = [
        p for p in provinces_touchees if p not in {r["province"] for r in labo_par_province}
    ]

    # --- Surveillance (cumul) — [[ACTIONS_SURVEILLANCE]] --------------------
    # Restreint aux provinces touchées (dynamique, cas confirmés — même
    # liste que [[NB_PROVINCES_TOUCHEES]]/Tableau 1, pas la config figée) :
    # Rapportage couvre le tracker au niveau national, alors que ce bilan ne
    # porte que sur les provinces déjà reconnues touchées par cette épidémie.
    cum_epi = cum.filter(pl.col("province").is_in(provinces_touchees))
    n_alertes_cum = _s(cum_epi, "n_alertes")
    n_alertes_valides_cum = _s(cum_epi, "n_alertes_valides")
    n_suspects_cum = _s(cum_epi, "n_suspects")
    provinces_alertes = (
        cum_epi.group_by("province").agg(pl.col("n_alertes").sum().alias("n")).filter(pl.col("n") > 0).height
    )
    pct_valides = round(n_alertes_valides_cum / n_alertes_cum * 100, 1) if n_alertes_cum else config.ND
    surveillance_stats = {
        "n_alertes": n_alertes_cum,
        "n_provinces_alertes": provinces_alertes,
        "n_alertes_validees": n_alertes_valides_cum,
        "pct_alertes_validees": pct_valides,
        # `n_suspects` est déjà daté par sa propre investigation dans
        # Rapportage (cf. CLAUDE.md B.4.5) : les suspects validés non encore
        # investigués n'y sont structurellement pas comptés, d'où un ratio
        # investigués/validés toujours à 100 % par construction du schéma.
        "n_suspects_valides": n_suspects_cum,
        "n_suspects_investigues": n_suspects_cum,
        "pct_suspects_investigues": 100.0 if n_suspects_cum else config.ND,
    }

    # --- Faits saillants « à date » ------------------------------------------
    npz = (
        day.group_by("province", "zone_sante")
        .agg(pl.col("n_confirmes").sum().alias("n"))
        .filter(pl.col("n") > 0)
        .sort("n", descending=True)
    )
    nouveaux_par_zone = [
        {"province": r["province"], "zone": r["zone_sante"], "n": int(r["n"])} for r in npz.to_dicts()
    ]

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

    # --- Pyramide âge x sexe (cas confirmés, cumul + jour) ------------------
    def _pyramid(frame: pl.DataFrame) -> dict:
        ag = frame.group_by("sexe_norm", "tranche_age").agg(pl.col("n_confirmes").sum())
        ag_map = {(r["sexe_norm"], r["tranche_age"]): int(r["n_confirmes"]) for r in ag.to_dicts()}
        return {sex: [ag_map.get((sex, age), 0) for age in config.AGE_ORDER] for sex in config.SEX_ORDER}

    agesex_pyramid = _pyramid(cum)
    agesex_pyramid_jour = _pyramid(day)

    # --- Courbe épidémique (DDS_Agg, par date de début des symptômes) -------
    onset = "date_debut_symptomes"
    ec_cum = dds_agg.filter(
        pl.col(onset).is_not_null()
        & (pl.col(onset) <= reporting_end)
        & (pl.col(onset) >= config.DATE_PLAUSIBLE_MIN)
    )
    ec = (
        ec_cum.group_by(onset)
        .agg(
            pl.col("n_confirmes_vivants").sum().alias("vivants"),
            pl.col("n_confirmes_deces").sum().alias("deces"),
        )
        .sort(onset)
        .with_columns(pl.col(onset).cast(pl.Date))
    )
    epi_curve = [(r[onset], int(r["vivants"]), int(r["deces"])) for r in ec.to_dicts()]

    return SitRepData(
        reporting_start=reporting_start,
        reporting_end=reporting_end,
        reporting_label=reporting_label,
        publication_date=publication_date,
        sitrep_number=sitrep_number,
        provinces_touchees=provinces_touchees,
        zones_by_province=zones_by_province,
        zones_atteintes=zones_atteintes,
        aires_atteintes=aires_atteintes,
        kpi=kpi,
        tableau1=tableau1,
        tableau1_total=tableau1_total,
        tableau2=tableau2,
        tableau2_total=tableau2_total,
        labo_par_province=labo_par_province,
        provinces_sans_labo=provinces_sans_labo,
        surveillance_stats=surveillance_stats,
        nouveaux_par_zone=nouveaux_par_zone,
        nouvelles_zones=nouvelles_zones,
        agesex_pyramid=agesex_pyramid,
        agesex_pyramid_jour=agesex_pyramid_jour,
        epi_curve=epi_curve,
        scope_label=scope_label,
        raw=cum,
        raw_day=day,
        raw_dds=ec_cum,
    )
