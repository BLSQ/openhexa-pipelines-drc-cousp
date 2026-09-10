"""Narratifs auto-calculés : FAITS_SAILLANTS, RESUME_POINTS_CLES, CONCLUSION.

Structure et phrasé calqués sur l'exemple programme (« Draft Final
SitRep_MVEBDB_108_30_08_2026 ») : ce sont 3 narratifs **distincts**
(portée/angle différents), pas une redondance —
FAITS_SAILLANTS = 1 paragraphe dense (situation du jour vs veille),
RESUME_POINTS_CLES = 5 paragraphes détaillés (dont répartition par province et
liste des ZS touchées par province), CONCLUSION = 3 paragraphes calculés +
1 paragraphe de recommandation manuel (narrative.yaml, non calculable).
"""

from __future__ import annotations

import config
from data.model import SitRepData
from utils.dates import fr_date
from utils.numbers import fmt_int as _fr_int
from utils.numbers import pct


def _province_article(p: str) -> str:
    """« de l'Ituri » / « du Nord-Kivu » / « de la Tshopo »… (accord préposition+article)."""
    return {
        "Ituri": "de l'Ituri",
        "Nord-Kivu": "du Nord-Kivu",
        "Sud-Kivu": "du Sud-Kivu",
        "Tshopo": "de la Tshopo",
        "Haut-Uélé": "du Haut-Uélé",
        "Bas-Uélé": "du Bas-Uélé",
    }.get(p, f"de {p}")


def _province_a_article(p: str) -> str:
    """« en Ituri » / « au Nord-Kivu » / « à la Tshopo »… (accord préposition+article)."""
    return {
        "Ituri": "en Ituri",
        "Nord-Kivu": "au Nord-Kivu",
        "Sud-Kivu": "au Sud-Kivu",
        "Tshopo": "à la Tshopo",
        "Haut-Uélé": "au Haut-Uélé",
        "Bas-Uélé": "au Bas-Uélé",
    }.get(p, f"à {p}")


def _province_subject(p: str) -> str:
    """« L'Ituri » / « Le Nord-Kivu » / « La Tshopo »… (article défini, sujet en tête de phrase).

    Utilisé partout où le nom de province est le sujet grammatical d'une
    phrase narrative (jamais dans les tableaux/légendes/panneaux de carte,
    où le nom nu reste correct).
    """
    return {
        "Ituri": "L'Ituri",
        "Nord-Kivu": "Le Nord-Kivu",
        "Sud-Kivu": "Le Sud-Kivu",
        "Tshopo": "La Tshopo",
        "Haut-Uélé": "Le Haut-Uélé",
        "Bas-Uélé": "Le Bas-Uélé",
    }.get(p, p)


def _variation(new: int, old: int) -> tuple[str, str]:
    """« baisse »/« hausse »/« stabilité » + « de 96 à 59 (-38,5 %) ».

    Returns:
        tuple[str, str]: ``(mot, fragment)`` — le mot qualifiant la
        variation, et le fragment « de X à Y (±Z %) » (sans delta si
        ``old`` est nul).
    """
    if old == new:
        mot = "stabilité"
    else:
        mot = "baisse" if new < old else "hausse"
    fragment = f"de {_fr_int(old)} à {_fr_int(new)}"
    if old:
        delta = (new - old) / old * 100
        signe = "+" if delta > 0 else ""
        fragment += f" ({signe}{delta:.1f}".replace(".", ",") + " %)"
    return mot, fragment


def _enumerate_fr(items: list[str]) -> str:
    """« a, b et c ».

    Returns:
        str: L'énumération française des éléments (« et » avant le dernier).
    """
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " et " + items[-1]


def _fr_pct(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}".replace(".", ",") + " %"


def _delta_phrase(new: int, old: int) -> str:
    """« diminué de 96 à 59 (-38,5 %) » / « augmenté de 8 à 20 (+150,0 %) ».

    Toujours utilisé après un verbe auxiliaire (« ont »/« a »), d'où le cas
    stable en « été stables à N » plutôt que « stable à N » (accord verbal).

    Returns:
        str: La phrase de variation, ou « été stables à N » si ``old == new``.
    """
    if old == new:
        return f"été stables à {_fr_int(new)}"
    sens = "diminué" if new < old else "augmenté"
    delta_pct = (new - old) / old * 100 if old else None
    if delta_pct is None:
        return f"{sens} de {_fr_int(old)} à {_fr_int(new)}"
    signe = "+" if delta_pct > 0 else ""
    return f"{sens} de {_fr_int(old)} à {_fr_int(new)} ({signe}{delta_pct:.1f}".replace(".", ",") + " %)"


def _epicentre(data: SitRepData) -> dict | None:
    """Province concentrant le plus de cas confirmés cumulés (« épicentre »).

    Returns:
        dict | None: La ligne de ``data.tableau1`` de cette province, ou
        ``None`` si aucune province touchée.
    """
    if not data.tableau1:
        return None
    return max(data.tableau1, key=lambda r: r["confirmes"])


def build_faits_saillants(data: SitRepData) -> list[str]:
    """``[[FAITS_SAILLANTS]]`` : 1 paragraphe dense, situation du jour vs veille.

    Returns:
        list[str]: Un seul paragraphe (liste à 1 élément, cf. B.6 —
        ``fill_shape_lines`` accepte aussi plusieurs éléments si besoin).
    """
    kpi = data.kpi
    n_jour = int(kpi["nouveaux_confirmes_periode"])
    n_veille = int(kpi["nouveaux_confirmes_veille"])
    phrase = f"Au cours des dernières 24 heures, les nouveaux cas confirmés ont {_delta_phrase(n_jour, n_veille)}"

    dc_jour, dc_veille = kpi["deces_communautaires_periode"], kpi["deces_communautaires_veille"]
    if isinstance(dc_jour, int) and isinstance(dc_veille, int):
        phrase += f" et les décès communautaires de {_delta_phrase(dc_jour, dc_veille)}"
    phrase += " par rapport à la veille."

    n_nouvelles = len(data.nouvelles_zones)
    phrase += (
        " Aucune nouvelle zone de santé n'a été touchée."
        if not n_nouvelles
        else f" {_fr_int(n_nouvelles)} nouvelle(s) zone(s) de santé {'a' if n_nouvelles == 1 else 'ont'} été touchée(s)."
    )

    di_jour, di_veille = kpi["deces_intra_cte_periode"], kpi["deces_intra_cte_veille"]
    if isinstance(di_jour, int) and isinstance(di_veille, int):
        alerte = " nécessitant une attention particulière" if di_jour > di_veille else ""
        phrase += (
            f" Toutefois, {_fr_int(di_jour)} décès intra-CTE ont été enregistrés contre "
            f"{_fr_int(di_veille)} la veille{alerte}."
        )

    return [phrase]


def build_resume_points_cles(data: SitRepData) -> list[str]:
    """``[[RESUME_POINTS_CLES]]`` : 5 paragraphes détaillés.

    Returns:
        list[str]: Les 5 paragraphes (certains omis si la donnée sous-jacente
        est indisponible, ex. décès communautaires tant que B.4.3 n'est pas
        levé côté compute_indicators_mve_tdb).
    """
    kpi = data.kpi
    rep_end = fr_date(data.reporting_end)
    lines: list[str] = []

    # 1) Situation du jour vs veille (national). Gras/espacement : cf.
    # ``utils/docx.py::fill_shape_lines`` (``**...**``, un paragraphe espacé
    # par élément de ``lines``) — spans calqués sur l'exemple programme.
    n_jour, n_veille = int(kpi["nouveaux_confirmes_periode"]), int(kpi["nouveaux_confirmes_veille"])
    bold1 = f"une baisse des nouveaux cas confirmés, passant de {_fr_int(n_veille)} à {_fr_int(n_jour)}"
    delta = (n_jour - n_veille) / n_veille * 100 if n_veille else None
    if delta is not None:
        bold1 += f" ({delta:+.1f}".replace(".", ",") + " %)"
    dc_jour, dc_veille = kpi["deces_communautaires_periode"], kpi["deces_communautaires_veille"]
    if isinstance(dc_jour, int) and isinstance(dc_veille, int):
        bold1 += f", ainsi que des décès communautaires, passés de {_fr_int(dc_veille)} à {_fr_int(dc_jour)}"
        d2 = (dc_jour - dc_veille) / dc_veille * 100 if dc_veille else None
        if d2 is not None:
            bold1 += f" ({d2:+.1f}".replace(".", ",") + " %)"
    bold1 += " par rapport à la veille"
    n_nouvelles = len(data.nouvelles_zones)
    bold2 = (
        "aucune nouvelle zone de santé n'a été touchée"
        if not n_nouvelles
        else f"{_fr_int(n_nouvelles)} nouvelle(s) zone(s) de santé {'a' if n_nouvelles == 1 else 'ont'} été touchée(s)"
    )
    p1 = f"Au {rep_end}, la situation épidémiologique montre **{bold1}**. Par ailleurs, **{bold2}** au cours des dernières 24 heures."
    lines.append(p1)

    # 2) Détail par province (jour) + guéris + décès intra-CTE vs veille.
    par_prov = sorted((r for r in data.tableau1 if r["nouveaux"]), key=lambda r: r["nouveaux"], reverse=True)
    if par_prov:
        detail = _enumerate_fr(
            [f"{_province_a_article(r['province'])} ({_fr_int(r['nouveaux'])})" for r in par_prov]
        )
        p2 = f"Les **{_fr_int(n_jour)} nouveaux cas confirmés** ont été notifiés {detail}."
        gueris = int(kpi["gueris_periode"])
        if gueris:
            patients = "patient a" if gueris == 1 else "patients ont"
            p2 += f" En outre, **{_fr_int(gueris)} {patients} été déclarés guéris**."
        di_jour, di_veille = kpi["deces_intra_cte_periode"], kpi["deces_intra_cte_veille"]
        deces_total = kpi["deces_total_periode"]
        if isinstance(di_jour, int) and isinstance(di_veille, int):
            p2 += (
                f" Toutefois, **{_fr_int(di_jour)} décès intra-CTE ont été rapportés contre "
                f"{_fr_int(di_veille)} la veille**, portant à **{_fr_int(deces_total)} le nombre total "
                "de décès confirmés du jour**."
            )
        lines.append(p2)

    # 3) Cumul national + épicentre.
    epi = _epicentre(data)
    p3 = (
        f"Au {rep_end}, le cumul national s'établit à **{_fr_int(kpi['cumul_confirmes'])} cas confirmés et "
        f"{_fr_int(kpi['cumul_deces'])} décès**, soit une létalité de **{_fr_pct(kpi['letalite'])}**."
    )
    if epi:
        pct_cumul = pct(epi["confirmes"], kpi["cumul_confirmes"])
        pct_jour = pct(epi["nouveaux"], n_jour) if n_jour else "0,0%"
        p3 += (
            f" {_province_subject(epi['province'])} demeure l'épicentre, concentrant "
            f"**{pct_cumul} des cas cumulés et {pct_jour} des nouveaux cas**."
        )
    lines.append(p3)

    # 4) Zones de santé touchées (national).
    n_zs = sum(int(d.get("touchees", 0)) for d in data.zones_atteintes.values())
    p4 = (
        f"Depuis le début de l'épidémie, **{_fr_int(n_zs)} des {_fr_int(config.TOTAL_ZONES_SANTE)} zones "
        f"de santé ({pct(n_zs, config.TOTAL_ZONES_SANTE)}) ont été touchées** dans les "
        f"{_fr_int(len(data.provinces_touchees))} provinces affectées, "
    )
    p4 += (
        "sans extension à une nouvelle zone de santé"
        if not n_nouvelles
        else f"avec {_fr_int(n_nouvelles)} nouvelle(s) zone(s) de santé"
    )
    p4 += " au cours des dernières 24 heures."
    lines.append(p4)

    # 5) Zones touchées par province, triées par nombre décroissant.
    par_zs = sorted(
        ((p, d.get("touchees", 0), d.get("total")) for p, d in data.zones_atteintes.items() if d.get("touchees")),
        key=lambda t: t[1],
        reverse=True,
    )
    if par_zs:
        tete, *reste = par_zs
        p5 = f"{_province_subject(tete[0])} compte {_fr_int(tete[1])} zones de santé touchées sur {_fr_int(tete[2])}"
        if reste:
            suivi = _enumerate_fr(
                [f"{_province_article(p)} ({_fr_int(k)}/{_fr_int(t)})" for p, k, t in reste]
            )
            p5 += f", suivi {suivi}"
        p5 += "."
        lines.append(p5)

    return lines


def build_conclusion(data: SitRepData, recommandation: str | None = None) -> list[str]:
    """``[[CONCLUSION]]`` : 3 paragraphes calculés + 1 recommandation manuelle.

    ``recommandation`` (narrative.yaml) n'est **pas** calculée : pure prose
    éditoriale, aucun indicateur n'y figure dans l'exemple programme.

    Returns:
        list[str]: Les 4 paragraphes (chacun son propre élément de liste, un
        paragraphe Word par élément — cf. ``fill_shape_lines``).
    """
    kpi = data.kpi
    lines: list[str] = []

    # 1) Progrès en surveillance (cas suspects investigués, cf. B.4.5). Gras/
    # espacement : cf. ``utils/docx.py::fill_shape_lines`` (``**...**``, un
    # paragraphe espacé par élément de ``lines``) — spans calqués sur
    # l'exemple programme.
    pct_susp = data.surveillance_stats["pct_suspects_investigues"]
    if isinstance(pct_susp, (int, float)):
        lines.append(
            f"**Des progrès sont enregistrés dans la surveillance épidémiologique**, "
            f"avec **{_fr_pct(pct_susp)} des cas suspects investigués**."
        )

    # 2) Transmission active (jour) + cumul national.
    n_jour = int(kpi["nouveaux_confirmes_periode"])
    deces_total = kpi["deces_total_periode"]
    p2 = (
        f"**La transmission demeure néanmoins active**, avec **{_fr_int(n_jour)} nouveaux cas confirmés et "
        f"{_fr_int(deces_total)} décès au cours des dernières 24 heures**"
    )
    dc, di = kpi["deces_communautaires_periode"], kpi["deces_intra_cte_periode"]
    if isinstance(dc, int) and isinstance(di, int):
        p2 += f", dont **{_fr_int(dc)} décès communautaires et {_fr_int(di)} décès intra-CTE**"
    p2 += (
        f", portant le cumul national à **{_fr_int(kpi['cumul_confirmes'])} cas confirmés et "
        f"{_fr_int(kpi['cumul_deces'])} décès**."
    )
    lines.append(p2)

    # 3) Épicentre.
    epi = _epicentre(data)
    if epi:
        pct_cumul = pct(epi["confirmes"], kpi["cumul_confirmes"])
        pct_jour = pct(epi["nouveaux"], n_jour) if n_jour else "0,0%"
        lines.append(
            f"**{_province_subject(epi['province'])} demeure l'épicentre de l'épidémie**, concentrant "
            f"**{pct_cumul} des cas confirmés cumulés et {pct_jour} des nouveaux cas** rapportés au cours "
            f"des dernières 24 heures, avec **{_fr_int(epi['zones_touchees'])} des "
            f"{_fr_int(epi['zones_total'])} Zones de Santé affectées**."
        )

    # 4) Recommandation (manuelle, narrative.yaml).
    if recommandation:
        lines.append(recommandation)

    return lines


def _zone_rows(data: SitRepData, province: str) -> list[dict]:
    """Lignes de zone de santé (non-province) du Tableau 2 pour ``province``."""
    return [r for r in data.tableau2 if not r["is_province"] and r["province"] == province]


def _province_summary_row(data: SitRepData, province: str) -> dict | None:
    """Ligne de résumé « province » du Tableau 2 (cumul + jour agrégés)."""
    return next((r for r in data.tableau2 if r["is_province"] and r["province"] == province), None)


def build_commentaire_tableau1(data: SitRepData) -> list[str]:
    """``[[COMMENTAIRE_TABLEAU_1]]`` : 4 puces (tendance nationale, épicentre,
    2ᵉ province, provinces sans nouveau cas + létalité nationale).

    ``data.tableau1`` est déjà trié par cas cumulés décroissants (épicentre en
    tête, cf. ``data/metrics.py``).

    Returns:
        list[str]: Les puces (jusqu'à 4).
    """
    kpi = data.kpi
    lines: list[str] = []

    n_jour, n_veille = int(kpi["nouveaux_confirmes_periode"]), int(kpi["nouveaux_confirmes_veille"])
    mot, frag = _variation(n_jour, n_veille)
    lines.append(f"**{mot.capitalize()} des nouveaux cas**, {frag} par rapport à la veille.")

    ranked = data.tableau1
    epi = ranked[0] if ranked else None
    second = ranked[1] if len(ranked) > 1 else None

    if epi:
        pct_epi = pct(epi["nouveaux"], n_jour) if n_jour else "0,0%"
        mot2, _ = _variation(epi["nouveaux"], epi["nouveaux_veille"])
        evolution = (
            f"avec une stabilité à {_fr_int(epi['nouveaux'])} cas"
            if mot2 == "stabilité"
            else f"{'malgré une' if mot2 == 'baisse' else 'avec une'} {mot2} "
            f"de {_fr_int(epi['nouveaux_veille'])} à {_fr_int(epi['nouveaux'])} cas"
        )
        lines.append(
            f"**{_province_subject(epi['province'])} demeure l'épicentre**, avec {pct_epi} des nouveaux cas, {evolution}."
        )

    if second:
        mot3, _ = _variation(second["nouveaux"], second["nouveaux_veille"])
        qualif = " importante" if mot3 != "stabilité" else ""
        lines.append(
            f"**{_province_subject(second['province'])} enregistre également une {mot3}{qualif}**, "
            f"de {_fr_int(second['nouveaux_veille'])} à {_fr_int(second['nouveaux'])} nouveaux cas, "
            f"mais conserve une létalité élevée ({_fr_pct(second['cfr'])})."
        )

    excluded = {r["province"] for r in (epi, second) if r}
    a_zero = [r["province"] for r in ranked if r["province"] not in excluded and r["nouveaux"] == 0]
    if a_zero:
        liste = _enumerate_fr([_province_a_article(p) for p in a_zero])
        lines.append(
            f"**Aucun nouveau cas n'a été rapporté {liste}** ; "
            f"la létalité nationale demeure élevée à {_fr_pct(kpi['letalite'])}."
        )

    return lines


def build_commentaire_tableau2(data: SitRepData) -> list[str]:
    """``[[COMMENTAIRE_TABLEAU_2]]`` : 3 puces classées par nouveaux cas (jour).

    Contrairement à ``build_commentaire_tableau1`` (classement sur le cumul),
    le classement ici porte sur les nouveaux cas du jour — il peut donc
    différer de l'épicentre cumulé.

    Returns:
        list[str]: Les puces (jusqu'à 3, selon le nombre de provinces avec
        de nouveaux cas).
    """
    n_jour = int(data.kpi["nouveaux_confirmes_periode"])
    ranked = sorted((r for r in data.tableau1 if r["nouveaux"] > 0), key=lambda r: r["nouveaux"], reverse=True)
    lines: list[str] = []
    if not ranked:
        return lines

    def _top_zones(province: str, limit: int) -> list[dict]:
        return sorted(
            (z for z in _zone_rows(data, province) if z["nouveaux"] > 0),
            key=lambda z: z["nouveaux"],
            reverse=True,
        )[:limit]

    p1 = ranked[0]
    top_zones = _top_zones(p1["province"], 2)
    if top_zones:
        pct1 = pct(p1["nouveaux"], n_jour) if n_jour else "0,0%"
        if len(top_zones) == 1:
            detail = f"principalement à {top_zones[0]['zone']} ({_fr_int(top_zones[0]['nouveaux'])} cas)"
        elif top_zones[0]["nouveaux"] == top_zones[1]["nouveaux"]:
            detail = (
                f"principalement à {top_zones[0]['zone']} et {top_zones[1]['zone']} "
                f"({_fr_int(top_zones[0]['nouveaux'])} cas chacune)"
            )
        else:
            detail = (
                f"principalement à {top_zones[0]['zone']} ({_fr_int(top_zones[0]['nouveaux'])} cas) "
                f"et {top_zones[1]['zone']} ({_fr_int(top_zones[1]['nouveaux'])} cas)"
            )
        lines.append(f"**{_province_subject(p1['province'])} concentre {pct1} des nouveaux cas**, {detail}.")

    if len(ranked) > 1:
        p2 = ranked[1]
        summary2 = _province_summary_row(data, p2["province"])
        deces2 = summary2["deces_total_jour"] if summary2 else 0
        lines.append(
            f"**{_province_subject(p2['province'])} enregistre {_fr_int(p2['nouveaux'])} nouveaux cas et "
            f"{_fr_int(deces2)} décès**."
        )

    for p in ranked[2:]:
        top = _top_zones(p["province"], 1)
        if not top:
            continue
        prefix = _province_a_article(p["province"])
        lines.append(
            f"**{prefix[:1].upper()}{prefix[1:]}, {top[0]['zone']} concentre "
            f"{_fr_int(top[0]['nouveaux'])} des {_fr_int(p['nouveaux'])} nouveaux cas** de la province."
        )

    return lines


def build_actions_surveillance(data: SitRepData) -> list[str]:
    """``[[ACTIONS_SURVEILLANCE]]`` : bilan des alertes/suspects (cumul, national).

    Returns:
        list[str]: 1 puce (2 phrases), vide si aucune alerte enregistrée.
    """
    s = data.surveillance_stats
    if not s["n_alertes"]:
        return []
    n_total_provinces = len(data.provinces_touchees)
    phrase = (
        f"Au total, {_fr_int(s['n_alertes'])} alertes ont été enregistrées en date du "
        f"{fr_date(data.reporting_end)}, dans {s['n_provinces_alertes']} des {n_total_provinces} "
        f"provinces touchées, {_fr_int(s['n_alertes_validees'])} ({_fr_pct(s['pct_alertes_validees'])}) "
        "ont été vérifiées."
    )
    if s["pct_suspects_investigues"] == 100.0:
        phrase += f" Les {_fr_int(s['n_suspects_valides'])} cas suspects validés ont tous été investigués (100 %)."
    elif isinstance(s["pct_suspects_investigues"], (int, float)):
        phrase += (
            f" Les {_fr_int(s['n_suspects_valides'])} cas suspects validés ont été investigués "
            f"à {_fr_pct(s['pct_suspects_investigues'])}."
        )
    return [phrase]


def build_actions_laboratoire(data: SitRepData) -> list[str]:
    """``[[ACTIONS_LABORATOIRE]]`` : résultats positifs par province (jour).

    Returns:
        list[str]: 1 puce par province (avec analyses > 0 sur la période),
        suivie d'une puce listant les provinces touchées sans analyse sur
        la période, s'il y en a.
    """
    bullets = [
        f"**{r['province']} : {_fr_int(r['positifs'])} nouveaux résultats positifs** "
        f"({_fr_int(r['vivants'])} vivants et {_fr_int(r['deces'])} décès) sur {_fr_int(r['analyses'])} "
        f"nouveaux échantillons reçus et analysés (**positivité : {_fr_pct(r['positivite'])}**)."
        for r in data.labo_par_province
    ]
    if data.provinces_sans_labo:
        liste = _enumerate_fr([_province_a_article(p) for p in data.provinces_sans_labo])
        bullets.append(f"Aucune analyse de laboratoire n'a été réalisée sur la période {liste}.")
    return bullets
