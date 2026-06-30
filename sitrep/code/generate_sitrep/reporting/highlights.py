from __future__ import annotations

from data.model import SitRepData
from utils.numbers import pct, spell_fr


def _cap(s: str) -> str:
    return s[:1].upper() + s[1:] if s else s


def _num(n: int) -> str:
    """« douze (12) » (minuscule ; capitaliser au besoin via _cap).

    Returns:
        str: L'entier en lettres suivi de sa valeur entre parenthèses.
    """
    return f"{spell_fr(n)} ({n})"


def _province_article(p: str) -> str:
    return {
        "Ituri": "de l'Ituri",
        "Nord-Kivu": "du Nord-Kivu",
        "Sud-Kivu": "du Sud-Kivu",
        "Kinshasa": "de Kinshasa",
    }.get(p, f"de {p}")


def _enumerate_fr(items: list[str]) -> str:
    """« a, b et c ».

    Returns:
        str: L'énumération française des éléments (« et » avant le dernier).
    """
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " et " + items[-1]


def build_highlights(data: SitRepData) -> list[str]:
    """Construit les faits saillants automatiques (liste de phrases).

    Returns:
        list[str]: Les phrases factuelles « à date » du rapport.
    """
    lines: list[str] = []
    single_day = data.reporting_start == data.reporting_end
    when = f"le {data.reporting_label}" if single_day else f"les {data.reporting_label}"

    # 1) Nouveaux cas confirmés de la période + répartition par zone de santé.
    n = int(data.kpi["nouveaux_confirmes_periode"])  # type: ignore
    if n and data.nouveaux_par_zone:
        cas = "nouveau cas confirmé notifié" if n == 1 else "nouveaux cas confirmés notifiés"
        repartition = _enumerate_fr([f"{z['zone']} ({z['n']})" for z in data.nouveaux_par_zone])
        lines.append(
            f"{_cap(_num(n))} {cas} {when}, répartis dans les zones de santé de {repartition}."
        )

    # 2) Zones nouvellement touchées, par province (« X sur N (p%) »).
    by_prov: dict[str, list[str]] = {}
    for z in data.nouvelles_zones:
        by_prov.setdefault(z["province"], []).append(z["zone"])
    for prov, prov_zones in by_prov.items():
        zones = sorted(prov_zones)
        atteint = data.zones_atteintes.get(prov, {})
        k, total = atteint.get("touchees"), atteint.get("total")
        if len(zones) == 1:
            phrase = f"La zone de santé de {zones[0]} est nouvellement touchée"
        else:
            phrase = f"Les zones de santé de {_enumerate_fr(zones)} sont nouvellement touchées"
        if k and total:
            phrase += (
                f", portant à {k} sur {total} ({pct(k, total)}) les zones "
                f"de santé atteintes dans la province {_province_article(prov)}"
            )
        lines.append(phrase + ".")

    # 3) Guéris sortis sur la période.
    g = int(data.kpi["gueris_periode"])  # type: ignore
    if g:
        patients = "patient guéri" if g == 1 else "patients guéris"
        lines.append(f"Sortie de {_num(g)} {patients}.")

    return lines
