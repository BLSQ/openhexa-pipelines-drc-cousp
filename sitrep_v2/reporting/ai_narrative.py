from __future__ import annotations

import re
from collections.abc import Callable

import config
from data.model import SitRepData
from reporting.highlights import _epicentre, build_conclusion, build_resume_points_cles
from utils.dates import fr_date
from utils.numbers import fmt_int as _fr_int
from utils.numbers import fmt_pct as _fr_pct
from utils.numbers import pct

_NUMBER_RE = re.compile(r"\d[\d\s ]*(?:,\d+)?\s?%?")

_SYSTEM_PROMPT = """Tu rédiges un paragraphe de rapport épidémiologique officiel \
(riposte MVE, RDC), registre formel, en français.

RÈGLES ABSOLUES :
- N'utilise QUE les chiffres, pourcentages, dates et noms de lieux fournis \
dans la liste de faits ci-dessous. N'en invente, n'en déduis et n'en arrondis \
aucun autre.
- Si une information n'est pas dans la liste, ne la mentionne pas.
- Mets en gras (**...**) les chiffres clés, comme dans un rapport officiel.
- Réponds avec exactement {n} paragraphes, un par ligne, sans numérotation, \
sans puce, sans texte d'introduction ni de conclusion ajouté."""


def _facts_resume_points_cles(data: SitRepData) -> list[str]:
    kpi = data.kpi
    epi = _epicentre(data)
    facts = [
        f"Date de rapportage : {fr_date(data.reporting_end)}",
        f"Nouveaux cas confirmés (jour) : {_fr_int(kpi['nouveaux_confirmes_periode'])}",
        f"Nouveaux cas confirmés (veille) : {_fr_int(kpi['nouveaux_confirmes_veille'])}",
        f"Nouvelles zones de santé touchées (jour) : {_fr_int(len(data.nouvelles_zones))}",
        f"Cumul cas confirmés : {_fr_int(kpi['cumul_confirmes'])}",
        f"Cumul décès confirmés : {_fr_int(kpi['cumul_deces'])}",
        f"Létalité cumulée : {_fr_pct(kpi['letalite'])}",
        f"Guéris (jour) : {_fr_int(kpi['gueris_periode'])}",
        f"Provinces touchées : {_fr_int(len(data.provinces_touchees))}",
    ]
    dc_jour, dc_veille = kpi["deces_communautaires_periode"], kpi["deces_communautaires_veille"]
    if isinstance(dc_jour, int) and isinstance(dc_veille, int):
        facts.append(f"Décès communautaires (jour) : {_fr_int(dc_jour)} (veille : {_fr_int(dc_veille)})")
    di_jour, di_veille = kpi["deces_intra_cte_periode"], kpi["deces_intra_cte_veille"]
    if isinstance(di_jour, int) and isinstance(di_veille, int):
        facts.append(f"Décès intra-CTE (jour) : {_fr_int(di_jour)} (veille : {_fr_int(di_veille)})")
        facts.append(f"Décès total confirmés (jour) : {_fr_int(kpi['deces_total_periode'])}")
    if epi:
        pct_jour = pct(epi["nouveaux"], kpi["nouveaux_confirmes_periode"]) if kpi["nouveaux_confirmes_periode"] else "0,0%"
        facts.append(
            f"Province épicentre : {epi['province']} "
            f"({pct(epi['confirmes'], kpi['cumul_confirmes'])} des cas cumulés, {pct_jour} des nouveaux cas)"
        )
    par_prov = sorted((r for r in data.tableau1 if r["nouveaux"]), key=lambda r: r["nouveaux"], reverse=True)
    if par_prov:
        detail = ", ".join(f"{r['province']} : {_fr_int(r['nouveaux'])}" for r in par_prov)
        facts.append(f"Nouveaux cas par province (jour) : {detail}")
    n_zs = sum(int(d.get("touchees", 0)) for d in data.zones_atteintes.values())
    facts.append(
        f"Zones de santé touchées (cumul) : {_fr_int(n_zs)} sur {_fr_int(config.TOTAL_ZONES_SANTE)} "
        f"({pct(n_zs, config.TOTAL_ZONES_SANTE)})"
    )
    par_zs = sorted(
        ((p, d.get("touchees", 0), d.get("total")) for p, d in data.zones_atteintes.items() if d.get("touchees")),
        key=lambda t: t[1],
        reverse=True,
    )
    if par_zs:
        detail = ", ".join(f"{p} : {_fr_int(k)}/{_fr_int(t)}" for p, k, t in par_zs)
        facts.append(f"Zones de santé touchées par province : {detail}")
    return facts


def _facts_conclusion(data: SitRepData) -> list[str]:
    kpi = data.kpi
    epi = _epicentre(data)
    facts = [
        f"Nouveaux cas confirmés (jour) : {_fr_int(kpi['nouveaux_confirmes_periode'])}",
        f"Décès total confirmés (jour) : {_fr_int(kpi['deces_total_periode'])}",
        f"Cumul cas confirmés : {_fr_int(kpi['cumul_confirmes'])}",
        f"Cumul décès confirmés : {_fr_int(kpi['cumul_deces'])}",
    ]
    dc, di = kpi["deces_communautaires_periode"], kpi["deces_intra_cte_periode"]
    if isinstance(dc, int) and isinstance(di, int):
        facts.append(f"Décès communautaires (jour) : {_fr_int(dc)}")
        facts.append(f"Décès intra-CTE (jour) : {_fr_int(di)}")
    pct_susp = data.surveillance_stats["pct_suspects_investigues"]
    if isinstance(pct_susp, (int, float)):
        facts.append(f"Cas suspects investigués : {_fr_pct(pct_susp)}")
    if epi:
        n_jour = kpi["nouveaux_confirmes_periode"]
        pct_jour = pct(epi["nouveaux"], n_jour) if n_jour else "0,0%"
        facts.append(
            f"Province épicentre : {epi['province']} "
            f"({pct(epi['confirmes'], kpi['cumul_confirmes'])} des cas cumulés, {pct_jour} des nouveaux cas, "
            f"{_fr_int(epi['zones_touchees'])}/{_fr_int(epi['zones_total'])} zones de santé touchées)"
        )
    return facts


def _call_claude(client, facts: list[str], n_paragraphs: int) -> list[str]:
    """Appelle le modèle et découpe la réponse en paragraphes.

    Returns:
        list[str]: Un élément de liste par paragraphe généré.
    """
    message = client.messages.create(
        model=config.AI_NARRATIVE_MODEL,
        max_tokens=1024,
        system=_SYSTEM_PROMPT.format(n=n_paragraphs),
        messages=[{"role": "user", "content": "Faits :\n" + "\n".join(f"- {f}" for f in facts)}],
    )
    text = message.content[0].text
    return [p.strip() for p in text.split("\n") if p.strip()]


def _normalize_number(token: str) -> str:
    """Ignore espaces et signe % : seule la valeur numérique compte pour comparer.

    Sans ceci, une reformulation légitime du modèle (``13,0%`` au lieu de
    ``13,0 %``, ou sans le ``%`` final) serait à tort traitée comme un
    chiffre différent du fait fourni.

    Returns:
        str: Le token sans espaces ni ``%``.
    """
    return re.sub(r"[\s%]", "", token)


def _validate(paragraphs: list[str], facts: list[str], provinces_touchees: list[str]) -> bool:
    """Rejette tout texte contenant un chiffre ou un nom de province non fourni.

    Returns:
        bool: ``True`` si chaque chiffre du texte correspond (une fois
        normalisé, cf. ``_normalize_number``) à un fait fourni, et si aucune
        province hors ``provinces_touchees`` n'est mentionnée.
    """
    allowed_numbers = set()
    for f in facts:
        allowed_numbers.update(_normalize_number(m.group()) for m in _NUMBER_RE.finditer(f))
    text = " ".join(paragraphs)
    for m in _NUMBER_RE.finditer(text):
        if _normalize_number(m.group()) not in allowed_numbers:
            return False
    mentioned = {p for p in config.PROVINCE_TOTAL_ZONES if p in text}
    return mentioned <= set(provinces_touchees)


def _generate_or_fallback(
    facts: list[str],
    fallback: list[str],
    provinces_touchees: list[str],
    client,
    logger: Callable[[str], None],
    label: str,
) -> list[str]:
    try:
        paragraphs = _call_claude(client, facts, n_paragraphs=len(fallback))
    except Exception as e:  # noqa: BLE001 — tout échec d'appel doit retomber sur le texte déterministe
        logger(f"AVERTISSEMENT : appel IA pour {label} échoué ({e}) — repli sur le texte déterministe.")
        return fallback
    if not _validate(paragraphs, facts, provinces_touchees):
        logger(
            f"AVERTISSEMENT : texte IA pour {label} rejeté (chiffre ou lieu non reconnu) "
            "— repli sur le texte déterministe."
        )
        return fallback
    return paragraphs


def build_resume_points_cles_ai(data: SitRepData, client, logger: Callable[[str], None] = print) -> list[str]:
    """``[[RESUME_POINTS_CLES]]`` rédigé par l'IA à partir des faits déjà
    calculés par ``data/metrics.py`` — jamais recalculés ici. Repli
    automatique sur ``reporting.highlights.build_resume_points_cles`` si
    l'appel échoue ou si le texte généré contient un chiffre non reconnu.

    Returns:
        list[str]: Les paragraphes à insérer, un par élément de liste.
    """
    fallback = build_resume_points_cles(data)
    facts = _facts_resume_points_cles(data)
    return _generate_or_fallback(facts, fallback, data.provinces_touchees, client, logger, "[[RESUME_POINTS_CLES]]")


def build_conclusion_ai(
    data: SitRepData,
    recommandation: str | None,
    client,
    logger: Callable[[str], None] = print,
) -> list[str]:
    """``[[CONCLUSION]]`` : mêmes garanties que ``build_resume_points_cles_ai``
    pour les paragraphes calculés. ``recommandation`` (narrative.yaml) n'est
    jamais envoyée à l'IA — ajoutée telle quelle après coup.

    Returns:
        list[str]: Les paragraphes à insérer, un par élément de liste.
    """
    fallback = build_conclusion(data, recommandation=None)
    facts = _facts_conclusion(data)
    paragraphs = _generate_or_fallback(facts, fallback, data.provinces_touchees, client, logger, "[[CONCLUSION]]")
    if recommandation:
        paragraphs = [*paragraphs, recommandation]
    return paragraphs
