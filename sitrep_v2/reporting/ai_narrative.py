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

_SCOPE_TERMS = ("national", "nationale", "nationaux", "nationales", "territoire national", "pays")

_SYSTEM_PROMPT = """Tu rédiges un paragraphe de rapport épidémiologique officiel \
(riposte MVE, RDC), registre formel, en français.

RÈGLES ABSOLUES :
- N'utilise QUE les chiffres, pourcentages, dates et noms de lieux fournis \
dans la liste de faits ci-dessous. N'en invente, n'en déduis et n'en arrondis \
aucun autre.
- Si une information n'est pas dans la liste, ne la mentionne pas.
- N'ajoute aucun qualificatif de portée géographique ou territoriale \
(« national », « pays », « territoire national », etc.) qui ne figure pas \
déjà tel quel dans les faits fournis. Par exemple, si un fait donne un \
nombre de zones de santé touchées parmi les provinces suivies, ne dis \
jamais que cela représente une part du « maillage sanitaire national » ou \
du « pays » — ce chiffre ne concerne que les provinces mentionnées dans le \
fait, pas l'ensemble du pays.
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
        f"Zones de santé touchées (cumul, dans les {_fr_int(len(config.PROVINCE_TOTAL_ZONES))} provinces suivies) : "
        f"{_fr_int(n_zs)} sur {_fr_int(config.TOTAL_ZONES_SANTE)} zones de santé que comptent ces provinces "
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


def _ungrounded_scope_terms(paragraphs: list[str], facts: list[str]) -> list[str]:
    """Termes de portée (``_SCOPE_TERMS``) présents dans le texte mais absents des faits.

    Un texte ne peut légitimement employer un terme de portée (échelle
    nationale, ex. « national », « pays ») que si un fait fourni l'emploie
    déjà lui-même — sinon c'est une généralisation ajoutée par le modèle,
    non un chiffre ou un lieu qu'une simple liste blanche suffirait à
    couvrir.

    Returns:
        list[str]: Les termes de ``_SCOPE_TERMS`` non ancrés dans les faits.
    """
    text = " ".join(paragraphs).lower()
    facts_text = " ".join(facts).lower()
    return [
        t
        for t in _SCOPE_TERMS
        if re.search(rf"\b{re.escape(t)}\b", text) and not re.search(rf"\b{re.escape(t)}\b", facts_text)
    ]


def _unrecognized(
    paragraphs: list[str], facts: list[str], provinces_touchees: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """Chiffres, provinces et termes de portée du texte absents des faits fournis.

    Returns:
        tuple[list[str], list[str], list[str]]: ``(chiffres_non_reconnus,
        provinces_non_reconnues, termes_de_portee_non_ancres)``, tous vides
        si le texte est entièrement cohérent avec les faits (chiffres
        comparés normalisés, cf. ``_normalize_number``).
    """
    allowed_numbers = set()
    for f in facts:
        allowed_numbers.update(_normalize_number(m.group()) for m in _NUMBER_RE.finditer(f))
    text = " ".join(paragraphs)
    bad_numbers = [
        m.group() for m in _NUMBER_RE.finditer(text) if _normalize_number(m.group()) not in allowed_numbers
    ]
    mentioned = {p for p in config.PROVINCE_TOTAL_ZONES if p in text}
    bad_provinces = sorted(mentioned - set(provinces_touchees))
    bad_scope = _ungrounded_scope_terms(paragraphs, facts)
    return bad_numbers, bad_provinces, bad_scope


def _validate(paragraphs: list[str], facts: list[str], provinces_touchees: list[str]) -> bool:
    """Rejette tout texte contenant un chiffre, un lieu ou un terme de portée non fourni.

    Returns:
        bool: ``True`` si chaque chiffre du texte correspond (une fois
        normalisé, cf. ``_normalize_number``) à un fait fourni, si aucune
        province hors ``provinces_touchees`` n'est mentionnée, et si aucun
        terme de ``_SCOPE_TERMS`` n'est employé sans être ancré dans les faits.
    """
    bad_numbers, bad_provinces, bad_scope = _unrecognized(paragraphs, facts, provinces_touchees)
    return not bad_numbers and not bad_provinces and not bad_scope


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
    bad_numbers, bad_provinces, bad_scope = _unrecognized(paragraphs, facts, provinces_touchees)
    if bad_numbers or bad_provinces or bad_scope:
        logger(
            f"AVERTISSEMENT : texte IA pour {label} rejeté — chiffre(s) non reconnu(s) : "
            f"{bad_numbers or 'aucun'} ; lieu(x) non reconnu(s) : {bad_provinces or 'aucun'} ; "
            f"terme(s) de portée non ancré(s) : {bad_scope or 'aucun'} "
            f"— texte généré : {paragraphs!r} — repli sur le texte déterministe."
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
