"""Formatage des dates et libellés de période en français."""

from __future__ import annotations

from datetime import date

MONTHS_FR = [
    "",
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
]


def fr_date(d: date) -> str:
    """Formate une date en français, ex. « 18 mai 2026 ».

    Returns:
        str: La date au format « jour mois année ».
    """
    return f"{d.day} {MONTHS_FR[d.month]} {d.year}"


def period_label(start: date, end: date) -> str:
    """Libellé de la fenêtre de rapportage, ex. « 17-18 mai 2026 ».

    Returns:
        str: Le libellé de la période, compacté si bornes proches.
    """
    if start == end:
        return fr_date(start)
    if (start.month, start.year) == (end.month, end.year):
        return f"{start.day}-{end.day} {MONTHS_FR[end.month]} {end.year}"
    if start.year == end.year:
        return f"{start.day} {MONTHS_FR[start.month]} - {end.day} {MONTHS_FR[end.month]} {end.year}"
    return (
        f"{start.day} {MONTHS_FR[start.month]} {start.year} - "
        f"{end.day} {MONTHS_FR[end.month]} {end.year}"
    )
