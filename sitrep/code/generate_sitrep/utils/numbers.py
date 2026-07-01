"""Formatage de nombres en français (cardinaux en lettres, pourcentages)."""

from __future__ import annotations

# Cardinaux français 0-16 puis composition jusqu'à 99.
_UNITS = [
    "zéro",
    "un",
    "deux",
    "trois",
    "quatre",
    "cinq",
    "six",
    "sept",
    "huit",
    "neuf",
    "dix",
    "onze",
    "douze",
    "treize",
    "quatorze",
    "quinze",
    "seize",
]
_TENS = {20: "vingt", 30: "trente", 40: "quarante", 50: "cinquante", 60: "soixante"}


def spell_fr(n: int) -> str:
    """Écrit un entier 0-99 en toutes lettres (français). Sinon le chiffre.

    Returns:
        str: L'entier en lettres, ou sa représentation décimale hors plage.
    """
    if n < 0 or n > 99:
        return str(n)
    if n <= 16:
        return _UNITS[n]
    if n < 20:
        return f"dix-{_UNITS[n - 10]}"
    if n < 70:
        ten, unit = (n // 10) * 10, n % 10
        if unit == 0:
            return _TENS[ten]
        if unit == 1:
            return f"{_TENS[ten]} et un"
        return f"{_TENS[ten]}-{_UNITS[unit]}"
    if n < 80:  # 70-79 : soixante + 10..19
        rem = n - 60
        return "soixante et onze" if rem == 11 else f"soixante-{spell_fr(rem)}"
    # 80-99 : quatre-vingt(s) + 0..19
    rem = n - 80
    return "quatre-vingts" if rem == 0 else f"quatre-vingt-{spell_fr(rem)}"


def fmt_pct(value: float, decimals: int = 1) -> str:
    """Formate un pourcentage déjà calculé : 17.9 -> « 17,9% ».

    Returns:
        str: Le pourcentage formaté à la française (virgule décimale, suffixe %).
    """
    return f"{value:.{decimals}f}".replace(".", ",") + "%"


def pct(part: float, total: float, decimals: int = 1) -> str:
    """Calcule et formate part/total en pourcentage français (« X,Y% »).

    Returns:
        str: Le ratio formaté, ou « 0,0% » si ``total`` est nul.
    """
    return fmt_pct(part / total * 100, decimals) if total else "0,0%"
