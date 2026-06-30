from __future__ import annotations

import re

import config
import polars as pl

_PREFIX_RE = r"^[a-z]{2}\s+"


def strip_prefix_expr(col: str) -> pl.Expr:
    """Retire un préfixe province à deux lettres (``nk ``, ``sk ``…).

    Returns:
        pl.Expr: L'expression Polars sur ``col`` sans le préfixe.
    """
    return pl.col(col).str.replace(_PREFIX_RE, "")


def canonical_province_expr(col: str) -> pl.Expr:
    """Nettoie + canonise une colonne province (``it Ituri Province`` -> ``Ituri``).

    Returns:
        pl.Expr: L'expression Polars renvoyant le nom de province canonique.
    """
    cleaned = (
        pl.col(col)
        .str.replace(_PREFIX_RE, "")
        .str.replace(config.PROVINCES_NAME_SUFFIX, "", literal=True)
        .str.strip_chars()
    )
    expr = cleaned
    for raw, canon in config.PROVINCE_CANONICAL.items():
        expr = (
            pl.when(cleaned.str.to_lowercase().str.replace("-", " ") == raw)
            .then(pl.lit(canon))
            .otherwise(expr)
        )
    return expr


def bare_name(name: str, suffix: str = "") -> str:
    """Retire le préfixe à deux lettres et un suffixe (« it Bunia Zone… »).

    Returns:
        str: Le nom nettoyé de son préfixe et de son suffixe éventuel.
    """
    out = re.sub(_PREFIX_RE, "", name)
    if suffix:
        out = out.replace(suffix, "")
    return out.strip()


def canonical_province_name(name: str, suffix: str = config.PROVINCES_NAME_SUFFIX) -> str:
    """« nk Nord Kivu Province » -> « Nord-Kivu ».

    Returns:
        str: Le nom de province canonique correspondant.
    """
    bare = bare_name(name, suffix)
    return config.PROVINCE_CANONICAL.get(bare.lower().replace("-", " "), bare)
