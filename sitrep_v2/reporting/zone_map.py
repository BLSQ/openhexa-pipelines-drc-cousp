from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # backend non interactif (pipeline headless)
import geopandas as gpd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import polars as pl
from adjustText import adjust_text
from data.model import SitRepData
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
from reporting import charts

# Paliers discrets (bornes hautes incluses) alignés sur le style « par classes »
# du SitRep officiel, du plus clair au plus foncé. Repris de v1 tel quel.
_CLASS_EDGES = (4, 9, 29, 49)  # > dernière borne = dernière classe
_CLASS_COLORS = ("#FCBBA1", "#FC9272", "#FB6A4A", "#DE2D26", "#A50F15")
_CLASS_LABELS = ("1 – 4", "5 – 9", "10 – 29", "30 – 49", "≥ 50")  # noqa: RUF001

_DPI = 200


def _class_index(value: int) -> int:
    """Indice de classe (0..4) d'un nombre de cas confirmés.

    Returns:
        int: L'indice de palier (0 = classe la plus basse).
    """
    for i, edge in enumerate(_CLASS_EDGES):
        if value <= edge:
            return i
    return len(_CLASS_EDGES)


def _zones_with_counts(frame: pl.DataFrame) -> gpd.GeoDataFrame:
    """GeoDataFrame national des ZS, colonne ``confirmes`` jointe depuis ``frame``.

    Returns:
        gpd.GeoDataFrame: Les zones de santé (échelle pays) avec leur compte
        de cas confirmés.
    """
    g = charts._load_zones()
    lookup = {
        (r["province"], r["zone_sante"]): r["confirmes"]
        for r in frame.group_by("province", "zone_sante")
        .agg(pl.col("n_confirmes").sum().alias("confirmes"))
        .to_dicts()
    }
    g["confirmes"] = [
        int(lookup.get((p, z), 0)) for p, z in zip(g["province"], g["zone"], strict=False)
    ]
    return g


def _draw_zone_panel(
    ax_map: Axes,
    ax_leg: Axes,
    g: gpd.GeoDataFrame,
    provinces_touchees: list[str],
    title: str,
) -> None:
    """Choroplèthe par paliers + marqueurs numérotés + légende latérale.

    Repris du style v1 (``sitrep/code/generate_sitrep/reporting/zone_map.py::
    zone_situation_map``) : cadrage serré sur les provinces touchées (pas
    l'échelle pays entière), marqueurs numérotés décongestionnés par
    ``adjustText`` (traits de rappel courts, la carte reste petite), et
    correspondance n°→nom/effectif dans un panneau latéral dédié — plus
    lisible que des noms affichés sur la carte ou hors carte quand les zones
    touchées sont nombreuses ou proches les unes des autres.
    """
    context = g[g["province"].isin(provinces_touchees)]
    context = context if not context.empty else g
    hot = context[context["confirmes"] > 0].copy()

    ax_map.set_title(title, fontsize=15, color=charts._RED, fontweight="bold")
    ax_leg.set_axis_off()
    if hot.empty:
        ax_map.set_axis_off()
        ax_map.text(
            0.5, 0.5, "Aucun cas sur la période", transform=ax_map.transAxes,
            ha="center", va="center", fontsize=11, color="#7B7D7D",
        )
        return

    # Classement décroissant : le n° 1 = la zone la plus touchée.
    hot = hot.sort_values("confirmes", ascending=False).reset_index(drop=True)
    hot["rang"] = range(1, len(hot) + 1)
    hot["cls"] = hot["confirmes"].map(_class_index)

    context.plot(ax=ax_map, color="#F4F4F4", edgecolor="#C8C8C8", linewidth=0.4)
    for cls in sorted(hot["cls"].unique()):
        sub = hot[hot["cls"] == cls]
        sub.plot(ax=ax_map, color=_CLASS_COLORS[cls], edgecolor="#7B241C", linewidth=0.6, hatch="///")

    # Délimitation des provinces (contours + nom), limitée aux provinces du
    # contexte, pour situer les ZS dans leur province d'appartenance.
    try:
        prov = charts._load_provinces()
    except Exception:
        prov = None
    if prov is not None:
        prov = prov[prov["province"].isin(context["province"].unique())]
        if not prov.empty:
            prov.boundary.plot(ax=ax_map, color="#34495E", linewidth=1.1, linestyle="-", zorder=3)
            for _, prow in prov.iterrows():
                c = prow.geometry.representative_point()
                ax_map.annotate(
                    str(prow["province"]).upper(),
                    (c.x, c.y),
                    fontsize=9.5,
                    fontweight="bold",
                    ha="center",
                    va="center",
                    color="#34495E",
                    zorder=4,
                    bbox={"boxstyle": "round,pad=0.2", "facecolor": "white", "edgecolor": "none", "alpha": 0.55},
                )

    # Cadrage sur l'étendue des provinces impactées (vue contextuelle, non
    # centrée sur les seules ZS avec cas) ; repli sur les ZS du contexte.
    frame = prov if (prov is not None and not prov.empty) else context
    minx, miny, maxx, maxy = frame.total_bounds
    mx, my = (maxx - minx) * 0.06 + 1e-6, (maxy - miny) * 0.06 + 1e-6
    ax_map.set_xlim(minx - mx, maxx + mx)
    ax_map.set_ylim(miny - my, maxy + my)
    ax_map.set_aspect("equal")

    # Marqueurs numérotés au point représentatif de chaque ZS touchée, puis
    # désempilement (adjustText) avec traits de rappel vers le polygone.
    pts = [row.geometry.representative_point() for _, row in hot.iterrows()]
    texts = [
        ax_map.text(
            p.x,
            p.y,
            str(rang),
            fontsize=9,
            fontweight="bold",
            ha="center",
            va="center",
            color="#1B2631",
            zorder=5,
            bbox={"boxstyle": "circle,pad=0.28", "facecolor": "white", "edgecolor": "#7B241C", "linewidth": 0.9},
        )
        for p, rang in zip(pts, hot["rang"], strict=False)
    ]
    adjust_text(
        texts,
        x=[p.x for p in pts],
        y=[p.y for p in pts],
        ax=ax_map,
        expand=(1.4, 1.6),
        arrowprops={"arrowstyle": "-", "color": "#7B241C", "lw": 0.5},
    )
    ax_map.set_axis_off()

    handles: list = [
        mpatches.Patch(facecolor=_CLASS_COLORS[i], edgecolor="#7B241C", label=lab)
        for i, lab in enumerate(_CLASS_LABELS)
    ]
    if prov is not None and not prov.empty:
        handles.append(Line2D([0], [0], color="#34495E", linewidth=1.1, label="Limite de province"))
    ax_map.legend(
        handles=handles,
        title="Cas confirmés (classes)",
        loc="lower left",
        fontsize=10,
        title_fontsize=10.5,
        framealpha=0.9,
    )

    # --- Légende latérale : n° → zone (cas), sur 2 colonnes -----------------
    # 2 colonnes (remplies colonne par colonne, pas en alternance) plutôt
    # qu'une seule : divise par ~2 le nombre d'entrées empilées verticalement,
    # donc une police plus grande à hauteur de ligne égale (cf. calcul
    # ``fontsize`` ci-dessous) — plus lisible qu'une longue colonne unique
    # quand il y a beaucoup de zones touchées.
    ax_leg.set_title("Zones de santé touchées", fontsize=13, color=charts._RED)
    n = len(hot)
    n_cols = 2 if n > 1 else 1
    per_col = -(-n // n_cols)  # ceil(n / n_cols)
    y0, dy = 0.96, min(0.052, 0.92 / max(per_col, 1))
    # Taille de police adaptée à la hauteur réellement disponible par entrée
    # (``dy`` en fraction d'axes → hauteur en points, indépendant du DPI) :
    # évite tout chevauchement bulle/texte quand il y a beaucoup de zones
    # touchées (``dy`` alors très petit) tout en gardant une police plus
    # grande quand la place ne manque pas.
    row_height_pt = ax_leg.get_position().height * ax_leg.figure.get_size_inches()[1] * 72 * dy
    fontsize = max(6.5, min(10.0, row_height_pt * 0.75))
    col_x = [(0.04, 0.15), (0.54, 0.65)]
    for i, row in hot.iterrows():
        col, i_in_col = divmod(i, per_col)
        num_x, text_x = col_x[col]
        y = y0 - i_in_col * dy
        ax_leg.annotate(
            str(row["rang"]),
            (num_x, y),
            xycoords="axes fraction",
            fontsize=fontsize,
            fontweight="bold",
            ha="center",
            va="center",
            color="#1B2631",
            bbox={"boxstyle": "circle,pad=0.22", "facecolor": _CLASS_COLORS[row["cls"]], "edgecolor": "#7B241C", "linewidth": 0.8},
        )
        ax_leg.text(
            text_x,
            y,
            f"{row['zone']}  ({row['confirmes']})",
            transform=ax_leg.transAxes,
            fontsize=fontsize,
            va="center",
            ha="left",
        )


def zone_situation_maps(data: SitRepData, out_dir: Path) -> Path | None:
    """2 cartes ZS (style v1, cadrées + numérotées) : cumul (période) et jour (24h).

    Empilées **verticalement** (1 carte + son panneau latéral par ligne)
    plutôt que côte à côte, pour un rendu plus grand/lisible — la taille de
    police est augmentée en conséquence (cf. ``_draw_zone_panel``).

    Returns:
        Path | None: Le chemin du PNG, ou ``None`` si les shapefiles sont
        indisponibles (carte omise).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "zone_situation_maps.png"
    assert data.raw is not None and data.raw_day is not None
    try:
        g_cum = _zones_with_counts(data.raw)
        g_jour = _zones_with_counts(data.raw_day)
    except Exception:
        return None

    fig, axes = plt.subplots(
        2, 2, figsize=(12.5, 13.0), gridspec_kw={"width_ratios": [3.0, 2.0]}
    )
    (ax_map_cum, ax_leg_cum), (ax_map_jour, ax_leg_jour) = axes
    _draw_zone_panel(
        ax_map_cum, ax_leg_cum, g_cum, data.provinces_touchees, "Cas confirmés cumulés par zone de santé"
    )
    _draw_zone_panel(
        ax_map_jour, ax_leg_jour, g_jour, data.provinces_touchees,
        "Nouveaux cas confirmés (24h) par zone de santé",
    )
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path
