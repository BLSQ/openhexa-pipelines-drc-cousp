from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # backend non interactif (pipeline headless)
import geopandas as gpd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import polars as pl
from adjustText import adjust_text
from data.loader import load_raw
from data.metrics import compute
from data.model import SitRepData
from matplotlib.lines import Line2D
from reporting import charts

# Paliers discrets (bornes hautes incluses) alignés sur le style « par classes »
# du SitRep officiel, du plus clair au plus foncé.
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


def _hot_zones(data: SitRepData) -> gpd.GeoDataFrame:
    """GeoDataFrame des ZS, colonne ``confirmes`` jointe depuis ``data.raw``.

    Returns:
        gpd.GeoDataFrame: Les zones de santé avec leur compte de cas confirmés.
    """
    g = charts._load_zones()
    assert data.raw is not None
    lookup = {
        (r["province"], r["zone_sante"]): r["confirmes"]
        for r in data.raw.group_by("province", "zone_sante")
        .agg(pl.col("n_confirmes").sum().alias("confirmes"))
        .to_dicts()
    }
    g["confirmes"] = [
        int(lookup.get((p, z), 0)) for p, z in zip(g["province"], g["zone"], strict=False)
    ]
    return g


def province_situation_map(data: SitRepData, out_dir: Path) -> Path | None:
    """Carte nationale des cas confirmés par province (choroplèthe).

    Réutilise ``charts._draw_provinces`` (déjà lisible) dans une figure dédiée.

    Returns:
        Path | None: Le chemin du PNG, ou ``None`` si les shapefiles sont
        indisponibles (carte omise).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "province_situation_map.png"
    fig, ax = plt.subplots(figsize=(7.0, 6.0))
    try:
        charts._draw_provinces(ax, data)
    except Exception:
        plt.close(fig)
        return None
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def zone_situation_map(data: SitRepData, out_dir: Path) -> Path | None:
    """Carte ZS lisible : choroplèthe par paliers + marqueurs numérotés + légende.

    Returns:
        Path | None: Le chemin du PNG, ou ``None`` si les shapefiles sont
        indisponibles (carte omise).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "zone_situation_map.png"
    try:
        g = _hot_zones(data)
    except Exception:
        return None

    # Contexte = ZS des provinces touchées ; cadrage serré sur les ZS atteintes.
    context = g[g["province"].isin(data.provinces_touchees)]
    context = context if not context.empty else g
    hot = context[context["confirmes"] > 0].copy()
    if hot.empty:
        return None

    # Classement décroissant : le n° 1 = la zone la plus touchée.
    hot = hot.sort_values("confirmes", ascending=False).reset_index(drop=True)
    hot["rang"] = range(1, len(hot) + 1)
    hot["cls"] = hot["confirmes"].map(_class_index)

    fig, (ax, ax_leg) = plt.subplots(
        1, 2, figsize=(11.0, 6.4), gridspec_kw={"width_ratios": [3.0, 1.55]}
    )

    # --- Carte ---------------------------------------------------------------
    context.plot(ax=ax, color="#F4F4F4", edgecolor="#C8C8C8", linewidth=0.4)
    for cls in sorted(hot["cls"].unique()):
        sub = hot[hot["cls"] == cls]
        sub.plot(
            ax=ax,
            color=_CLASS_COLORS[cls],
            edgecolor="#7B241C",
            linewidth=0.6,
            hatch="///",
        )

    # Délimitation des provinces (contours uniquement) pour situer les ZS dans
    # leur province d'appartenance ; limitée aux provinces du contexte. On
    # étiquette aussi le nom de chaque province impactée.
    try:
        prov = charts._load_provinces()
    except Exception:
        prov = None  # contours omis si la géométrie provinces est indisponible
    if prov is not None:
        prov = prov[prov["province"].isin(context["province"].unique())]
        if not prov.empty:
            prov.boundary.plot(ax=ax, color="#34495E", linewidth=1.1, linestyle="-", zorder=3)
            for _, prow in prov.iterrows():
                c = prow.geometry.representative_point()
                ax.annotate(
                    str(prow["province"]).upper(),
                    (c.x, c.y),
                    fontsize=8.0,
                    fontweight="bold",
                    ha="center",
                    va="center",
                    color="#34495E",
                    zorder=4,
                    bbox=dict(
                        boxstyle="round,pad=0.2",
                        facecolor="white",
                        edgecolor="none",
                        alpha=0.55,
                    ),
                )

    # Cadrage sur l'étendue des provinces impactées (vue contextuelle, non
    # centrée sur les seules ZS avec cas) ; repli sur les ZS du contexte.
    frame = prov if (prov is not None and not prov.empty) else context
    minx, miny, maxx, maxy = frame.total_bounds
    mx, my = (maxx - minx) * 0.06 + 1e-6, (maxy - miny) * 0.06 + 1e-6
    ax.set_xlim(minx - mx, maxx + mx)
    ax.set_ylim(miny - my, maxy + my)
    ax.set_aspect("equal")

    # Marqueurs numérotés au point représentatif de chaque ZS touchée, puis
    # désempilement (adjustText) avec traits de rappel vers le polygone.
    pts = [row.geometry.representative_point() for _, row in hot.iterrows()]
    texts = [
        ax.text(
            p.x,
            p.y,
            str(rang),
            fontsize=7.5,
            fontweight="bold",
            ha="center",
            va="center",
            color="#1B2631",
            zorder=5,
            bbox=dict(
                boxstyle="circle,pad=0.28",
                facecolor="white",
                edgecolor="#7B241C",
                linewidth=0.9,
            ),
        )
        for p, rang in zip(pts, hot["rang"], strict=False)
    ]
    adjust_text(
        texts,
        x=[p.x for p in pts],
        y=[p.y for p in pts],
        ax=ax,
        expand=(1.4, 1.6),
        arrowprops=dict(arrowstyle="-", color="#7B241C", lw=0.5),
    )
    ax.set_axis_off()
    ax.set_title("Cas confirmés MVE par zone de santé", fontsize=11, color=charts._RED)

    # Légende des paliers (classes), en bas à gauche de la carte.
    handles: list = [
        mpatches.Patch(facecolor=_CLASS_COLORS[i], edgecolor="#7B241C", label=lab)
        for i, lab in enumerate(_CLASS_LABELS)
    ]
    if prov is not None and not prov.empty:
        handles.append(Line2D([0], [0], color="#34495E", linewidth=1.1, label="Limite de province"))
    ax.legend(
        handles=handles,
        title="Cas confirmés (classes)",
        loc="lower left",
        fontsize=7,
        title_fontsize=7.5,
        framealpha=0.9,
    )

    # --- Légende latérale : n° → zone (cas) ----------------------------------
    ax_leg.set_axis_off()
    ax_leg.set_title("Zones de santé touchées", fontsize=10, color=charts._RED)
    n = len(hot)
    y0, dy = 0.96, min(0.052, 0.92 / max(n, 1))
    for i, row in hot.iterrows():
        y = y0 - i * dy  # type: ignore
        ax_leg.annotate(
            str(row["rang"]),
            (0.04, y),
            xycoords="axes fraction",
            fontsize=7,
            fontweight="bold",
            ha="center",
            va="center",
            color="#1B2631",
            bbox=dict(
                boxstyle="circle,pad=0.22",
                facecolor=_CLASS_COLORS[row["cls"]],
                edgecolor="#7B241C",
                linewidth=0.8,
            ),
        )
        ax_leg.text(
            0.13,
            y,
            f"{row['zone']}  ({row['confirmes']})",
            transform=ax_leg.transAxes,
            fontsize=7.5,
            va="center",
            ha="left",
        )

    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def _preview(csv_path: str, out_dir: str) -> None:
    """Rendu de démonstration depuis un CSV agrégé (sans le pipeline complet)."""
    data = compute(load_raw(csv_path))
    out = zone_situation_map(data, Path(out_dir))
    print(f"Carte écrite : {out}" if out else "Carte omise (shapefiles absents).")


if __name__ == "__main__":
    import argparse

    import config

    p = argparse.ArgumentParser(description="Aperçu de la carte ZS lisible.")
    p.add_argument("--csv", default=str(config.DEFAULT_CSV))
    p.add_argument("--out-dir", default="data/_sitrep_assets")
    a = p.parse_args()
    _preview(a.csv, a.out_dir)
