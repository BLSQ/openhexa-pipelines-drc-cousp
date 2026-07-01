from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # backend non interactif (pipeline headless)
from collections import Counter

import config
import geopandas as gpd
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from data.model import SitRepData
from matplotlib.axes import Axes
from utils import geo

# Palette alignée sur la nouvelle version du SitRep.
_RED = "#C00000"
_MALE = "#E07B39"  # orange
_FEMALE = "#7E2F8E"  # violet
_BASEMAP = "#F2F2F2"
_EDGE = "#BBBBBB"
_BLUE = "#1a3a5c"

_DPI = 150
_GRID = {"color": "#CCCCCC", "linestyle": "--", "linewidth": 0.5, "alpha": 0.8}


def _add_grid(ax: Axes, axis: str) -> None:
    """Grille de fond discrète pour faciliter la lecture (derrière les barres)."""
    ax.grid(axis=axis, **_GRID)  # type: ignore
    ax.set_axisbelow(True)


def epi_curve(data: SitRepData, out_dir: Path) -> Path | None:
    """Histogramme par semaine épidémiologique de début des symptômes.

    Returns:
        Path | None: Le chemin du PNG, ou ``None`` si la série est vide (ex.
        ``date_debut_symptomes`` non alimentée) → le rendu affichera « visuel
        indisponible ».
    """
    if not data.epi_curve:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "epi_curve.png"

    dates = [d for d, _ in data.epi_curve]
    values = [v for _, v in data.epi_curve]

    fig, ax = plt.subplots(figsize=(7.2, 3.0))
    # Barres hebdomadaires : ~6 jours de large (semaine = 7 j, 1 j d'écart).
    ax.bar(dates, values, color=_RED, width=6, align="edge")  # type: ignore
    _add_grid(ax, "y")
    ax.set_ylabel("Nombre de nouveaux\ncas confirmés", fontsize=9)
    # Abscisse = semaine épidémiologique ISO (« S20 », lundi).
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))  # type: ignore
    ax.xaxis.set_major_formatter(mdates.DateFormatter("S%V"))
    ax.set_xlabel("Semaine épidémiologique (début des symptômes)", fontsize=9)
    ax.tick_params(axis="both", labelsize=8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def age_sex_pyramid(data: SitRepData, out_dir: Path) -> Path:
    """Pyramide des cas confirmés par tranche d'âge et sexe.

    Returns:
        Path: Le chemin du PNG généré.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "age_sex_pyramid.png"

    ages = config.AGE_ORDER
    male = data.agesex_pyramid["Masculin"]
    female = data.agesex_pyramid["Feminin"]
    y = range(len(ages))

    fig, ax = plt.subplots(figsize=(7.2, 3.4))
    _add_grid(ax, "x")
    ax.barh(y, [-v for v in male], color=_MALE, label="Masculin")
    ax.barh(y, female, color=_FEMALE, label="Feminin")
    ax.set_yticks(list(y))
    ax.set_yticklabels(ages, fontsize=8)
    ax.set_xlabel("Nombre de cas confirmés", fontsize=9)
    ax.set_ylabel("Groupes d'âge", fontsize=9)

    # Axe x symétrique avec valeurs absolues.
    maxv = max([1, *male, *female])
    ticks = range(-maxv, maxv + 1, max(1, maxv // 4))
    ax.set_xticks(list(ticks))
    ax.set_xticklabels([str(abs(t)) for t in ticks], fontsize=8)
    ax.legend(title="Sexe", fontsize=8, title_fontsize=8, loc="lower right")
    ax.axvline(0, color="black", linewidth=0.6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def combinaison_symptomes(data: SitRepData, out_dir: Path) -> Path | None:
    """Combinaison des symptomes des cas confirmés.

    Returns:
        Path | None: Le chemin du PNG généré, ou ``None`` si les colonnes de
        symptômes sont absentes de l'extraction (visuel alors omis).
    """
    assert data.raw is not None
    if not set(config.DE_UUID_SYMPTOMES).issubset(data.raw.columns):
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "combi_symptomes.png"

    min_count = 5  # seuil minimum par combinaison
    df_conf = (
        data.raw.rename(config.DE_UUID_SYMPTOMES).filter(pl.col("n_confirmes") != 0).to_pandas()
    )
    memberships = []
    for _, row in df_conf.iterrows():
        signs = frozenset(label for col, label in config.SIGNES.items() if row[col] == "O")
        if signs:
            memberships.append(signs)

    n_total = len(memberships)

    # ── Combinaisons filtrées ─────────────────────────────────────────────────────
    combo_counts = Counter()
    for m in memberships:
        combo_counts[m] += 1

    top_combos = sorted(
        [(combo, count) for combo, count in combo_counts.items() if count >= min_count],
        key=lambda x: -x[1],
    )

    # Supprimer les symptômes absents de toutes les combinaisons retenues
    active_labels = set()
    for combo, _ in top_combos:
        active_labels |= combo

    all_labels = list(config.SIGNES.values())
    set_sizes = {label: sum(1 for m in memberships if label in m) for label in all_labels}
    labels = sorted(
        [lab for lab in all_labels if lab in active_labels],
        key=lambda lab: -set_sizes[lab],
    )
    n_labels = len(labels)
    n_combos = len(top_combos)

    # ── Figure ────────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 7))
    gs = gridspec.GridSpec(
        2, 2, width_ratios=[0.25, 4], height_ratios=[2.5, 1], hspace=0.05, wspace=0.05
    )

    ax_bar = fig.add_subplot(gs[0, 1])
    ax_matrix = fig.add_subplot(gs[1, 1])
    ax_empty_tl = fig.add_subplot(gs[0, 0])
    ax_empty_bl = fig.add_subplot(gs[1, 0])
    ax_empty_tl.axis("off")
    ax_empty_bl.axis("off")

    # Barres verticales
    counts = [c for _, c in top_combos]
    ax_bar.bar(np.arange(n_combos), counts, color=_BLUE, width=0.6)
    for xi, count in enumerate(counts):
        ax_bar.text(xi, count + 0.2, str(count), ha="center", va="bottom", fontsize=8)
    ax_bar.set_xlim(-0.5, n_combos - 0.5)
    ax_bar.set_xticks([])
    ax_bar.set_ylabel("Nombre de cas", fontsize=9)
    ax_bar.spines[["top", "right", "bottom"]].set_visible(False)
    ax_bar.set_title(
        f"Combinaisons de symptômes — Cas confirmés BVD\n"
        f"n = {n_total} cas avec au moins un symptôme renseigné | combinaisons ≥ {min_count} cas",
        fontsize=10,
        loc="center",
        pad=10,
    )

    # Matrice points
    ax_matrix.set_xlim(-0.5, n_combos - 0.5)
    ax_matrix.set_ylim(-0.5, n_labels - 0.5)
    ax_matrix.set_xticks([])
    ax_matrix.set_yticks(range(n_labels))
    ax_matrix.set_yticklabels(labels, fontsize=8.5)
    ax_matrix.tick_params(left=False)
    ax_matrix.spines[["top", "right", "bottom", "left"]].set_visible(False)

    for i in range(n_labels):
        if i % 2 == 0:
            ax_matrix.axhspan(i - 0.5, i + 0.5, color="#f5f5f5", zorder=0)

    for yi in range(n_labels):
        for xi in range(n_combos):
            ax_matrix.scatter(xi, yi, color="#cccccc", s=70, zorder=1)

    for xi, (combo, _) in enumerate(top_combos):
        active_y = sorted([labels.index(s) for s in combo if s in labels])
        for yi in active_y:
            ax_matrix.scatter(xi, yi, color=_BLUE, s=90, zorder=3)
        if len(active_y) > 1:
            ax_matrix.plot([xi, xi], [min(active_y), max(active_y)], color=_BLUE, lw=2, zorder=2)

    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _load_provinces() -> gpd.GeoDataFrame:
    """Charge la géométrie des provinces avec leur nom canonique.

    Returns:
        gpd.GeoDataFrame: Les provinces, colonne ``province`` canonisée.
    """
    g = gpd.read_parquet(config.DEFAULT_PROVINCES_SHAPEFILE)
    g["province"] = g["name"].astype(str).map(geo.canonical_province_name)
    return g


def _load_zones() -> gpd.GeoDataFrame:
    """Charge la géométrie des zones de santé (colonnes ``zone``/``province``).

    Returns:
        gpd.GeoDataFrame: Les zones de santé prêtes pour la jointure.
    """
    try:
        g = gpd.read_parquet(config.DEFAULT_SHAPEFILE)
    except Exception:
        g = gpd.read_file(config.DEFAULT_SHAPEFILE_FALLBACK)
    name = g["name"].astype(str)
    g["zone"] = (
        name.str.replace(r"^[a-z]{2}\s+", "", regex=True)
        .str.replace(config.SHAPE_NAME_SUFFIX, "", regex=False)
        .str.strip()
    )
    g["province"] = name.str.split(" ").str[0].map(config.SHAPE_PREFIX_TO_PROVINCE)
    return g


def _label_color(value: float, vmax: float) -> str:
    """Texte blanc sur fond foncé, sombre sinon.

    Returns:
        str: La couleur du texte (blanc ou anthracite).
    """
    return "white" if vmax and value > 0.5 * vmax else "#1B2631"


def _draw_provinces(ax: Axes, data: SitRepData) -> None:
    g = _load_provinces()
    assert data.raw is not None
    lookup = {
        r["province"]: r["confirmes"]
        for r in data.raw.group_by("province")
        .agg(pl.col("n_confirmes").sum().alias("confirmes"))
        .to_dicts()
    }
    g["confirmes"] = [int(lookup.get(p, 0)) for p in g["province"]]

    g.plot(ax=ax, color=_BASEMAP, edgecolor=_EDGE, linewidth=0.4)
    hot = g[g["confirmes"] > 0]
    vmax = max(hot["confirmes"]) if not hot.empty else 0
    if not hot.empty:
        hot.plot(
            ax=ax,
            column="confirmes",
            cmap="Reds",
            edgecolor="#7B241C",
            linewidth=0.6,
            legend=True,
            legend_kwds={"label": "Cas confirmés", "shrink": 0.45},
        )
    # Étiquette le nom de TOUTES les provinces : les touchées en gras avec le
    # nombre de cas (texte contrasté sur l'aplat), les autres en gris discret.
    for _, row in g.iterrows():
        c = row.geometry.representative_point()
        if row["confirmes"] > 0:
            ax.annotate(
                f"{row['province']}\n({row['confirmes']})",
                (c.x, c.y),
                fontsize=6.5,
                ha="center",
                va="center",
                fontweight="bold",
                color=_label_color(row["confirmes"], vmax),
            )
        else:
            ax.annotate(
                str(row["province"]),
                (c.x, c.y),
                fontsize=5.0,
                ha="center",
                va="center",
                color="#7B7D7D",
            )
    ax.set_axis_off()
    ax.set_title("Cas confirmés par province", fontsize=10, color=_RED)


def _draw_zones(ax: Axes, data: SitRepData) -> None:
    g = _load_zones()
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

    # Zoom sur les provinces touchées (contexte des ZS voisines inclus).
    frame = g[g["province"].isin(data.provinces_touchees)]
    frame = frame if not frame.empty else g
    frame.plot(ax=ax, color=_BASEMAP, edgecolor=_EDGE, linewidth=0.3)
    hot = frame[frame["confirmes"] > 0]
    if not hot.empty:
        hot.plot(
            ax=ax,
            column="confirmes",
            cmap="Reds",
            edgecolor="#7B241C",
            linewidth=0.4,
            legend=True,
            legend_kwds={"label": "Cas confirmés", "shrink": 0.45},
        )
        for _, row in hot.iterrows():
            c = row.geometry.representative_point()
            ax.annotate(
                row["zone"],
                (c.x, c.y),
                fontsize=4.5,
                ha="center",
                va="center",
                color="#1B2631",
            )
    ax.set_axis_off()
    ax.set_title("Cas confirmés par zone de santé", fontsize=10, color=_RED)


def situation_maps(data: SitRepData, out_dir: Path) -> Path | None:
    """Deux cartes côte à côte : par province (national) et par zone de santé.

    Returns:
        Path | None: Le chemin du PNG, ou ``None`` si les shapefiles sont
        indisponibles (carte omise).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "situation_maps.png"
    fig, (ax_prov, ax_zone) = plt.subplots(1, 2, figsize=(11.0, 5.4))
    try:
        _draw_provinces(ax_prov, data)
        _draw_zones(ax_zone, data)
    except Exception:
        plt.close(fig)
        return None
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def build_all(data: SitRepData, out_dir: Path) -> dict[str, Path | None]:
    """Génère tous les visuels et renvoie un dict de chemins.

    Returns:
        dict[str, Path | None]: Les chemins des visuels (``None`` si omis).
    """
    return {
        "epi_curve": epi_curve(data, out_dir),
        "age_sex_pyramid": age_sex_pyramid(data, out_dir),
        "affected_zones_map": situation_maps(data, out_dir),
        "combi_symptomes": combinaison_symptomes(data, out_dir),
    }
