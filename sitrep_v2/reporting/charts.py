from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # backend non interactif (pipeline headless)

import config
import geopandas as gpd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
from data.model import SitRepData
from matplotlib.axes import Axes
from utils import geo
from utils.dates import fr_month_year

# Palette alignée sur la nouvelle version du SitRep (reprise de v1 telle quelle).
_RED = "#C00000"
_MALE = "#E07B39"  # orange
_FEMALE = "#7E2F8E"  # violet
_BASEMAP = "#F2F2F2"
_EDGE = "#BBBBBB"

_DPI = 150
_GRID = {"color": "#CCCCCC", "linestyle": "--", "linewidth": 0.5, "alpha": 0.8}


def _add_grid(ax: Axes, axis: str) -> None:
    """Grille de fond discrète pour faciliter la lecture (derrière les barres)."""
    ax.grid(axis=axis, **_GRID)  # type: ignore
    ax.set_axisbelow(True)


def epi_curve(data: SitRepData, out_dir: Path) -> Path | None:
    """Histogramme quotidien par date de début des symptômes, split vivant/décédé.

    Contrairement à v1 (hebdomadaire, une seule série), la courbe est
    **quotidienne** sur la fenêtre glissante de
    ``data.metrics.EPI_CURVE_WINDOW_DAYS`` jours et empile 2 séries (cas
    confirmés vivants / décédés).

    Returns:
        Path | None: Le chemin du PNG, ou ``None`` si la série est vide.
    """
    if not data.epi_curve:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "epi_curve.png"

    dates = [d for d, _, _ in data.epi_curve]
    vivants = [v for _, v, _ in data.epi_curve]
    deces = [d for _, _, d in data.epi_curve]
    n_total = sum(vivants) + sum(deces)

    fig, ax = plt.subplots(figsize=(9.0, 3.4))
    ax.bar(dates, vivants, color="#1a3a5c", width=0.8, label="Vivant")
    ax.bar(dates, deces, bottom=vivants, color=_RED, width=0.8, label="Décédé")
    _add_grid(ax, "y")
    ax.set_ylabel("Nouveaux cas confirmés", fontsize=9)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax.set_xlabel("Date de début des symptômes", fontsize=9)
    ax.tick_params(axis="both", labelsize=7)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_title(f"Nombre de cas confirmés par date de début des symptômes (n = {n_total})", fontsize=10)
    # Légende (« Vivant »/« Décédé ») centrée sous le titre de l'axe X, sur une seule ligne.
    ax.legend(fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.42), ncol=2, frameon=False)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    # Note de source : coin bas-droit de la figure entière (coordonnées figure,
    # indépendantes de la légende ci-dessus, pour ne jamais se chevaucher),
    # répartie sur 2 lignes.
    fig.subplots_adjust(bottom=0.46)
    fig.text(
        0.99,
        0.02,
        f"Source : DHIS2 Tracker - Riposte MVE, {fr_month_year(data.reporting_end)}.\n"
        f"Données incluses : cas confirmés avec statut vital et date de début de symptômes renseignés, n = {n_total}",
        ha="right",
        va="bottom",
        fontsize=8,
        color="#7B7D7D",
    )
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    return path


def _draw_pyramid(ax: Axes, pyramid: dict, title: str) -> None:
    ages = config.AGE_ORDER
    male, female = config.SEX_ORDER
    male_vals = pyramid[male]
    female_vals = pyramid[female]
    y = range(len(ages))

    _add_grid(ax, "x")
    ax.barh(y, [-v for v in male_vals], color=_MALE, label=male)
    ax.barh(y, female_vals, color=_FEMALE, label=female)
    ax.set_yticks(list(y))
    # Les libellés portent un préfixe numérique (ordre de tri, cf. config.AGE_ORDER
    # = config.AGE_LABELS de compute_indicators_mve_tdb) — masqué à l'affichage.
    ax.set_yticklabels([age.split(". ", 1)[-1] for age in ages], fontsize=9)
    ax.set_xlabel("Nombre de cas confirmés", fontsize=10, labelpad=22)
    ax.set_title(title, fontsize=11)

    maxv = max([1, *male_vals, *female_vals])
    ticks = range(-maxv, maxv + 1, max(1, maxv // 4))
    ax.set_xticks(list(ticks))
    ax.set_xticklabels([str(abs(t)) for t in ticks], fontsize=9)
    # Légende (« Masculin »/« Féminin ») centrée sous le titre de l'axe X, sur une seule ligne.
    ax.legend(fontsize=9, loc="upper center", bbox_to_anchor=(0.5, -0.32), ncol=2, frameon=False)
    ax.axvline(0, color="black", linewidth=0.6)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)


def age_sex_pyramid(data: SitRepData, out_dir: Path) -> Path:
    """Deux pyramides des cas confirmés par tranche d'âge et sexe, côte à côte.

    La 1ʳᵉ affiche le cumul depuis le début de l'épidémie, la 2ᵉ uniquement
    les nouveaux cas des dernières 24h.

    Returns:
        Path: Le chemin du PNG généré.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "age_sex_pyramid.png"

    fig, (ax_cum, ax_jour) = plt.subplots(1, 2, figsize=(10.8, 3.6))
    _draw_pyramid(ax_cum, data.agesex_pyramid, "Cumul (depuis le début de l'épidémie)")
    _draw_pyramid(ax_jour, data.agesex_pyramid_jour, "Dernières 24 heures")
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
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


def build_all(data: SitRepData, out_dir: Path) -> dict[str, Path | None]:
    """Génère les visuels non cartographiques et renvoie un dict de chemins.

    Les cartes (province/zone de santé) sont produites séparément par
    ``reporting.zone_map``. ``combinaison_symptomes`` de v1 n'est pas repris
    (absent du nouveau template).

    Returns:
        dict[str, Path | None]: Les chemins des visuels (``None`` si omis).
    """
    return {
        "epi_curve": epi_curve(data, out_dir),
        "age_sex_pyramid": age_sex_pyramid(data, out_dir),
    }
