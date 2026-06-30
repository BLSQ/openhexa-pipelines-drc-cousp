from __future__ import annotations

from pathlib import Path

import config
import polars as pl
from openhexa.sdk import workspace
from utils import geo

METRICS = [
    "n_confirmes",
    "n_probables",
    "n_suspects_en_cours",
    "n_suspects_en_cours_investigation",
    "n_deces",
    "n_deces_confirmes",
    "n_deces_suspect",
    "n_orientes_isolement_cumul",
    "n_confirme_isole",
    "n_suspect_isole",
    "n_alertes",
    "n_alertes_investiguees",
    "n_alertes_validees",
    "n_alertes_invalidees",
    "n_echantillons_collectes",
    "n_echantillons_positifs",
    "n_echantillons_en_cours",
    "n_echantillons_invalides",
    "n_positifs_suspect_decedes",
    "n_isole_stock",
    "n_cas_actifs_stock",
    "n_gueris",
    "n_deces_pec",
    "n_evades",
    "n_transferes",
    "n_isole_stock_pec",
]

_RENAME = {
    "date_notification": "date_notif",
    "level_2_name": "province",
    "level_3_name": "zone_sante",
    "level_4_name": "aire_sante",
}

_REPORT_DATE_SRC = ("enrolled_at", "date_report")

_LONG_DATETIME_COLS = (
    "enrolled_at",
    "created_at",
    "occurred_at",
    "MPOX-N-Date et heure de notification de l'alerte",
)
_LONG_DATE_COLS = ("MVE - DDS (Date de début des symptômes)",)

_LONG_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)


def _sexe_norm_expr() -> pl.Expr:
    """Normalise le sexe brut (M/F/Homme/Femme…) vers Masculin/Feminin/Inconnu.

    Returns:
        pl.Expr: L'expression Polars produisant la colonne ``sexe_norm``.
    """
    e = pl.col("sexe").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    expr = pl.lit(config.SEXE_INCONNU)
    for raw, canon in config.SEXE_CANONICAL.items():
        expr = pl.when(e == raw).then(pl.lit(canon)).otherwise(expr)
    return expr.alias("sexe_norm")


def _tranche_age_expr() -> pl.Expr:
    """Bucketise l'âge brut (années) dans les tranches de ``config.AGE_BUCKETS``.

    Returns:
        pl.Expr: L'expression Polars produisant la colonne ``tranche_age``.
    """
    age = pl.col("age").cast(pl.Float64, strict=False)
    expr = pl.lit("Inconnu")
    for lo, hi, label in config.AGE_BUCKETS:
        expr = pl.when((age >= lo) & (age <= hi)).then(pl.lit(label)).otherwise(expr)
    return expr.alias("tranche_age")


def _clean(df: pl.DataFrame) -> pl.DataFrame:
    """Normalise une extraction brute vers le schéma interne du générateur.

    Returns:
        pl.DataFrame: Le DataFrame renommé, daté, géo-canonisé et complété.
    """
    df = df.rename({k: v for k, v in _RENAME.items() if k in df.columns})

    # Date de rapportage : enrolled_at (sinon date_report).
    for src in _REPORT_DATE_SRC:
        if "date_rapportage" not in df.columns and src in df.columns:
            df = df.rename({src: "date_rapportage"})

    # Dates (tolère datetime/string ; tronque à la date).
    for c in ("date_rapportage", "date_notif", "date_debut_symptomes"):
        if c in df.columns:
            df = df.with_columns(
                pl.col(c)
                .cast(pl.Utf8)
                .str.slice(0, 10)
                .str.to_date("%Y-%m-%d", strict=False)
                .alias(c)
            )
    # Si la date de rapportage est absente (extraction sans date_report), on se
    # rabat sur la date de notification comme axe de fenêtre.
    if "date_rapportage" not in df.columns and "date_notif" in df.columns:
        df = df.with_columns(pl.col("date_notif").alias("date_rapportage"))

    # Nettoyage géographique (préfixes province + suffixe « Zone de Santé »,
    # canonisation), pour matcher les noms de la géométrie.
    if "province" in df.columns:
        df = df.with_columns(geo.canonical_province_expr("province").alias("province"))
    for c in ("zone_sante", "aire_sante"):
        if c in df.columns:
            df = df.with_columns(
                geo.strip_prefix_expr(c)
                .str.replace(config.SHAPE_NAME_SUFFIX, "", literal=True)
                .str.strip_chars()
                .alias(c)
            )

    # Démographie : sexe normalisé + tranche d'âge.
    df = df.with_columns(
        _sexe_norm_expr()
        if "sexe" in df.columns
        else pl.lit(config.SEXE_INCONNU).alias("sexe_norm")
    )
    df = df.with_columns(
        _tranche_age_expr() if "age" in df.columns else pl.lit("Inconnu").alias("tranche_age")
    )

    # Compteurs : caste/complète à 0 les présents, crée les absents.
    present = [m for m in METRICS if m in df.columns]
    df = df.with_columns([pl.col(m).cast(pl.Int64, strict=False).fill_null(0) for m in present])
    missing = [m for m in METRICS if m not in df.columns]
    if missing:
        df = df.with_columns([pl.lit(0, dtype=pl.Int64).alias(m) for m in missing])
    return df


def date_anomalies(df: pl.DataFrame) -> dict[str, dict]:
    """Repère (sans filtrer) les dates hors plage plausible, pour les signaler.

    Returns:
        dict[str, dict]: ``{colonne: {count, examples, lo, hi}}`` pour
        ``date_rapportage`` et ``date_notif`` ayant au moins une valeur <
        ``DATE_PLAUSIBLE_MIN`` ou > ``DATE_PLAUSIBLE_MAX``.
    """
    lo, hi = config.DATE_PLAUSIBLE_MIN, config.DATE_PLAUSIBLE_MAX
    out: dict[str, dict] = {}
    for col in ("date_rapportage", "date_notif"):
        if col not in df.columns:
            continue
        bad = df.filter((pl.col(col) < lo) | (pl.col(col) > hi))
        if bad.height:
            ex = sorted({str(d) for d in bad[col].drop_nulls().unique().to_list()})
            out[col] = {"count": bad.height, "examples": ex[:5], "lo": lo, "hi": hi}
    return out


def _parse_long_datetime(col: str) -> pl.Expr:
    """Parse une colonne Utf8 datetime ISO tolérante aux formats mixtes.

    Returns:
        pl.Expr: L'expression Datetime (microsecondes), nulle si aucun format ne matche.
    """
    base = pl.col(col).str.strip_chars().str.replace("Z$", "")
    return pl.coalesce(
        [base.str.to_datetime(f, strict=False, time_unit="us") for f in _LONG_DATETIME_FORMATS]
    ).alias(col)


def _type_long_dates(df: pl.DataFrame) -> pl.DataFrame:
    """Type les colonnes temporelles de l'extraction longue (lue en Utf8).

    Returns:
        pl.DataFrame: Le DataFrame avec ``enrolled_at``/``created_at``/… en
        Datetime et la DDS en Date, prêt pour le pivot des indicateurs.
    """
    exprs = [
        _parse_long_datetime(c)
        for c in _LONG_DATETIME_COLS
        if c in df.columns and df.schema[c] == pl.Utf8
    ]
    exprs += [
        pl.col(c).str.to_date("%Y-%m-%d", strict=False).alias(c)
        for c in _LONG_DATE_COLS
        if c in df.columns and df.schema[c] == pl.Utf8
    ]
    return df.with_columns(exprs) if exprs else df


def load_raw(path: str | Path) -> pl.DataFrame:
    """Charge et nettoie une extraction fichier (CSV/Parquet) au schéma interne.

    Returns:
        pl.DataFrame: Le DataFrame nettoyé au schéma interne.
    """
    p = Path(path)
    df = pl.read_parquet(p) if p.suffix == ".parquet" else pl.read_csv(p, infer_schema_length=0)
    if "data_element_id" in df.columns:
        from data.indicators import build_definitive_from_raw  # noqa: PLC0415

        return build_definitive_from_raw(_type_long_dates(df))
    return _clean(df)


def _read_db(table: str | None = None, schema: str | None = None) -> pl.DataFrame:
    """Lit la table brute du workspace (format long DHIS2, sans nettoyage).

    Returns:
        pl.DataFrame: La table lue telle quelle, sans normalisation.
    """
    table = table or config.AGG_TABLE
    schema = schema or config.AGG_DB_SCHEMA
    fq = f'"{schema}"."{table}"' if schema else f'"{table}"'
    return pl.read_database_uri(f"SELECT * FROM {fq}", uri=workspace.database_url)


def load_from_db(table: str | None = None, schema: str | None = None) -> pl.DataFrame:
    """Lit et nettoie une table déjà agrégée (schéma interne) du workspace.

    Returns:
        pl.DataFrame: Le DataFrame nettoyé au schéma interne.
    """
    return _clean(_read_db(table, schema))
