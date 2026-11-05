"""Tests de l'upsert par tranches (SQLite en mémoire fichier, sans PostgreSQL)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
from sqlalchemy import create_engine, inspect, text

import db_operations

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.engine import Engine


def _events(ids: list[str], value: str) -> pl.DataFrame:
    return pl.DataFrame({"event_id": ids, "value": [value] * len(ids)})


def _rows(engine: Engine, table: str) -> list[tuple]:
    with engine.connect() as conn:
        return conn.execute(text(f'SELECT event_id, value FROM "{table}" ORDER BY 1, 2')).all()


def test_upsert_par_tranches(tmp_path: Path) -> None:
    """Staging écrite par tranches, puis remplacement des lignes par event_id."""
    engine = create_engine(f"sqlite:///{tmp_path / 'db.sqlite'}")
    messages: list[str] = []

    # Première écriture : 5 lignes en tranches de 2 -> la staging devient la cible.
    n = db_operations.upsert_events(
        engine,
        "t",
        _events(["e1", "e1", "e2", "e3", "e4"], "v1"),
        chunk_size=2,
        log=messages.append,
    )
    assert n == 5
    assert len(_rows(engine, "t")) == 5
    assert sum("tranche" in m for m in messages) == 3

    # Upsert : e1 n'a plus qu'une ligne, e5 apparaît, e2..e4 inchangés.
    db_operations.upsert_events(engine, "t", _events(["e1", "e5"], "v2"), chunk_size=1)
    assert _rows(engine, "t") == [
        ("e1", "v2"),
        ("e2", "v1"),
        ("e3", "v1"),
        ("e4", "v1"),
        ("e5", "v2"),
    ]


def _flagged(ids: list[str], value: str, deleted: list[bool | None]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "event_id": ids,
            "value": [value] * len(ids),
            "deleted": deleted,
            "enrollment_deleted": [False] * len(ids),
        },
        schema_overrides={"deleted": pl.Boolean},
    )


def test_upsert_ecarte_les_lignes_supprimees(tmp_path: Path) -> None:
    """Les lignes supprimées dans DHIS2 retirent leur event_id sans être stockées."""
    engine = create_engine(f"sqlite:///{tmp_path / 'db.sqlite'}")

    # Première écriture : e2 (supprimé) n'est pas stocké, un drapeau nul l'est.
    n = db_operations.upsert_events(engine, "t", _flagged(["e1", "e2", "e3"], "v1", [False, True, None]))
    assert n == 2
    assert _rows(engine, "t") == [("e1", "v1"), ("e3", "v1")]
    # Les drapeaux de suppression ne sont pas écrits dans la cible.
    assert {c["name"] for c in inspect(engine).get_columns("t")} == {"event_id", "value"}

    # e1 supprimé ensuite dans DHIS2 : ses lignes disparaissent de la cible.
    n = db_operations.upsert_events(engine, "t", _flagged(["e1"], "v2", [True]))
    assert n == 0
    assert _rows(engine, "t") == [("e3", "v1")]


def test_upsert_replace_vide_la_cible(tmp_path: Path) -> None:
    """En rechargement complet, les event_id absents du lot (purgés) disparaissent."""
    engine = create_engine(f"sqlite:///{tmp_path / 'db.sqlite'}")
    db_operations.upsert_events(engine, "t", _flagged(["e1", "e2"], "v1", [False, False]))

    n = db_operations.upsert_events(engine, "t", _flagged(["e2", "e3"], "v2", [False, True]), replace=True)
    assert n == 1
    assert _rows(engine, "t") == [("e2", "v2")]


def test_upsert_replace_retire_les_colonnes_obsoletes(tmp_path: Path) -> None:
    """Le rechargement complet recrée la table : drapeaux et colonnes obsolètes disparaissent."""
    engine = create_engine(f"sqlite:///{tmp_path / 'db.sqlite'}")
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE "t" (event_id TEXT, value TEXT, deleted BOOLEAN, ancienne TEXT)'))
        conn.execute(text("INSERT INTO \"t\" VALUES ('e9', 'x', 0, 'y')"))

    # Upsert incrémental : la table garde son schéma, les nouvelles lignes ont deleted nul.
    db_operations.upsert_events(engine, "t", _flagged(["e1"], "v1", [False]))
    assert _rows(engine, "t") == [("e1", "v1"), ("e9", "x")]

    db_operations.upsert_events(engine, "t", _flagged(["e1"], "v2", [False]), replace=True)
    assert {c["name"] for c in inspect(engine).get_columns("t")} == {"event_id", "value"}
    assert _rows(engine, "t") == [("e1", "v2")]
