"""Tests de l'upsert par tranches (SQLite en mémoire fichier, sans PostgreSQL)."""

from __future__ import annotations

import polars as pl
from sqlalchemy import create_engine, text

import db_operations


def _events(ids: list[str], value: str) -> pl.DataFrame:
    return pl.DataFrame({"event_id": ids, "value": [value] * len(ids)})


def _rows(engine, table: str) -> list[tuple]:
    with engine.connect() as conn:
        return conn.execute(text(f'SELECT event_id, value FROM "{table}" ORDER BY 1, 2')).all()


def test_upsert_par_tranches(tmp_path):
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
