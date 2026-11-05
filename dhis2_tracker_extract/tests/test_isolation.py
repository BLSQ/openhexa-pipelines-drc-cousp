"""Tests de l'exécution d'un programme dans un processus dédié."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import polars as pl
import pytest
from sqlalchemy import create_engine, inspect

import isolation
import programme

if TYPE_CHECKING:
    from pathlib import Path


def test_executer_isole_dans_un_autre_processus() -> None:
    """La fonction tourne dans un processus distinct du parent."""
    assert isolation.executer_isole(os.getpid) != os.getpid()


def test_executer_isole_propage_les_exceptions() -> None:
    """Une exception de la fonction remonte telle quelle au parent."""
    with pytest.raises(ValueError, match="invalid literal"):
        isolation.executer_isole(int, "pas un nombre")


def test_executer_isole_signale_un_processus_tue() -> None:
    """Un processus qui meurt sans résultat (ex. OOM) lève une erreur explicite."""
    with pytest.raises(isolation.ProcessusInterrompuError, match="limite mémoire"):
        isolation.executer_isole(os._exit, 1)


def test_extraire_programme_ecrit_et_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Le programme extrait, écarte les lignes supprimées, écrit la table et résume."""
    df = pl.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "tracked_entity_id": ["t1", "t2"],
            "deleted": [False, True],
            "enrollment_deleted": [False, False],
        }
    )
    monkeypatch.setattr(programme, "DHIS2", lambda _con: None)
    monkeypatch.setattr(programme.toolbox, "extract_tracker", lambda *_a, **_k: df)
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"

    resume = programme.extraire_programme(None, url, "P", "prog")  # type: ignore[arg-type]

    assert resume["lignes"] == 1
    assert resume["cas"] == 1
    assert resume["supprimees"] == 1
    assert resume["pic_mo"] > 0
    assert inspect(create_engine(url)).has_table("prog_events")


def test_extraire_programme_echec_explicite(monkeypatch: pytest.MonkeyPatch) -> None:
    """Un échec remonte en RuntimeError (sérialisable) avec le programme et l'étape."""

    def _boom(*_a: object, **_k: object) -> pl.DataFrame:
        raise KeyError("x")

    monkeypatch.setattr(programme, "DHIS2", lambda _con: None)
    monkeypatch.setattr(programme.toolbox, "extract_tracker", _boom)
    with pytest.raises(RuntimeError, match="prog : échec pendant l'extraction — KeyError"):
        programme.extraire_programme(None, "sqlite://", "P", "prog")  # type: ignore[arg-type]
