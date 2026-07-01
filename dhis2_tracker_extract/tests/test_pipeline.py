"""Tests de la logique de décision incrémentale du pipeline (sans BD).

On vérifie le choix du ``updatedAfter`` selon le mode (incrémental / fenêtre
manuelle / rechargement complet / première exécution) en simulant le filigrane.
"""

from __future__ import annotations

from datetime import datetime

import pipeline


def test_watermark_premiere_execution(monkeypatch):
    monkeypatch.setattr(pipeline.db_operations, "read_watermark", lambda *_: None)
    out = pipeline.incremental_watermark(None, "mve_notification", False, False)
    assert out is None  # complet


def test_watermark_incremental(monkeypatch):
    monkeypatch.setattr(
        pipeline.db_operations,
        "read_watermark",
        lambda *_: datetime(2026, 6, 1, 8, 30, 0),
    )
    out = pipeline.incremental_watermark(None, "mve_notification", False, False)
    assert out == "2026-06-01T08:30:00"


def test_watermark_fenetre_manuelle_ignore_filigrane(monkeypatch):
    monkeypatch.setattr(
        pipeline.db_operations,
        "read_watermark",
        lambda *_: datetime(2026, 6, 1, 8, 30, 0),
    )
    out = pipeline.incremental_watermark(None, "mve_notification", True, False)
    assert out is None


def test_watermark_full_refresh_ignore_filigrane(monkeypatch):
    monkeypatch.setattr(
        pipeline.db_operations,
        "read_watermark",
        lambda *_: datetime(2026, 6, 1, 8, 30, 0),
    )
    out = pipeline.incremental_watermark(None, "mve_notification", False, True)
    assert out is None


def test_validate_date_rejette_format_invalide():
    import pytest

    with pytest.raises(ValueError):
        pipeline.validate_date("31/05/2026", "occurred_after")
    pipeline.validate_date("2026-05-31", "occurred_after")  # ok
    pipeline.validate_date(None, "occurred_after")  # ok
