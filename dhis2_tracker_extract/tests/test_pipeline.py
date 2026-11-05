"""Tests de la logique de décision incrémentale du pipeline (sans BD).

On vérifie le choix du ``updatedAfter`` selon le mode (incrémental / fenêtre
manuelle / rechargement complet / première exécution) en simulant le filigrane.
"""

from __future__ import annotations

from datetime import datetime

import pytest

import pipeline


def test_watermark_premiere_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sans filigrane, l'extraction est complète."""
    monkeypatch.setattr(pipeline.db_operations, "read_watermark", lambda *_: None)
    out = pipeline.incremental_watermark(None, "mve_notification", False, False)
    assert out is None  # complet


def test_watermark_incremental(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avec filigrane, il sert de borne updatedAfter."""
    monkeypatch.setattr(
        pipeline.db_operations,
        "read_watermark",
        lambda *_: datetime(2026, 6, 1, 8, 30, 0),
    )
    out = pipeline.incremental_watermark(None, "mve_notification", False, False)
    assert out == "2026-06-01T08:30:00"


def test_watermark_fenetre_manuelle_ignore_filigrane(monkeypatch: pytest.MonkeyPatch) -> None:
    """Une fenêtre manuelle ignore le filigrane."""
    monkeypatch.setattr(
        pipeline.db_operations,
        "read_watermark",
        lambda *_: datetime(2026, 6, 1, 8, 30, 0),
    )
    out = pipeline.incremental_watermark(None, "mve_notification", True, False)
    assert out is None


def test_watermark_full_refresh_ignore_filigrane(monkeypatch: pytest.MonkeyPatch) -> None:
    """Le rechargement complet ignore le filigrane."""
    monkeypatch.setattr(
        pipeline.db_operations,
        "read_watermark",
        lambda *_: datetime(2026, 6, 1, 8, 30, 0),
    )
    out = pipeline.incremental_watermark(None, "mve_notification", False, True)
    assert out is None


def test_validate_date_rejette_format_invalide() -> None:
    """Seul le format YYYY-MM-DD (ou l'absence de date) est accepté."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        pipeline.validate_date("31/05/2026", "occurred_after")
    pipeline.validate_date("2026-05-31", "occurred_after")  # ok
    pipeline.validate_date(None, "occurred_after")  # ok


def test_memory_mb_positif() -> None:
    """La mémoire courante et le pic sont mesurés."""
    rss, peak = pipeline.memory_mb()
    assert rss > 0
    assert peak > 0
