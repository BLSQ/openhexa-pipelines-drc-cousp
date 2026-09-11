from __future__ import annotations

from pathlib import Path

import config
import yaml


def load_narrative(path: str | Path | None = None) -> dict:
    """Charge le YAML narratif. Renvoie un dict vide si le fichier est absent.

    Returns:
        dict: Le contenu narratif désérialisé, ou un dict vide si absent.
    """
    p = Path(path) if path else config.NARRATIVE_YAML
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
