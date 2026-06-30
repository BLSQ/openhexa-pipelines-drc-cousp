"""Isolation du chemin d'import pour les tests de ``dhis2_tracker_extract``.

Le dépôt héberge deux pipelines OpenHexa qui exposent chacun un module
``config`` (importé en nom nu, comme l'exige OpenHexa). En production, chaque
pipeline tourne avec son dossier en tête de ``sys.path`` : pas de collision. En
local, on insère ici le dossier du pipeline en tête pour que ``import config`` /
``import toolbox`` résolvent bien ce paquet.

À lancer isolément : ``uv run pytest code/dhis2_tracker_extract/tests``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))
