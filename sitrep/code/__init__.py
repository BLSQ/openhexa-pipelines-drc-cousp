"""Espace de code du dépôt RDC SiteRep (pipelines OpenHexa).

Ce package racine s'appelle ``code``, ce qui occulte le module standard
``code`` (utilisé notamment par ``pdb`` et IDLE). Pour rester compatible avec
ces outils (et donc avec ``pytest``, qui importe ``pdb``), on ré-exporte ici
l'API publique du module standard ``code``.
"""

from __future__ import annotations

import importlib.util as _ilu
import sysconfig as _sc
from pathlib import Path as _Path

_stdlib_code = _Path(_sc.get_paths()["stdlib"]) / "code.py"
if _stdlib_code.exists():
    _spec = _ilu.spec_from_file_location("_stdlib_code", _stdlib_code)
    if _spec and _spec.loader:
        _mod = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        InteractiveInterpreter = _mod.InteractiveInterpreter
        InteractiveConsole = _mod.InteractiveConsole
        interact = _mod.interact
        compile_command = _mod.compile_command
        del _spec, _mod

del _ilu, _sc, _Path, _stdlib_code
