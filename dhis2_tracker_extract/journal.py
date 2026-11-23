from __future__ import annotations

import resource
import sys
from pathlib import Path

from openhexa.sdk import current_run


def memory_mb() -> tuple[float, float]:
    """Mémoire du processus : résidente actuelle et pic, en Mo.

    La valeur courante vient de ``/proc/self/statm`` (Linux, cas des pods
    OpenHexa) ; ailleurs, on retombe sur le pic.

    Returns
    -------
    tuple[float, float]
        ``(rss_mb, peak_mb)``.
    """
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss est en octets sous macOS, en Ko sous Linux.
    peak = peak_kb / 1024 / (1024 if sys.platform == "darwin" else 1)
    try:
        resident_pages = int(Path("/proc/self/statm").read_text(encoding="ascii").split()[1])
        rss = resident_pages * resource.getpagesize() / 1024**2
    except OSError:
        rss = peak
    return rss, peak


def log_step(msg: str) -> None:
    """Journalise une étape avec la mémoire courante et le pic du processus.

    En cas d'arrêt brutal (dépassement de la limite mémoire du pod, qui ne
    laisse aucune trace Python), le dernier message situe l'étape et la mémoire
    atteinte. Dans le processus d'un programme (cf. :mod:`isolation`), les
    valeurs sont celles de ce processus.

    Parameters
    ----------
    msg : str
        Message d'étape.
    """
    rss, peak = memory_mb()
    current_run.log_info(f"{msg} [RAM {rss:.0f} Mo, pic {peak:.0f} Mo]")
