from __future__ import annotations

import multiprocessing
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import TypeVar

T = TypeVar("T")


class ProcessusInterrompuError(RuntimeError):
    """Le processus d'un programme s'est arrêté sans rendre de résultat."""


def executer_isole(fonction: Callable[..., T], *args: object, **kwargs: object) -> T:
    """Exécute ``fonction`` dans un processus neuf, détruit à la fin de l'appel.

    Toute la mémoire du processus est rendue au système à son retour : le
    programme suivant repart de zéro, au lieu d'hériter de la mémoire que les
    allocateurs de Python et de polars gardent réservée. On n'utilise pas les
    ``@task`` du SDK : leur ``Pool`` réutilise ses processus d'une tâche à
    l'autre, et un processus tué y laisse le run bloqué jusqu'au timeout.

    Le démarrage se fait en ``spawn`` (processus vierge, pas de copie de la
    mémoire du parent). ``fonction`` et ses arguments doivent être sérialisables
    (fonction définie au niveau d'un module importable).

    Parameters
    ----------
    fonction : Callable[..., T]
        Fonction à exécuter.
    *args, **kwargs : object
        Arguments transmis à ``fonction``.

    Returns
    -------
    T
        Le résultat de ``fonction``.

    Raises
    ------
    ProcessusInterrompuError
        Si le processus meurt sans rendre de résultat, typiquement tué par
        Kubernetes pour dépassement de la limite mémoire du pod.
    """
    contexte = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=contexte) as executeur:
        futur = executeur.submit(fonction, *args, **kwargs)
        try:
            return futur.result()
        except BrokenProcessPool:
            raise ProcessusInterrompuError(
                "processus arrêté brutalement sans rendre de résultat — très probablement tué "
                "pour dépassement de la limite mémoire du pod (cf. le dernier message [RAM …])"
            ) from None
