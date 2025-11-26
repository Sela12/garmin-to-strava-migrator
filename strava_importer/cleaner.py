"""Pre-sweep cleaner for FIT files.

This module provides a small pre-scan that inspects each FIT file's
``file_id`` message and moves files that are not activities into a
``_junk`` subfolder. The goal is to avoid uploading device logs,
monitoring files and other non-activity FITs to Strava and wasting
API quota.

Heuristics:
- If ``file_id.type`` exists and its string contains "activity" -> keep
- If ``file_id.type`` exists and does NOT contain "activity" -> move to ``_junk``
- If parsing fails or ``file_id`` not found -> keep file (safer)

This module depends on the ``fitparse`` package.
"""
from pathlib import Path
import logging
import shutil
from typing import Tuple, List, Any
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

from fitparse import FitFile, FitParseError


logger = logging.getLogger(__name__)


def _inspect_fit(path_str: str) -> Tuple[str, str, str]:
    """Inspect a single FIT file to determine whether it should be moved.

    This function is safe to run in a separate process (it takes a string
    path and returns a small tuple). The return value is a 3-tuple:

    - ``path_str``: the original path string passed in
    - ``action``: one of ``'move'``, ``'keep'`` or ``'error'``
    - ``reason``: a short string describing the file type or the parsing error

    Parameters
    ----------
    path_str: str
        Filesystem path to the .fit file examined.

    Returns
    -------
    Tuple[str, str, str]
        See description above.
    """
    try:
        fit = FitFile(path_str)
        file_id_msgs = list(fit.get_messages("file_id"))
        if not file_id_msgs:
            return path_str, "keep", "no_file_id"

        file_id = file_id_msgs[0]
        # `file_id` can be a fitparse Message or a dict-like object depending
        # on how fitparse yields messages in different versions/environments.
        # Guard access so static type checkers (Pylance) won't complain.
        try:
            if hasattr(file_id, "get_value"):
                ftype = getattr(file_id, "get_value")("type")  # type: Any
            elif isinstance(file_id, dict):
                ftype = file_id.get("type")  # type: Any
            else:
                ftype = None
        except Exception:
            ftype = None

        if ftype is None:
            return path_str, "keep", "no_type"

        ftype_str = str(ftype).lower()
        if "activity" in ftype_str:
            return path_str, "keep", ftype_str
        else:
            return path_str, "move", ftype_str

    except FitParseError as e:
        return path_str, "error", f"fitparse:{e}"
    except Exception as e:
        return path_str, "error", str(e)


def pre_sweep_move_junk(fit_folder: Path, workers: int | None = None) -> Tuple[int, int]:
    """Scan ``fit_folder`` and move non-activity files to a ``_junk`` subfolder.

    This function parallelizes the FIT inspection using a :class:`ProcessPoolExecutor`.

    Parameters
    ----------
    fit_folder: Path
        Path to the folder containing .fit files to inspect.
    workers: Optional[int]
        Number of worker processes to spawn. If ``None`` the function will
        pick a sensible default based on CPU count.

    Returns
    -------
    Tuple[int, int]
        ``(moved_count, inspected_count)``
    """
    fit_folder = Path(fit_folder)
    if not fit_folder.exists():
        logger.critical("FIT folder does not exist: %s", fit_folder)
        return 0, 0

    junk_dir = fit_folder / "_junk"
    junk_dir.mkdir(parents=True, exist_ok=True)

    fits: List[Path] = sorted(fit_folder.glob("*.fit")) + sorted(fit_folder.glob("*.FIT"))
    # Filter out files already in _junk/_failed
    fits = [f for f in fits if f.parent.name not in ("_junk", "_failed")]

    inspected = 0
    moved = 0

    if not fits:
        logger.info("No FIT files found in %s", fit_folder)
        return moved, inspected

    workers = workers or min(32, (os.cpu_count() or 1))
    logger.info("Pre-sweep starting: inspecting %s files with %s workers", len(fits), workers)

    # Submit inspect tasks
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_inspect_fit, str(f)): f for f in fits}

        for fut in as_completed(futures):
            fpath = futures[fut]
            try:
                path_str, action, reason = fut.result()
            except Exception as e:
                logger.exception("Worker crashed inspecting %s: %s", fpath, e)
                # Keep file when worker fails
                continue

            inspected += 1
            if action == 'move':
                try:
                    dest = junk_dir / Path(path_str).name
                    shutil.move(path_str, str(dest))
                    moved += 1
                    logger.info("Moved non-activity file %s -> %s (type=%s)", path_str, dest, reason)
                except Exception:
                    logger.exception("Failed to move %s to _junk", path_str)
            elif action == 'error':
                logger.warning("Error inspecting %s: %s; keeping file", path_str, reason)
            else:
                logger.debug("Keeping activity file %s (reason=%s)", path_str, reason)

    logger.info("Pre-sweep complete: inspected=%s moved_to_junk=%s", inspected, moved)
    return moved, inspected
