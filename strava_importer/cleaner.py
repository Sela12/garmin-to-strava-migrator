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
from typing import Tuple, List
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
                ftype = getattr(file_id, "get_value")("type")
            elif isinstance(file_id, dict):
                ftype = file_id.get("type")
            else:
                ftype = None
        except Exception:
            ftype = None

        if ftype is None:
            return path_str, "keep", "no_type"

        ftype_str = str(ftype).lower()
        if "activity" in ftype_str:
            # Additional heuristic: check session.sport and distance fields
            try:
                sessions = list(fit.get_messages("session"))
                if sessions:
                    session = sessions[0]
                    # Extract sport field
                    sport = None
                    distance = None
                    try:
                        if hasattr(session, "get_value"):
                            sport = getattr(session, "get_value")("sport")
                            distance = getattr(session, "get_value")("total_distance")
                        elif isinstance(session, dict):
                            sport = session.get("sport")
                            distance = session.get("total_distance")
                    except Exception:
                        pass

                    sport_str = str(sport).lower() if sport else ""
                    
                    # Reject training/synthetic activities without distance data
                    if sport_str == "training" or (distance is None and "training" in sport_str):
                        return path_str, "move", f"training_activity:{sport_str}"
            except Exception:
                pass
            
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

    processing_dir = fit_folder / "_processing"
    processing_dir.mkdir(parents=True, exist_ok=True)

    fits: List[Path] = sorted(fit_folder.glob("*.fit")) + sorted(fit_folder.glob("*.FIT"))
    # Filter out files already in special folders
    fits = [f for f in fits if f.parent.name not in ("_junk", "_failed", "_processing")]

    inspected = 0
    moved = 0

    if not fits:
        logger.info("No FIT files found in %s", fit_folder)
        return moved, inspected

    workers = workers or min(32, (os.cpu_count() or 1))
    logger.info("Pre-sweep starting: inspecting %s files with %s workers", len(fits), workers)

    # To avoid races where files may be moved/processed concurrently we first
    # atomically move each candidate into a dedicated `_processing` folder and
    # then submit the moved path to worker processes. Parent process then moves
    # the file to the final destination based on inspection result. Using
    # Path.replace/os.replace ensures moves are atomic on the same filesystem.
    moved_into_processing: List[Path] = []
    for f in fits:
        src = f
        dst = processing_dir / f.name
        try:
            # Atomic move; if file was removed/moved by another process this will fail
            src.replace(dst)
            moved_into_processing.append(dst)
        except FileNotFoundError:
            logger.warning("File disappeared before processing (skipping): %s", src)
        except Exception:
            logger.exception("Failed to move %s into processing; skipping", src)

    if not moved_into_processing:
        logger.info("No files moved into processing; nothing to inspect")
        return moved, inspected

    # Submit inspect tasks for files under _processing
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_inspect_fit, str(f)): f for f in moved_into_processing}

        for fut in as_completed(futures):
            proc_path = futures[fut]
            try:
                path_str, action, reason = fut.result()
            except Exception as e:
                logger.exception("Worker crashed inspecting %s: %s", proc_path, e)
                # Attempt to move file back to original folder so it can be retried
                try:
                    dest_back = fit_folder / proc_path.name
                    proc_path.replace(dest_back)
                except Exception:
                    logger.exception("Failed to move %s back after worker crash", proc_path)
                continue

            inspected += 1
            try:
                if action == 'move':
                    try:
                        dest = junk_dir / Path(path_str).name
                        Path(path_str).replace(dest)
                        moved += 1
                        logger.info("Moved non-activity file %s -> %s (type=%s)", path_str, dest, reason)
                    except Exception:
                        logger.exception("Failed to move %s to _junk", path_str)
                elif action == 'error':
                    logger.warning("Error inspecting %s: %s; moving back to folder", path_str, reason)
                    try:
                        dest_back = fit_folder / Path(path_str).name
                        Path(path_str).replace(dest_back)
                    except Exception:
                        logger.exception("Failed to move %s back after error", path_str)
                else:
                    # keep -> move back to original folder
                    try:
                        dest_back = fit_folder / Path(path_str).name
                        Path(path_str).replace(dest_back)
                        logger.debug("Keeping activity file %s (reason=%s)", path_str, reason)
                    except Exception:
                        logger.exception("Failed to move %s back to folder", path_str)
            except Exception:
                logger.exception("Unexpected error handling inspected file %s", path_str)

    logger.info("Pre-sweep complete: inspected=%s moved_to_junk=%s", inspected, moved)
    return moved, inspected
