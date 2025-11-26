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
from typing import Tuple, List, Dict, Any
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

from fitparse import FitFile, FitParseError
from tqdm import tqdm


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
            try:
                sessions = list(fit.get_messages("session"))
                if sessions:
                    session = sessions[0]
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


def pre_sweep_move_junk(fit_folder: Path, workers: int | None = None) -> Dict[str, Any]:
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
    Dict[str, Any]
        A summary of the operation.
    """
    fit_folder = Path(fit_folder)
    if not fit_folder.exists():
        logger.critical("FIT folder does not exist: %s", fit_folder)
        return {"inspected": 0, "moved": 0, "errors": 0}

    junk_dir = fit_folder / "_junk"
    junk_dir.mkdir(parents=True, exist_ok=True)

    processing_dir = fit_folder / "_processing"
    processing_dir.mkdir(exist_ok=True)

    fits_to_process: List[Path] = sorted(fit_folder.glob("*.fit")) + sorted(fit_folder.glob("*.FIT"))
    # Also include files that were moved to _processing in a previous run
    fits_to_process += sorted(processing_dir.glob("*.fit")) + sorted(processing_dir.glob("*.FIT"))

    if not fits_to_process:
        return {"inspected": 0, "moved": 0, "errors": 0}

    inspected = 0
    moved = 0
    errors = 0

    with tqdm(total=len(fits_to_process), desc="Inspecting FIT files") as pbar:
        for fit_path in fits_to_process:
            if not fit_path.exists():
                logger.warning(f"File disappeared before processing (skipping): {fit_path}")
                pbar.update(1)
                continue

            # Move file to _processing to avoid clashes
            processing_path = processing_dir / fit_path.name
            try:
                fit_path.replace(processing_path)
            except (FileNotFoundError, PermissionError) as e:
                logger.error(f"Could not move {fit_path.name} to _processing folder, skipping: {e}")
                errors += 1
                pbar.update(1)
                continue

            try:
                path_str, action, reason = _inspect_fit(str(processing_path))
                inspected += 1

                if action == 'move':
                    dest = junk_dir / Path(path_str).name
                    try:
                        Path(path_str).replace(dest)
                        moved += 1
                    except FileNotFoundError:
                        logger.error(f"File {Path(path_str).name} was moved or deleted during inspection.")
                        errors += 1
                    except Exception:
                        logger.exception("Failed to move %s to _junk", path_str)
                        errors += 1
                elif action == 'error':
                    errors += 1
                    logger.warning("Error inspecting %s: %s", path_str, reason)
                    # Move back to main folder if it's an error
                    dest_back = fit_folder / Path(path_str).name
                    try:
                        Path(path_str).replace(dest_back)
                    except Exception:
                        logger.exception(f"Failed to move {Path(path_str).name} back after error")

                else:  # Keep
                    dest_back = fit_folder / Path(path_str).name
                    try:
                        Path(path_str).replace(dest_back)
                    except Exception:
                        logger.exception(f"Failed to move {Path(path_str).name} back to main folder")


            except Exception as e:
                logger.exception("A critical error occurred inspecting %s: %s", processing_path.name, e)
                errors += 1
                # Try to move it back to the main folder
                dest_back = fit_folder / processing_path.name
                try:
                    if processing_path.exists():
                        processing_path.replace(dest_back)
                except Exception:
                    logger.exception(f"Failed to move {processing_path.name} back to main folder after critical error")

            pbar.update(1)

    # Cleanup: move any remaining files from _processing back to the main folder
    for remaining_file in processing_dir.glob("*.fit"):
        logger.warning(f"Moving orphaned file {remaining_file.name} back to main directory.")
        dest_back = fit_folder / remaining_file.name
        try:
            remaining_file.replace(dest_back)
        except Exception:
            logger.exception(f"Failed to move orphaned file {remaining_file.name} back.")


    return {"inspected": inspected, "moved": moved, "errors": errors}

