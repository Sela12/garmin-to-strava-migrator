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

    Files are inspected IN-PLACE to avoid race conditions from multiple
    move operations happening simultaneously across processes.

    Parameters
    ----------
    fit_folder: Path
        Path to the folder containing .fit files to inspect.
    workers: Optional[int]
        Unused (kept for backward compatibility).

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

    fits_to_process: List[Path] = sorted(fit_folder.glob("*.fit")) + sorted(fit_folder.glob("*.FIT"))

    if not fits_to_process:
        return {"inspected": 0, "moved": 0, "errors": 0}

    inspected = 0
    moved = 0
    errors = 0

    with tqdm(total=len(fits_to_process), desc="Inspecting FIT files") as pbar:
        for fit_path in fits_to_process:
            if not fit_path.exists():
                # File likely moved by system/antivirus; silently skip
                pbar.update(1)
                continue

            try:
                # Inspect file IN-PLACE (no intermediate moves)
                path_str, action, reason = _inspect_fit(str(fit_path))
                inspected += 1

                if action == 'move':
                    # Move junk files directly to _junk
                    dest = junk_dir / fit_path.name
                    try:
                        fit_path.replace(dest)
                        moved += 1
                    except FileNotFoundError:
                        # File disappeared; skip silently
                        pass
                    except Exception as e:
                        logger.debug(f"Could not move {fit_path.name} to _junk: {e}")
                        errors += 1
                elif action == 'error':
                    # Could not parse file; leave it for upload (safer)
                    logger.debug(f"Could not inspect {fit_path.name}: {reason}")
                    errors += 1
                # else: 'keep' - do nothing, file stays in main folder

            except Exception as e:
                logger.debug(f"Error processing {fit_path.name}: {e}")
                errors += 1

            pbar.update(1)

    return {"inspected": inspected, "moved": moved, "errors": errors}

