"""Utility helpers for the Strava importer project.

Currently contains logging configuration helper used by `main.py`.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def configure_logging(log_file: str = "strava_upload.log", level: int = logging.INFO, truncate: bool = True) -> None:
    """Configure root logger to write to both console and file.

    If `truncate` is True and the file exists, it will be truncated at startup.
    """
    # Ensure parent dir exists
    log_path = Path(log_file)
    if log_path.parent and not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)

    if truncate and log_path.exists():
        # truncate file
        with open(log_file, "w", encoding="utf-8"):
            pass

    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # File handler: detailed INFO level logs
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(level)

    # Console handler: minimal output, only WARNING+ messages
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.WARNING)  # Only show warnings/errors in terminal

    root.addHandler(fh)
    root.addHandler(ch)
