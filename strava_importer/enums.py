"""Enumerations used by the uploader.

This module contains small enums describing upload outcomes; they are
used for categorizing results in higher-level logic.
"""
from enum import Enum, auto


class UploadResult(Enum):
    """High-level upload results used by the application."""
    SUCCESS = auto()
    DUPLICATE = auto()
    RATE_LIMITED = auto()
    SERVER_ERROR = auto()
    FAILED = auto()