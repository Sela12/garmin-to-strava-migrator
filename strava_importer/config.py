from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class AppConfig:
	"""Application configuration for the Strava uploader.

	This is a minimal config dataclass used by `main.py` and other modules.
	"""
	client_id: str
	client_secret: str
	auth_code: str
	fit_folder: Path
	token_file: Optional[Path] = None
