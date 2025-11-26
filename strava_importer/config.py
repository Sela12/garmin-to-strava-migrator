from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppConfig:
	"""Application configuration for the Strava uploader.

	Attributes
	----------
	client_id: str
		Strava application client id.
	client_secret: str
		Strava application client secret.
	auth_code: str
		One-time OAuth authorization code (exchanged for tokens).
	fit_folder: Path
		Folder containing `.fit` files to import.
	token_file: Path | None
		Optional path to persist OAuth tokens (access/refresh).
	"""
	client_id: str
	client_secret: str
	auth_code: str
	fit_folder: Path
	token_file: Path | None = None
