import sys
from os import getenv
from pathlib import Path

from dotenv import load_dotenv

from strava_importer.config import AppConfig
from strava_importer.async_core import AsyncStravaUploader
from strava_importer.cleaner import pre_sweep_move_junk
from strava_importer.utils import configure_logging

# Load environment from .env (if present)
load_dotenv(encoding="utf-8")

# --- CONFIGURATION (from environment) ---
CLIENT_ID = getenv("CLIENT_ID")
CLIENT_SECRET = getenv("CLIENT_SECRET")
AUTH_CODE = getenv("AUTH_CODE")
FIT_FOLDER = getenv("FIT_FOLDER")
MAX_CONCURRENT = int(getenv("MAX_CONCURRENT", "5"))


def main() -> None:
    # configure logging (truncate previous log to keep tidy)
    configure_logging("strava_upload.log", truncate=True)

    # Validate configuration
    import logging

    if not CLIENT_ID or not CLIENT_SECRET or not AUTH_CODE:
        logging.critical(
            "Missing Strava credentials. Please set CLIENT_ID, CLIENT_SECRET and AUTH_CODE in your environment or .env file."
        )
        return

    if not FIT_FOLDER:
        logging.critical("Missing FIT_FOLDER in environment or .env. Please set FIT_FOLDER to your FIT files folder.")
        return

    config = AppConfig(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        auth_code=AUTH_CODE,
        fit_folder=Path(FIT_FOLDER),
    )

    # Run a pre-sweep to move non-activity files to `_junk` to save API quota
    if not config.fit_folder or not config.fit_folder.exists():
        logging.critical(f"FIT_FOLDER is missing or does not exist: {config.fit_folder}")
        return

    try:
        pre_sweep_summary = pre_sweep_move_junk(config.fit_folder)
        print("\n--- Pre-sweep Report ---")
        print(f"  Inspected: {pre_sweep_summary['inspected']}")
        print(f"  Moved to _junk: {pre_sweep_summary['moved']}")
        if pre_sweep_summary["errors"] > 0:
            print(f"  Errors: {pre_sweep_summary['errors']}")
        print("------------------------\n")
    except Exception:
        logging.exception("Pre-sweep failed; continuing to upload existing files")

    try:
        # Run async uploader with configured concurrency
        uploader = AsyncStravaUploader(config)
        uploader.run(max_concurrent=MAX_CONCURRENT)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        logging.critical(f"Fatal Error: {e}")


if __name__ == "__main__":
    main()