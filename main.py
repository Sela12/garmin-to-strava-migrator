import logging
import sys
from pathlib import Path
from os import getenv

from dotenv import load_dotenv
from strava_importer.config import AppConfig
from strava_importer.core import StravaUploader
from strava_importer.cleaner import pre_sweep_move_junk

# Load environment from .env (if present)
load_dotenv()

# --- CONFIGURATION (from environment) ---
CLIENT_ID = getenv('CLIENT_ID')
CLIENT_SECRET = getenv('CLIENT_SECRET')
AUTH_CODE = getenv('AUTH_CODE')
FIT_FOLDER = getenv('FIT_FOLDER')

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler("strava_upload.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main() -> None:
    setup_logging()
    # Validate configuration
    if not CLIENT_ID or not CLIENT_SECRET or not AUTH_CODE:
        logging.critical("Missing Strava credentials. Please set CLIENT_ID, CLIENT_SECRET and AUTH_CODE in your environment or .env file.")
        return

    config = AppConfig(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        auth_code=AUTH_CODE,
        fit_folder=Path(FIT_FOLDER)
    )

    # Run a pre-sweep to move non-activity files to `_junk` to save API quota
    if not config.fit_folder or not config.fit_folder.exists():
        logging.critical(f"FIT_FOLDER is missing or does not exist: {config.fit_folder}")
        return

    try:
        moved, inspected = pre_sweep_move_junk(config.fit_folder)
        logging.info(f"Pre-sweep moved {moved} non-activity files out of {inspected} inspected")
    except Exception:
        logging.exception("Pre-sweep failed; continuing to upload existing files")

    try:
        uploader = StravaUploader(config)
        uploader.run()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        logging.critical(f"Fatal Error: {e}")

if __name__ == "__main__":
    main()