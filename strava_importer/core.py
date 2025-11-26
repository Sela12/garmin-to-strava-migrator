import logging
import time
import shutil
from pathlib import Path
from typing import Callable

import requests

from .config import AppConfig
from .auth import StravaAuth
from .limiter import request_with_retries, RateLimiter

logger = logging.getLogger(__name__)


class StravaUploader:
	"""Uploads FIT files from a folder to Strava using OAuth tokens.

	The uploader handles common edge cases: duplicate uploads (409), polling
	for final processing status, and moving permanently failed files to a
	``_failed`` directory.
	"""

	UPLOAD_URL = "https://www.strava.com/api/v3/uploads"
	UPLOAD_STATUS_URL = "https://www.strava.com/api/v3/uploads/{upload_id}"

	def __init__(self, config: AppConfig, daily_limit: int = 2000, window_limit: int = 200) -> None:
		"""Create a new uploader.

		Parameters
		----------
		config: AppConfig
			Application configuration containing credentials and paths.
		daily_limit: int
			Max uploads per day (approximate safety limit).
		window_limit: int
			Max uploads per short window (to avoid 429s).
		"""
		self.config = config
		token_file = config.token_file or (Path.cwd() / ".strava_tokens.json")
		self.auth = StravaAuth(config.client_id, config.client_secret, config.auth_code, token_file=token_file)
		self.session = requests.Session()
		# wrap session.request with retry/backoff logic
		self.request: Callable[..., requests.Response] = request_with_retries(self.session.request)
		self.limiter = RateLimiter(daily_limit=daily_limit, window_limit=window_limit)

	def _move_to_failed(self, fit_path: Path) -> None:
		failed_dir = Path(self.config.fit_folder) / "_failed"
		failed_dir.mkdir(parents=True, exist_ok=True)
		try:
			dest = failed_dir / fit_path.name
			shutil.move(str(fit_path), str(dest))
			logger.info("Moved failed file %s -> %s", fit_path, dest)
		except Exception:
			logger.exception("Failed to move %s to failed folder", fit_path)

	def _poll_upload_status(self, upload_id: int, max_wait: int = 300, interval: float = 5.0) -> dict:
		"""Poll Strava's upload status until activity_id is present or an error is returned.

		Returns the final JSON response from the upload status endpoint.
		"""
		token = self.auth.ensure_token()
		headers = {"Authorization": f"Bearer {token}"}
		deadline = time.time() + max_wait
		attempt = 0

		while time.time() < deadline:
			attempt += 1
			try:
				resp = self.request("GET", self.UPLOAD_STATUS_URL.format(upload_id=upload_id), headers=headers, timeout=30)
				if resp.status_code == 401:
					logger.info("Upload status unauthorized; refreshing token and retrying")
					self.auth.refresh()
					token = self.auth.ensure_token()
					headers = {"Authorization": f"Bearer {token}"}
					resp = self.request("GET", self.UPLOAD_STATUS_URL.format(upload_id=upload_id), headers=headers, timeout=30)

				# if 200, parse
				if resp.status_code == 200:
					data = resp.json()
					# If activity_id present, success
					if data.get("activity_id"):
						logger.info("Upload %s processed: activity_id=%s", upload_id, data.get("activity_id"))
						return data
					# If error or status indicates duplicate/failed, return it
					status = data.get("status") or ""
					if status and ("duplicate" in status.lower() or "failed" in status.lower() or data.get("error")):
						logger.info("Upload %s returned status: %s", upload_id, status)
						return data
					# Still processing
					logger.debug("Upload %s still processing (attempt %s): %s", upload_id, attempt, status)
				elif 500 <= resp.status_code < 600:
					logger.warning("Server error when polling upload %s: %s", upload_id, resp.status_code)
				else:
					logger.warning("Unexpected status when polling upload %s: %s", upload_id, resp.status_code)

			except requests.RequestException:
				logger.exception("Exception while polling upload status %s (attempt %s)", upload_id, attempt)

			time.sleep(interval)

		logger.warning("Upload %s polling timed out after %s seconds", upload_id, max_wait)
		return {"id": upload_id, "status": "timed_out", "activity_id": None}

	def _upload_single(self, fit_path: Path) -> dict | None:
		token = self.auth.ensure_token()
		headers = {"Authorization": f"Bearer {token}"}
		data = {"data_type": "fit"}

		self.limiter.wait_if_needed()
		try:
			with fit_path.open("rb") as fh:
				files = {"file": (fit_path.name, fh, "application/octet-stream")}
				resp = self.request("POST", self.UPLOAD_URL, headers=headers, files=files, data=data, timeout=120)

				# If unauthorized, try refreshing token and retry once
				if resp.status_code == 401:
					logger.info("Access token unauthorized; refreshing and retrying")
					self.auth.refresh()
					token = self.auth.ensure_token()
					headers = {"Authorization": f"Bearer {token}"}
					resp = self.request("POST", self.UPLOAD_URL, headers=headers, files=files, data=data, timeout=120)

				# Handle duplicates (409) specially: treat as duplicate and delete file locally
				if resp.status_code == 409:
					try:
						result = resp.json()
					except Exception:
						result = {"status": "duplicate", "error": None}
					logger.info("Upload returned 409 (duplicate) for %s: %s", fit_path.name, result)
					self.limiter.record_request()
					return {"status": "duplicate", "activity_id": None, "upload_id": result.get("id")}

				# For other error codes raise (will be retried by wrapper) or handled below
				resp.raise_for_status()

				result = resp.json()
				logger.info("Uploaded %s -> upload response: %s", fit_path.name, result)
				self.limiter.record_request()

				upload_id = result.get("id")
				# If Strava immediately provides activity_id, we're done
				if result.get("activity_id"):
					return {"status": "created", "activity_id": result.get("activity_id"), "upload_id": upload_id}

				# Otherwise poll for processing completion
				if upload_id:
					final = self._poll_upload_status(upload_id)
					return final

				# If no upload id returned, return the raw result
				return result

		except requests.RequestException as e:
			logger.exception("Failed to upload %s: %s", fit_path, e)
			return None

	def run(self) -> None:
		folder = Path(self.config.fit_folder)
		if not folder.exists():
			logger.critical("FIT folder does not exist: %s", folder)
			return

		# Only consider files directly in the FIT folder (no subfolders)
		fits = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".fit"]
		# Exclude any files that are in special folders or explicitly quarantined
		fits = [f for f in fits if "_junk" not in f.parts and "_failed" not in f.parts]
		if not fits:
			logger.info("No FIT files found in %s", folder)
			return

		for fit in fits:
			logger.info("Processing %s", fit)
			res = self._upload_single(fit)

			# res can be None (network error), a dict with processing info, or direct API result
			if res is None:
				logger.warning("Upload failed for %s; keeping file in folder", fit)
				continue

			# Interpret the final response
			status = res.get("status") if isinstance(res, dict) else None
			activity_id = res.get("activity_id") if isinstance(res, dict) else None
			upload_id = res.get("id") or res.get("upload_id") if isinstance(res, dict) else None

			# If activity created, delete file
			if activity_id:
				logger.info("Activity created for %s: %s. Deleting file.", fit, activity_id)
				try:
					fit.unlink()
					logger.info("Deleted %s", fit)
				except Exception as e:
					logger.exception("Failed to delete %s: %s", fit, e)
				continue

			# If duplicate, delete locally (we assume activity exists already)
			if status and "duplicate" in str(status).lower():
				logger.info("Upload marked duplicate for %s (upload_id=%s). Deleting file.", fit, upload_id)
				try:
					fit.unlink()
					logger.info("Deleted duplicate %s", fit)
				except Exception as e:
					logger.exception("Failed to delete duplicate %s: %s", fit, e)
				continue

			# If we timed out or failed processing, quarantine the file
			if status == "timed_out" or (res.get("error") or (status and "failed" in str(status).lower())):
				logger.warning("Upload for %s failed or timed out. Moving to failed folder.", fit)
				self._move_to_failed(fit)
				continue

			# Unknown final state: keep file and log details
			logger.warning("Upload for %s returned unhandled state: %s. Keeping file for manual inspection.", fit, res)
