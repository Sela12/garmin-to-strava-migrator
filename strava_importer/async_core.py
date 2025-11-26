"""Async uploader for FIT files to Strava using aiohttp."""
import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp

from .config import AppConfig
from .auth import StravaAuth
from .limiter import RateLimiter

logger = logging.getLogger(__name__)


class AsyncStravaUploader:
    """Async uploader for FIT files to Strava using OAuth tokens.

    Handles concurrent uploads, duplicate detection (409), polling for
    processing status, and quarantine of failed files to a ``_failed``
    directory.
    """

    UPLOAD_URL = "https://www.strava.com/api/v3/uploads"
    UPLOAD_STATUS_URL = "https://www.strava.com/api/v3/uploads/{upload_id}"

    def __init__(self, config: AppConfig, daily_limit: int = 2000, window_limit: int = 200) -> None:
        """Create a new async uploader.

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
        self.limiter = RateLimiter(daily_limit=daily_limit, window_limit=window_limit)

    def _move_to_failed(self, fit_path: Path) -> None:
        """Move a failed upload to the _failed folder."""
        failed_dir = Path(self.config.fit_folder) / "_failed"
        failed_dir.mkdir(parents=True, exist_ok=True)
        try:
            dest = failed_dir / fit_path.name
            # Atomic replace on same filesystem
            fit_path.replace(dest)
            logger.info(f"Moved failed file {fit_path} -> {dest}")
        except Exception:
            logger.exception(f"Failed to move {fit_path} to failed folder")

    async def _poll_upload_status(
        self, session: aiohttp.ClientSession, upload_id: int, max_wait: int = 300, interval: float = 5.0
    ) -> dict:
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
                async with session.get(
                    self.UPLOAD_STATUS_URL.format(upload_id=upload_id), headers=headers, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 401:
                        logger.info("Upload status unauthorized; refreshing token and retrying")
                        self.auth.refresh()
                        token = self.auth.ensure_token()
                        headers = {"Authorization": f"Bearer {token}"}
                        async with session.get(
                            self.UPLOAD_STATUS_URL.format(upload_id=upload_id), headers=headers, timeout=aiohttp.ClientTimeout(total=30)
                        ) as retry_resp:
                            if retry_resp.status == 200:
                                data = await retry_resp.json()
                                if data.get("activity_id"):
                                    logger.info(f"Upload {upload_id} processed: activity_id={data.get('activity_id')}")
                                    return data
                                status = data.get("status") or ""
                                if status and ("duplicate" in status.lower() or "failed" in status.lower() or data.get("error")):
                                    logger.info(f"Upload {upload_id} returned status: {status}")
                                    return data
                                logger.debug(f"Upload {upload_id} still processing (attempt {attempt}): {status}")
                    elif resp.status == 200:
                        data = await resp.json()
                        if data.get("activity_id"):
                            logger.info(f"Upload {upload_id} processed: activity_id={data.get('activity_id')}")
                            return data
                        status = data.get("status") or ""
                        if status and ("duplicate" in status.lower() or "failed" in status.lower() or data.get("error")):
                            logger.info(f"Upload {upload_id} returned status: {status}")
                            return data
                        logger.debug(f"Upload {upload_id} still processing (attempt {attempt}): {status}")
                    elif 500 <= resp.status < 600:
                        logger.warning(f"Server error when polling upload {upload_id}: {resp.status}")
                    else:
                        logger.warning(f"Unexpected status when polling upload {upload_id}: {resp.status}")
            except asyncio.TimeoutError:
                logger.exception(f"Timeout while polling upload {upload_id} (attempt {attempt})")
            except Exception:
                logger.exception(f"Exception while polling upload {upload_id} (attempt {attempt})")

            await asyncio.sleep(interval)

        logger.warning(f"Upload {upload_id} polling timed out after {max_wait} seconds")
        return {"id": upload_id, "status": "timed_out", "activity_id": None}

    async def _upload_single(self, session: aiohttp.ClientSession, fit_path: Path) -> Optional[dict]:
        """Upload a single FIT file to Strava."""
        token = self.auth.ensure_token()
        headers = {"Authorization": f"Bearer {token}"}
        data = aiohttp.FormData()
        data.add_field("data_type", "fit")

        self.limiter.wait_if_needed()
        try:
            with fit_path.open("rb") as fh:
                data.add_field("file", fh, filename=fit_path.name, content_type="application/octet-stream")

                async with session.post(self.UPLOAD_URL, headers=headers, data=data, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    # If unauthorized, try refreshing token and retry once
                    if resp.status == 401:
                        logger.info("Access token unauthorized; refreshing and retrying")
                        self.auth.refresh()
                        token = self.auth.ensure_token()
                        headers = {"Authorization": f"Bearer {token}"}
                        data = aiohttp.FormData()
                        data.add_field("data_type", "fit")
                        data.add_field("file", fh, filename=fit_path.name, content_type="application/octet-stream")

                        async with session.post(self.UPLOAD_URL, headers=headers, data=data, timeout=aiohttp.ClientTimeout(total=120)) as retry_resp:
                            if retry_resp.status == 409:
                                try:
                                    result = await retry_resp.json()
                                except Exception:
                                    result = {"status": "duplicate", "error": None}
                                logger.info(f"Upload returned 409 (duplicate) for {fit_path.name}: {result}")
                                self.limiter.record_request()
                                return {"status": "duplicate", "activity_id": None, "upload_id": result.get("id")}

                            if retry_resp.status >= 400:
                                logger.error(f"Retry upload failed with status {retry_resp.status}")
                                return None

                            result = await retry_resp.json()
                            logger.info(f"Uploaded {fit_path.name} -> upload response: {result}")
                            self.limiter.record_request()

                            upload_id = result.get("id")
                            if result.get("activity_id"):
                                return {"status": "created", "activity_id": result.get("activity_id"), "upload_id": upload_id}

                            if upload_id:
                                final = await self._poll_upload_status(session, upload_id)
                                return final

                            return result

                    # Handle duplicates (409) specially
                    if resp.status == 409:
                        try:
                            result = await resp.json()
                        except Exception:
                            result = {"status": "duplicate", "error": None}
                        logger.info(f"Upload returned 409 (duplicate) for {fit_path.name}: {result}")
                        self.limiter.record_request()
                        return {"status": "duplicate", "activity_id": None, "upload_id": result.get("id")}

                    if resp.status >= 400:
                        logger.error(f"Upload failed with status {resp.status}")
                        return None

                    result = await resp.json()
                    logger.info(f"Uploaded {fit_path.name} -> upload response: {result}")
                    self.limiter.record_request()

                    upload_id = result.get("id")
                    if result.get("activity_id"):
                        return {"status": "created", "activity_id": result.get("activity_id"), "upload_id": upload_id}

                    if upload_id:
                        final = await self._poll_upload_status(session, upload_id)
                        return final

                    return result

        except asyncio.TimeoutError:
            logger.exception(f"Timeout uploading {fit_path}")
            return None
        except Exception as e:
            logger.exception(f"Failed to upload {fit_path}: {e}")
            return None

    async def run_async(self, max_concurrent: int = 5) -> None:
        """Run the uploader with concurrent uploads.

        Parameters
        ----------
        max_concurrent: int
            Maximum number of concurrent uploads. Default 5 to respect rate limits.
        """
        folder = Path(self.config.fit_folder)
        if not folder.exists():
            logger.critical(f"FIT folder does not exist: {folder}")
            return

        # Only consider files directly in the FIT folder (no subfolders)
        fits = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".fit"]
        # Exclude files in special folders
        fits = [f for f in fits if "_junk" not in f.parts and "_failed" not in f.parts and "_uploading" not in f.parts]

        # To avoid race conditions where other processes might touch files while
        # we schedule uploads, atomically move candidates into an `_uploading`
        # directory and operate on those files. This prevents the same file from
        # being processed twice.
        uploading_dir = folder / "_uploading"
        uploading_dir.mkdir(parents=True, exist_ok=True)

        fits_to_upload = []
        for f in fits:
            src = f
            dst = uploading_dir / f.name
            try:
                src.replace(dst)
                fits_to_upload.append(dst)
            except FileNotFoundError:
                logger.warning("File disappeared before upload scheduling (skipping): %s", src)
            except Exception:
                logger.exception("Failed to move %s into _uploading; skipping", src)

        fits = fits_to_upload

        if not fits:
            logger.info(f"No FIT files found in {folder}")
            return

        logger.info(f"Starting async upload of {len(fits)} files with max_concurrent={max_concurrent}")

        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrent)

        async def upload_with_semaphore(session: aiohttp.ClientSession, fit: Path) -> None:
            async with semaphore:
                logger.info(f"Processing {fit}")
                res = await self._upload_single(session, fit)

                if res is None:
                    logger.warning(f"Upload failed for {fit}; keeping file in folder")
                    return

                status = res.get("status") if isinstance(res, dict) else None
                activity_id = res.get("activity_id") if isinstance(res, dict) else None
                upload_id = res.get("id") or res.get("upload_id") if isinstance(res, dict) else None

                # If activity created, delete file
                if activity_id:
                    logger.info(f"Activity created for {fit}: {activity_id}. Deleting file.")
                    try:
                        fit.unlink()
                        logger.info(f"Deleted {fit}")
                    except Exception as e:
                        logger.exception(f"Failed to delete {fit}: {e}")
                    return

                # If duplicate, delete locally
                if status and "duplicate" in str(status).lower():
                    logger.info(f"Upload marked duplicate for {fit} (upload_id={upload_id}). Deleting file.")
                    try:
                        fit.unlink()
                        logger.info(f"Deleted duplicate {fit}")
                    except Exception as e:
                        logger.exception(f"Failed to delete duplicate {fit}: {e}")
                    return

                # If timed out or failed processing, quarantine the file
                if status == "timed_out" or (res.get("error") or (status and "failed" in str(status).lower())):
                    logger.warning(f"Upload for {fit} failed or timed out. Moving to failed folder.")
                    self._move_to_failed(fit)
                    return

                # Unknown final state
                logger.warning(f"Upload for {fit} returned unhandled state: {res}. Keeping file for manual inspection.")

        connector = aiohttp.TCPConnector(limit_per_host=2, limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [upload_with_semaphore(session, fit) for fit in fits]
            await asyncio.gather(*tasks)

        logger.info("Async upload run complete")

    def run(self) -> None:
        """Run the uploader synchronously (wrapper around async run)."""
        asyncio.run(self.run_async())
