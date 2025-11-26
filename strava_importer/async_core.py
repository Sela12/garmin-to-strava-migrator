"""Async uploader for FIT files to Strava using aiohttp."""
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any

import aiohttp
from tqdm.asyncio import tqdm

from .config import AppConfig
from .auth import StravaAuth
from .limiter import AsyncRateLimiter
from .poller import UploadPoller

logger = logging.getLogger(__name__)


class AsyncStravaUploader:
    """Async uploader for FIT files to Strava using OAuth tokens."""

    UPLOAD_URL = "https://www.strava.com/api/v3/uploads"
    UPLOAD_STATUS_URL = "https://www.strava.com/api/v3/uploads/{upload_id}"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        token_file = config.token_file or (Path.cwd() / ".strava_tokens.json")
        # Create TokenStore for token persistence
        from .token_store import FileTokenStore
        token_store = FileTokenStore(Path(token_file))
        self.auth = StravaAuth(config.client_id, config.client_secret, config.auth_code, token_store=token_store)
        self.limiter = AsyncRateLimiter()
        self.upload_stats = {"total": 0, "success": 0, "duplicate": 0, "failed": 0, "retries": 0}
        # list of processed files for after-action report
        self.processed: list[dict] = []
        self._pbar: tqdm | None = None

    async def _move_to_failed(self, fit_path: Path):
        """Move a failed upload to the _failed folder."""
        if not fit_path.exists():
            # File already gone (system/antivirus moved it)
            return

        failed_dir = self.config.fit_folder / "_failed"
        failed_dir.mkdir(exist_ok=True)
        try:
            dest = failed_dir / fit_path.name
            fit_path.replace(dest)
            logger.debug(f"Moved failed file to {dest}")
        except FileNotFoundError:
            # File disappeared between check and move
            pass
        except Exception:
            logger.debug(f"Could not move {fit_path} to _failed (likely already processed)")

    async def _poll_upload_status(self, session: aiohttp.ClientSession, upload_id: int) -> Dict[str, Any]:
        """Legacy per-worker poll helper kept for compatibility.

        The uploader now uses a centralized UploadPoller; this helper is
        retained unused to avoid breaking callers that may depend on it.
        """
        return {"id": upload_id, "status": "not_polled"}

    async def _process_upload_status(self, fit_path: Path, final_status: Dict[str, Any]):
        """Process the final status of an upload and move the file accordingly."""
        # Normalize fields
        status = final_status.get("status") or ""
        upload_id = final_status.get("id") or final_status.get("upload_id")
        activity_id = final_status.get("activity_id")

        if activity_id:
            self.upload_stats["success"] += 1
            logger.info(f"✓ Upload successful: {fit_path.name} → activity_id={activity_id}, upload_id={upload_id}")
            try:
                self.processed.append({"file": str(fit_path), "status": "created", "upload_id": upload_id, "activity_id": activity_id})
            except Exception:
                pass
            # Silently try to delete; file may already be gone
            try:
                if fit_path.exists():
                    fit_path.unlink()
                    logger.debug(f"Deleted uploaded file: {fit_path.name}")
            except Exception as e:
                logger.debug(f"Could not delete {fit_path.name}: {e}")
        elif "duplicate" in str(status).lower():
            self.upload_stats["duplicate"] += 1
            logger.info(f"⊗ Duplicate detected: {fit_path.name} (upload_id={upload_id})")
            try:
                self.processed.append({"file": str(fit_path), "status": "duplicate", "upload_id": upload_id, "activity_id": activity_id})
            except Exception:
                pass
            # Silently try to delete; file may already be gone
            try:
                if fit_path.exists():
                    fit_path.unlink()
                    logger.debug(f"Deleted duplicate file: {fit_path.name}")
            except Exception as e:
                logger.debug(f"Could not delete duplicate {fit_path.name}: {e}")
        else:
            # Log all failure statuses to file only (not terminal)
            logger.info(f"✗ Upload failed: {fit_path.name} | Status: {status} | upload_id={upload_id}")
            self.upload_stats["failed"] += 1
            try:
                self.processed.append({"file": str(fit_path), "status": "failed", "upload_id": upload_id, "activity_id": activity_id, "reason": status})
            except Exception:
                pass
            await self._move_to_failed(fit_path)

    async def _handle_upload_response(
        self, resp_obj: Dict[str, Any], fit_path: Path
    ) -> bool:
        """Handles the response from the initial upload POST request. Returns True if retry is needed."""
        status_code = resp_obj.get("status_code")
        headers = resp_obj.get("headers", {})
        body = resp_obj.get("body", {})
        
        if status_code == 201:
            upload_id = body.get("id")
            # Enqueue to centralized poller for status checks
            if hasattr(self, "_poller") and self._poller is not None:
                async def _cb(fp, status):
                    # Ensure we pass a Path to the original processor
                    await self._process_upload_status(Path(fp), status)

                await self._poller.enqueue(upload_id, str(fit_path), _cb)
            else:
                # Fallback: poller should always be available, but log if not
                logger.info("Poller not available for upload_id %s", body.get("id"))
        elif status_code == 409:  # Duplicate
            self.upload_stats["duplicate"] += 1
            logger.info(f"⊗ Duplicate at upload: {fit_path.name}")
            if fit_path.exists():
                fit_path.unlink()
                logger.debug(f"Deleted duplicate: {fit_path.name}")
        elif status_code == 429:  # Rate limited
            # When rate limited, prefer to use Retry-After header if provided
            ra = headers.get("Retry-After") or headers.get("retry-after")
            ra_val = None
            if ra:
                try:
                    ra_val = float(ra)
                except Exception:
                    ra_val = None
            if self._pbar:
                self._pbar.set_description("Rate limited. Re-queueing...")
            logger.info(f"⚠ Rate limit 429 for {fit_path.name} | Retry-After: {ra_val or 'not specified'}")
            await self.limiter.force_backoff(ra_val)
            return True  # Retry
        else:
            # Unexpected status code
            if status_code and status_code >= 400:
                logger.error(f"Upload failed with status {status_code} for {fit_path.name}")
        return False

    async def _upload_single(
        self, fit_path: Path, queue: asyncio.Queue, session: aiohttp.ClientSession
    ):
        """Uploads a single file and re-queues on rate limit."""
        if not fit_path.exists():
            logger.info(f"File disappeared before upload: {fit_path.name}")
            if self._pbar:
                self._pbar.update(1)
            return

        try:
            await self.limiter.acquire()
            token = self.auth.ensure_token()
            headers = {"Authorization": f"Bearer {token}"}
            
            logger.info(f"→ Uploading: {fit_path.name}")
            
            # Read file content into memory and close the handle immediately
            try:
                with fit_path.open("rb") as f:
                    fit_content = f.read()
                logger.debug(f"Read {len(fit_content)} bytes from {fit_path.name}")
            except FileNotFoundError:
                logger.info(f"File disappeared before upload: {fit_path.name}")
                if self._pbar:
                    self._pbar.update(1)
                return

            data = aiohttp.FormData()
            data.add_field("data_type", "fit")
            data.add_field("file", fit_content, filename=fit_path.name, content_type="application/octet-stream")

            # Add 60-second timeout to prevent stuck uploads
            timeout = aiohttp.ClientTimeout(total=60)
            resp = await session.post(self.UPLOAD_URL, headers=headers, data=data, timeout=timeout)
            
            logger.info(f"← Response for {fit_path.name}: HTTP {resp.status}")
            
            # Parse response and convert to normalized dict for handler
            resp_dict = {
                "status_code": resp.status,
                "headers": resp.headers,
                "body": await resp.json() if resp.status == 201 else {},
            }
            
            if resp.status == 201 and resp_dict["body"]:
                logger.debug(f"Upload response body for {fit_path.name}: {resp_dict['body']}")
            
            # Update rate limits from response headers
            self.limiter.update_limits(dict(resp.headers))
            
            retry_needed = await self._handle_upload_response(resp_dict, fit_path)
            if retry_needed:
                self.upload_stats["retries"] += 1
                logger.info(f"↻ Re-queuing {fit_path.name} for retry (rate limited)")
                await queue.put(fit_path)  # Re-add to the queue for retry
            else:
                if self._pbar:
                    self._pbar.update(1)

        except asyncio.TimeoutError:
            logger.error(f"✗ Upload timeout (60s) for {fit_path.name}")
            self.upload_stats["failed"] += 1
            await self._move_to_failed(fit_path)
            if self._pbar:
                self._pbar.update(1)
        except aiohttp.ClientError as e:
            logger.error(f"✗ Network error uploading {fit_path.name}: {e}")
            self.upload_stats["failed"] += 1
            await self._move_to_failed(fit_path)
            if self._pbar:
                self._pbar.update(1)
        except Exception as e:
            logger.error(f"✗ Unexpected error uploading {fit_path.name}: {e}")
            self.upload_stats["failed"] += 1
            await self._move_to_failed(fit_path)
            if self._pbar:
                self._pbar.update(1)


    async def _worker(self, name: str, queue: asyncio.Queue, session: aiohttp.ClientSession):
        """Worker task that consumes from the queue."""
        while True:
            fit_path = await queue.get()
            try:
                await self._upload_single(fit_path, queue, session)
            finally:
                queue.task_done()

    def _setup_folders(self):
        """Create necessary subdirectories if they don't exist."""
        (self.config.fit_folder / "_failed").mkdir(exist_ok=True)
        (self.config.fit_folder / "_junk").mkdir(exist_ok=True)
        (self.config.fit_folder / "_processing").mkdir(exist_ok=True)

    async def run_async(self, max_concurrent: int = 5):
        """Runs the async uploader with a queue and worker pattern."""
        folder = self.config.fit_folder
        
        # Get all FIT files, filtering out special directories
        all_fits = sorted(list(folder.glob("*.fit")) + list(folder.glob("*.FIT")))
        fits_to_upload = [f for f in all_fits if f.exists() and f.parent == folder]
        
        # Double-check: exclude files in _junk, _failed, _processing subdirs
        fits_to_upload = [f for f in fits_to_upload if f.parent.name not in ("_junk", "_failed", "_processing")]

        if not fits_to_upload:
            print("No new FIT files to upload.")
            logger.info("No FIT files found in main directory")
            return

        self.upload_stats["total"] = len(fits_to_upload)
        print(f"Found {len(fits_to_upload)} FIT files to upload.")
        logger.info(f"Starting upload session: {len(fits_to_upload)} files queued")

        queue: asyncio.Queue[Path] = asyncio.Queue()
        for fit in fits_to_upload:
            await queue.put(fit)

        async with aiohttp.ClientSession() as session:
            # Start centralized poller
            poller = UploadPoller(self.auth, self.limiter, session, self.UPLOAD_STATUS_URL)
            self._poller = poller
            poller.start()

            with tqdm(total=len(fits_to_upload), desc="Uploading FIT files") as pbar:
                self._pbar = pbar
                tasks = [
                    asyncio.create_task(self._worker(f"worker-{i}", queue, session))
                    for i in range(min(max_concurrent, len(fits_to_upload)))
                ]

                await queue.join()  # Wait for all files to be processed

                # Wait for poller to finish processing any pending status checks
                await poller.stop()

                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        self._print_summary()

    def _print_summary(self):
        """Prints a summary of the upload session."""
        import json
        from datetime import datetime

        print("\n--- Upload Report ---")
        print(f"  Total files to process: {self.upload_stats['total']}")
        print(f"  Successfully uploaded: {self.upload_stats['success']}")
        print(f"  Duplicates (already on Strava): {self.upload_stats['duplicate']}")
        print(f"  Failed uploads: {self.upload_stats['failed']}")
        if self.upload_stats["retries"] > 0:
            print(f"  Files re-queued due to rate limits: {self.upload_stats['retries']}")
        print("---------------------\n")

        # Append to single after-action report JSON file
        try:
            json_file = "upload_history.json"
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            
            # Create new report entry
            report_entry = {
                "timestamp": ts,
                "summary": self.upload_stats,
                "processed": self.processed
            }
            
            # Load existing reports or create new list
            try:
                with open(json_file, "r", encoding="utf-8") as jf:
                    history = json.load(jf)
                    if not isinstance(history, list):
                        history = []
            except (FileNotFoundError, json.JSONDecodeError):
                history = []
            
            # Append new report
            history.append(report_entry)
            
            # Write back to file
            with open(json_file, "w", encoding="utf-8") as jf:
                json.dump(history, jf, ensure_ascii=False, indent=2)

            print(f"After-action report appended to: {json_file}")
        except Exception:
            logger.exception("Failed to write after-action report")

    def run(self, max_concurrent: int = 5):
        """Runs the uploader synchronously with configurable concurrency."""
        try:
            asyncio.run(self.run_async(max_concurrent=max_concurrent))
        except KeyboardInterrupt:
            print("\nUpload cancelled by user.")
            # Print summary even when interrupted
            self._print_summary()
