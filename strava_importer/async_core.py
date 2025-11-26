"""Async uploader for FIT files to Strava using aiohttp."""
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any

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
            logger.warning(f"File disappeared before it could be moved to _failed: {fit_path.name}")
            return

        failed_dir = self.config.fit_folder / "_failed"
        failed_dir.mkdir(exist_ok=True)
        try:
            dest = failed_dir / fit_path.name
            fit_path.replace(dest)
            logger.debug(f"Moved failed file to {dest}")
        except FileNotFoundError:
            logger.warning(f"File disappeared before it could be moved to _failed: {fit_path.name}")
        except Exception:
            logger.exception(f"Failed to move {fit_path} to failed folder")

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
            try:
                self.processed.append({"file": str(fit_path), "status": "created", "upload_id": upload_id, "activity_id": activity_id})
            except Exception:
                pass
            if fit_path.exists():
                try:
                    fit_path.unlink()
                except Exception:
                    logger.exception("Failed to delete file after success: %s", fit_path)
        elif "duplicate" in str(status).lower():
            self.upload_stats["duplicate"] += 1
            try:
                self.processed.append({"file": str(fit_path), "status": "duplicate", "upload_id": upload_id, "activity_id": activity_id})
            except Exception:
                pass
            if fit_path.exists():
                try:
                    fit_path.unlink()
                except Exception:
                    logger.exception("Failed to delete duplicate file %s", fit_path)
        else:
            logger.warning(f"Upload of {fit_path.name} failed with status: {final_status.get('status')}")
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
                logger.warning("Poller not available for upload_id %s", body.get("id"))
        elif status_code == 409:  # Duplicate
            self.upload_stats["duplicate"] += 1
            if fit_path.exists():
                fit_path.unlink()
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
            logger.warning("Upload returned 429 for %s. Retry-After=%s", fit_path.name, ra)
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
            logger.warning(f"File disappeared before upload: {fit_path.name}")
            if self._pbar:
                self._pbar.update(1)
            return

        try:
            await self.limiter.acquire()
            token = self.auth.ensure_token()
            headers = {"Authorization": f"Bearer {token}"}
            
            # Read file content into memory and close the handle immediately
            try:
                with fit_path.open("rb") as f:
                    fit_content = f.read()
            except FileNotFoundError:
                logger.warning(f"File disappeared before upload: {fit_path.name}")
                if self._pbar:
                    self._pbar.update(1)
                return

            data = aiohttp.FormData()
            data.add_field("data_type", "fit")
            data.add_field("file", fit_content, filename=fit_path.name, content_type="application/octet-stream")

            resp = await session.post(self.UPLOAD_URL, headers=headers, data=data)
            
            # Parse response and convert to normalized dict for handler
            resp_dict = {
                "status_code": resp.status,
                "headers": resp.headers,
                "body": await resp.json() if resp.status == 201 else {},
            }
            
            # Update rate limits from response headers
            self.limiter.update_limits(dict(resp.headers))
            
            retry_needed = await self._handle_upload_response(resp_dict, fit_path)
            if retry_needed:
                self.upload_stats["retries"] += 1
                await queue.put(fit_path)  # Re-add to the queue for retry
            else:
                if self._pbar:
                    self._pbar.update(1)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Upload of {fit_path.name} failed: {e}")
            self.upload_stats["failed"] += 1
            await self._move_to_failed(fit_path)
            if self._pbar:
                self._pbar.update(1)
        except Exception:
            logger.exception(f"An unexpected error occurred while uploading {fit_path.name}")
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
        fits_to_upload: List[Path] = sorted(list(folder.glob("*.fit")) + list(folder.glob("*.FIT")))
        fits_to_upload = [f for f in fits_to_upload if f.parent.name not in ("_junk", "_failed", "_processing")]

        if not fits_to_upload:
            print("No new FIT files to upload.")
            return

        self.upload_stats["total"] = len(fits_to_upload)
        print(f"Found {len(fits_to_upload)} FIT files to upload.")

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
        import csv
        from datetime import datetime

        print("\n--- Upload Report ---")
        print(f"  Total files to process: {self.upload_stats['total']}")
        print(f"  Successfully uploaded: {self.upload_stats['success']}")
        print(f"  Duplicates (already on Strava): {self.upload_stats['duplicate']}")
        print(f"  Failed uploads: {self.upload_stats['failed']}")
        if self.upload_stats["retries"] > 0:
            print(f"  Files re-queued due to rate limits: {self.upload_stats['retries']}")
        print("---------------------\n")

        # Write after-action report (JSON + CSV)
        try:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            json_file = f"after_action_report_{ts}.json"
            csv_file = f"after_action_report_{ts}.csv"

            with open(json_file, "w", encoding="utf-8") as jf:
                json.dump({"summary": self.upload_stats, "processed": self.processed}, jf, ensure_ascii=False, indent=2)

            # CSV: file,status,upload_id,activity_id,reason
            with open(csv_file, "w", encoding="utf-8", newline="") as cf:
                writer = csv.writer(cf)
                writer.writerow(["file", "status", "upload_id", "activity_id", "reason"])
                for entry in self.processed:
                    writer.writerow([
                        entry.get("file"),
                        entry.get("status"),
                        entry.get("upload_id"),
                        entry.get("activity_id"),
                        entry.get("reason", ""),
                    ])

            print(f"After-action report written: {json_file}, {csv_file}")
        except Exception:
            logger.exception("Failed to write after-action report")

    def run(self, max_concurrent: int = 5):
        """Runs the uploader synchronously with configurable concurrency."""
        try:
            asyncio.run(self.run_async(max_concurrent=max_concurrent))
        except KeyboardInterrupt:
            print("\nUpload cancelled by user.")
