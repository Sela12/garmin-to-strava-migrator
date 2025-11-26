"""Async uploader for FIT files to Strava using aiohttp."""
import asyncio
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import time

import aiohttp
from tqdm.asyncio import tqdm

from .config import AppConfig
from .auth import StravaAuth
from .limiter import AsyncRateLimiter

logger = logging.getLogger(__name__)


class AsyncStravaUploader:
    """Async uploader for FIT files to Strava using OAuth tokens."""

    UPLOAD_URL = "https://www.strava.com/api/v3/uploads"
    UPLOAD_STATUS_URL = "https://www.strava.com/api/v3/uploads/{upload_id}"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        token_file = config.token_file or (Path.cwd() / ".strava_tokens.json")
        self.auth = StravaAuth(config.client_id, config.client_secret, config.auth_code, token_file=token_file)
        self.limiter = AsyncRateLimiter()
        self.upload_stats = {"total": 0, "success": 0, "duplicate": 0, "failed": 0}

    async def _move_to_failed(self, fit_path: Path):
        """Move a failed upload to the _failed folder."""
        await asyncio.sleep(0.1)  # Small delay to allow OS to release file handle
        if not fit_path.exists():
            return

        failed_dir = self.config.fit_folder / "_failed"
        failed_dir.mkdir(exist_ok=True)
        try:
            dest = failed_dir / fit_path.name
            fit_path.replace(dest)
            logger.debug(f"Moved failed file to {dest}")
        except Exception:
            logger.exception(f"Failed to move {fit_path} to failed folder")

    async def _poll_upload_status(self, session: aiohttp.ClientSession, upload_id: int) -> Dict[str, Any]:
        """Polls for upload completion."""
        token = self.auth.ensure_token()
        headers = {"Authorization": f"Bearer {token}"}
        for _ in range(10):  # Poll for up to 50 seconds
            async with session.get(self.UPLOAD_STATUS_URL.format(upload_id=upload_id), headers=headers) as resp:
                self.limiter.update_limits(resp.headers)
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("activity_id"):
                        return data
                    if "error" in data or "duplicate" in data.get("status", ""):
                        return data
                await asyncio.sleep(5)
        return {"id": upload_id, "status": "timed_out"}

    async def _upload_single(self, session: aiohttp.ClientSession, fit_path: Path, pbar: tqdm) -> None:
        """Uploads a single file with retries and handles responses."""
        f = None
        try:
            await self.limiter.acquire()
            token = self.auth.ensure_token()
            headers = {"Authorization": f"Bearer {token}"}
            data = aiohttp.FormData()
            data.add_field("data_type", "fit")

            f = fit_path.open("rb")
            data.add_field("file", f, filename=fit_path.name, content_type="application/octet-stream")
            async with session.post(self.UPLOAD_URL, headers=headers, data=data) as resp:
                self.limiter.update_limits(resp.headers)

                if resp.status == 201:
                    upload_data = await resp.json()
                    final_status = await self._poll_upload_status(session, upload_data["id"])
                    if final_status.get("activity_id"):
                        self.upload_stats["success"] += 1
                        fit_path.unlink()
                    elif "duplicate" in final_status.get("status", ""):
                        self.upload_stats["duplicate"] += 1
                        fit_path.unlink()
                    else:
                        self.upload_stats["failed"] += 1
                        await self._move_to_failed(fit_path)
                    pbar.update(1)
                    return

                if resp.status == 409:
                    self.upload_stats["duplicate"] += 1
                    fit_path.unlink()
                    pbar.update(1)
                    return

                if resp.status == 429:
                    pbar.set_description("Rate limited. Waiting...")
                    await self.limiter.force_backoff()
                    # We will retry the file in the next iteration of the main loop
                    return

                if resp.status >= 400:
                    logger.warning(f"Upload of {fit_path.name} failed with status {resp.status}")
                    self.upload_stats["failed"] += 1
                    await self._move_to_failed(fit_path)
                    pbar.update(1)
                    return
        
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Network error uploading {fit_path.name}: {e}")
            self.upload_stats["failed"] += 1
            await self._move_to_failed(fit_path)
            pbar.update(1)
            return
        finally:
            if f:
                f.close()


    async def run_async(self, max_concurrent: int = 5):
        """Runs the async uploader with a progress bar and summary."""
        folder = self.config.fit_folder
        fits_to_upload = list(folder.glob("*.fit")) + list(folder.glob("*.FIT"))
        fits_to_upload = [f for f in fits_to_upload if f.parent.name not in ("_junk", "_failed")]

        self.upload_stats["total"] = len(fits_to_upload)

        if not fits_to_upload:
            print("No new FIT files to upload.")
            return

        print(f"Found {len(fits_to_upload)} FIT files to upload.")
        
        async with aiohttp.ClientSession() as session:
            with tqdm(total=len(fits_to_upload), desc="Uploading FIT files") as pbar:
                while fits_to_upload:
                    tasks = [self._upload_single(session, fit, pbar) for fit in fits_to_upload]
                    await asyncio.gather(*tasks)
                    
                    # Check for files that need to be retried
                    fits_to_upload = list(folder.glob("*.fit")) + list(folder.glob("*.FIT"))
                    fits_to_upload = [f for f in fits_to_upload if f.parent.name not in ("_junk", "_failed")]
                    pbar.total = len(fits_to_upload)


        self._print_summary()

    def _print_summary(self):
        """Prints a summary of the upload session."""
        print("\n--- Upload Report ---")
        print(f"  Total files processed: {self.upload_stats['total']}")
        print(f"  Successfully uploaded: {self.upload_stats['success']}")
        print(f"  Duplicates (already on Strava): {self.upload_stats['duplicate']}")
        print(f"  Failed uploads: {self.upload_stats['failed']}")
        print("---------------------\n")

    def run(self):
        """Runs the uploader synchronously."""
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("\nUpload cancelled by user.")
