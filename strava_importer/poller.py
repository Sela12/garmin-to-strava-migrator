"""Centralized upload poller to reduce concurrent status checks.

This module provides an UploadPoller which accepts upload IDs and associated
fit file paths, polls the Strava upload status endpoint in a rate-limited
fashion and invokes a provided callback with the final status.

Keeping polling centralized reduces 429 storms caused by many workers
polling the same endpoint concurrently.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable, Optional, Tuple, Any

import aiohttp

from .auth import StravaAuth
from .limiter import AsyncRateLimiter

logger = logging.getLogger(__name__)


class UploadPoller:
    """Central poller for upload status checks.

    enqueue(upload_id, fit_path, callback) -> None
        - upload_id: int
        - fit_path: Path
        - callback: async callable accepting (fit_path, status_dict)
    """

    def __init__(
        self,
        auth: StravaAuth,
        limiter: AsyncRateLimiter,
        session: aiohttp.ClientSession,
        status_url_template: str,
        poll_interval: float = 2.0,
    ) -> None:
        self.auth = auth
        self.limiter = limiter
        self.session = session
        self.status_url_template = status_url_template
        self.poll_interval = poll_interval

        # fit_path and callback are intentionally loosely typed to support
        # Path or str for fit_path and different callback signatures.
        self._queue: asyncio.Queue[Tuple[int, object, Callable[[object, dict], Awaitable[Any]]]] = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping = True
        # Wait until queue is drained
        await self._queue.join()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def enqueue(self, upload_id: int, fit_path: object, callback: Callable[[object, dict], Awaitable[Any]]):
        await self._queue.put((upload_id, fit_path, callback))

    async def _run(self) -> None:
        backoff_base = self.poll_interval
        try:
            while True:
                upload_id, fit_path, callback = await self._queue.get()
                try:
                    await self._poll_and_handle(upload_id, fit_path, callback, backoff_base)
                except Exception:
                    logger.exception("Error while polling upload %s", upload_id)
                    # If polling fails hard, call callback with a timed_out status
                    try:
                        await callback(fit_path, {"id": upload_id, "status": "timed_out"})
                    except Exception:
                        logger.exception("Callback failed for %s after polling error", fit_path)
                finally:
                    self._queue.task_done()

                if self._stopping and self._queue.empty():
                    break
        except asyncio.CancelledError:
            logger.debug("UploadPoller task cancelled")

    async def _poll_and_handle(self, upload_id: int, fit_path: object, callback: Callable[[object, dict], Awaitable[Any]], backoff_base: float) -> None:
        token = self.auth.ensure_token()
        headers = {"Authorization": f"Bearer {token}"}

        attempt = 0
        backoff = backoff_base
        while True:
            attempt += 1
            try:
                await self.limiter.acquire()
                url = self.status_url_template.format(upload_id=upload_id)
                async with self.session.get(url, headers=headers) as resp:
                    # Convert headers to plain dict for update_limits
                    self.limiter.update_limits(dict(resp.headers))
                    if resp.status == 429:
                        ra = resp.headers.get("Retry-After")
                        # Try to parse Retry-After as seconds when possible
                        ra_val = None
                        if ra:
                            try:
                                ra_val = float(ra)
                            except Exception:
                                ra_val = None
                        logger.warning("Poller: 429 for upload %s, Retry-After=%s", upload_id, ra)
                        await self.limiter.force_backoff(ra_val)
                        # retry after backoff loop
                        continue

                    resp.raise_for_status()
                    data = await resp.json()
                    # If activity is created or an error/duplicate occurred, call callback
                    status_text = data.get("status", "")
                    if data.get("activity_id") or "error" in data or "duplicate" in status_text:
                        await callback(fit_path, data)
                        return

            except aiohttp.ClientError as e:
                logger.warning("Poll attempt %d for %s failed: %s", attempt, upload_id, e)

            # If we get here, wait and try again with incremental backoff (capped)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 10.0)
