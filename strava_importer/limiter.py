import asyncio
import logging
import random
import time
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)


class AsyncRateLimiter:
    """
    An asyncio-compatible rate limiter that adjusts to Strava's API limits.
    This limiter avoids very long sleeps that block workers by performing
    short incremental waits and supports forced backoff with Retry-After.
    """

    def __init__(self, initial_daily_limit: int = 1000, initial_window_limit: int = 100):
        self.daily_limit = initial_daily_limit
        self.window_limit = initial_window_limit
        self.window_size = 15 * 60  # 15 minutes in seconds

        self.daily_count = 0
        self.daily_start_time = time.time()

        self.window_requests = deque()
        self._lock = asyncio.Lock()

    def _prune_window(self):
        """Remove requests from the window that are older than the window size."""
        now = time.time()
        while self.window_requests and self.window_requests[0] < now - self.window_size:
            self.window_requests.popleft()

    async def acquire(self):
        """Acquire a permit to make a request, waiting in short increments if needed."""
        # Use a loop with short sleeps so workers don't get blocked for very long
        while True:
            async with self._lock:
                now = time.time()

                # Reset daily count if a day has passed
                if now - self.daily_start_time > 24 * 3600:
                    self.daily_count = 0
                    self.daily_start_time = now

                if self.daily_count >= self.daily_limit:
                    logger.warning("Daily limit of %d reached. Sleeping for 24 hours.", self.daily_limit)
                    # Keep a cooperative short-sleep loop even for daily limit to allow cancellation
                    # Sleep in 60-second increments up to 24 hours
                    remaining = 24 * 3600
                    while remaining > 0:
                        to_sleep = min(60, remaining)
                        await asyncio.sleep(to_sleep)
                        remaining -= to_sleep
                    self.daily_count = 0
                    self.daily_start_time = time.time()

                self._prune_window()

                if len(self.window_requests) < self.window_limit:
                    # permit one request
                    self.window_requests.append(time.time())
                    self.daily_count += 1
                    return

                # compute how long until the oldest request falls out of the window
                wait_time = self.window_requests[0] + self.window_size - now

            # Release lock and wait a short time before retrying to acquire
            # Sleep in small increments (max 5s) so backpressure is cooperative
            sleep_for = min(max(wait_time, 0.5), 5.0)
            logger.debug("RateLimiter waiting %.1fs before trying again", sleep_for)
            await asyncio.sleep(sleep_for)

    def update_limits(self, headers: Optional[dict]):
        """Update rate limits based on Strava API response headers."""
        if not headers:
            return

        try:
            short_term_usage, short_term_limit = map(int, headers.get("X-RateLimit-Usage", "0,0").split(','))
            long_term_usage, long_term_limit = map(int, headers.get("X-RateLimit-Limit", "0,0").split(','))

            # Only update if values look sane
            if short_term_limit > 0:
                self.window_limit = short_term_limit
            if long_term_limit > 0:
                self.daily_limit = long_term_limit

            # Adjust current counts based on headers
            now = time.time()
            self._prune_window()

            # Update window count to match the server's view
            while len(self.window_requests) < short_term_usage:
                self.window_requests.append(now)

            self.daily_count = long_term_usage

        except (ValueError, IndexError):
            logger.warning("Could not parse Strava rate limit headers.")

    async def force_backoff(self, retry_after: Optional[float] = None):
        """Force a backoff when a 429 is received.

        If `retry_after` is provided (seconds), respect it up to a cap. Otherwise use
        a sensible default based on window size and current limit, clamped to a range.
        Backoff uses small increments and jitter to stay cooperative.
        """
        if retry_after is None:
            # base wait proportional to the per-request spacing, but clamp
            base = (self.window_size / max(self.window_limit, 1))
            wait_time = max(1.0, min(base, 60.0))
        else:
            try:
                wait_time = float(retry_after)
            except (TypeError, ValueError):
                wait_time = 5.0

        # clamp and add a bit of jitter
        wait_time = min(max(wait_time, 1.0), 300.0)
        jitter = random.uniform(0, min(5.0, wait_time * 0.1))
        total = wait_time + jitter
        logger.warning("Forced backoff due to 429. Sleeping for %.1f seconds (retry_after=%s).", total, retry_after)

        remaining = total
        # Sleep in short increments so tasks can be cancelled cooperatively
        while remaining > 0:
            to_sleep = min(5.0, remaining)
            await asyncio.sleep(to_sleep)
            remaining -= to_sleep

