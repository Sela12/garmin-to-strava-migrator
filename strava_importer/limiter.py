import asyncio
import logging
import time
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

class AsyncRateLimiter:
    """
    An asyncio-compatible rate limiter that adjusts to Strava's API limits.
    It uses a token bucket-like approach and adjusts based on API header feedback.
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
        """Acquire a permit to make a request, waiting if necessary."""
        async with self._lock:
            now = time.time()

            # Reset daily count if a day has passed
            if now - self.daily_start_time > 24 * 3600:
                self.daily_count = 0
                self.daily_start_time = now

            if self.daily_count >= self.daily_limit:
                logger.warning("Daily limit of %d reached. Sleeping for 24 hours.", self.daily_limit)
                await asyncio.sleep(24 * 3600)
                self.daily_count = 0
                self.daily_start_time = time.time()

            self._prune_window()

            if len(self.window_requests) >= self.window_limit:
                wait_time = self.window_requests[0] + self.window_size - now
                if wait_time > 0:
                    logger.warning("Short-term rate limit reached. Sleeping for %.1f seconds.", wait_time)
                    await asyncio.sleep(wait_time)
                
                # Prune again after waiting
                self._prune_window()
            
            self.window_requests.append(time.time())
            self.daily_count += 1

    def update_limits(self, headers: Optional[dict]):
        """Update rate limits based on Strava API response headers."""
        if not headers:
            return

        try:
            short_term_usage, short_term_limit = map(int, headers.get("X-RateLimit-Usage", "0,0").split(','))
            long_term_usage, long_term_limit = map(int, headers.get("X-RateLimit-Limit", "0,0").split(','))

            self.window_limit = short_term_limit
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

    async def force_backoff(self):
        """Force a backoff when a 429 is received without a Retry-After header."""
        wait_time = self.window_size / self.window_limit if self.window_limit > 0 else 5
        logger.warning("Forced backoff due to 429. Sleeping for %.1f seconds.", wait_time)
        await asyncio.sleep(wait_time)

