import time
import time
import logging
from typing import Callable

import requests

logger = logging.getLogger(__name__)


def request_with_retries(fn: Callable[..., requests.Response], max_retries: int = 4, backoff_factor: float = 1.0) -> Callable[..., requests.Response]:
    """Return a wrapper that calls `fn` and retries on transient errors.

    The wrapped function should have the same signature as `requests.request`/session.request
    and return a `requests.Response`.
    """

    def wrapper(*args, **kwargs) -> requests.Response:
        attempt = 0
        while True:
            try:
                resp = fn(*args, **kwargs)
            except requests.RequestException as e:
                attempt += 1
                if attempt > max_retries:
                    logger.exception("Request failed after %s attempts", attempt)
                    raise
                sleep_for = backoff_factor * (2 ** (attempt - 1))
                logger.warning("Request exception: %s. Retrying in %ss (attempt %s)", e, sleep_for, attempt)
                time.sleep(sleep_for)
                continue

            # On HTTP errors, optionally respect Retry-After for 429
            if resp.status_code == 429:
                attempt += 1
                if attempt > max_retries:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                sleep_for = float(retry_after) if retry_after and retry_after.isdigit() else backoff_factor * (2 ** (attempt - 1))
                logger.warning("Rate limited (429). Sleeping %s seconds before retry (attempt %s)", sleep_for, attempt)
                time.sleep(sleep_for)
                continue

            if 500 <= resp.status_code < 600:
                attempt += 1
                if attempt > max_retries:
                    resp.raise_for_status()
                sleep_for = backoff_factor * (2 ** (attempt - 1))
                logger.warning("Server error %s. Retrying in %s seconds (attempt %s)", resp.status_code, sleep_for, attempt)
                time.sleep(sleep_for)
                continue

            return resp

    return wrapper


class RateLimiter:
    """Simple per-window and daily counters to avoid hitting Strava limits.

    The numbers should be tuned to your app's quota and traffic pattern.
    """

    def __init__(self, daily_limit: int = 10000, window_limit: int = 100):
        self.daily_limit = daily_limit
        self.window_limit = window_limit
        # Strava's short window is 15 min; add tiny buffer
        self.window_size = 15 * 60 + 5

        self.daily_count = 0
        self.window_count = 0
        self.window_start = time.time()

    def wait_if_needed(self) -> None:
        if self.daily_count >= self.daily_limit:
            logger.warning("Daily limit (%s) reached. Pausing for 24 hours.", self.daily_limit)
            time.sleep(24 * 3600)
            self.daily_count = 0

        if self.window_count >= self.window_limit:
            elapsed = time.time() - self.window_start
            if elapsed < self.window_size:
                sleep_time = self.window_size - elapsed
                logger.warning("Rate window full. Cooling down for %.1fs...", sleep_time)
                time.sleep(sleep_time)
            self.window_count = 0
            self.window_start = time.time()

    def record_request(self) -> None:
        self.daily_count += 1
        self.window_count += 1

    def force_backoff(self) -> None:
        logger.warning("Received 429 Rate Limit. Forcing %s second sleep.", self.window_size)
        time.sleep(self.window_size)
        self.window_count = 0
        self.window_start = time.time()
