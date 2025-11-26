"""Facade Strava uploader that delegates to the async uploader implementation.

This module keeps a synchronous interface while reusing the async uploader
implementation (`AsyncStravaUploader`) so behavior is consistent across the
project (rate limiting and centralized polling).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import asyncio
import logging
from typing import Optional

from .config import AppConfig
from .async_core import AsyncStravaUploader

logger = logging.getLogger(__name__)


@dataclass
class StravaUploader:
	"""Synchronous facade for the async uploader.

	Parameters
	----------
	config: AppConfig
		Application configuration with credentials and paths.
	max_concurrent: Optional[int]
		Maximum concurrent uploads. If None, use uploader default.
	"""
	config: AppConfig
	max_concurrent: Optional[int] = 5

	def run(self) -> None:
		"""Run the uploader synchronously by delegating to the async implementation."""
		uploader = AsyncStravaUploader(self.config)
		try:
			asyncio.run(uploader.run_async(max_concurrent=self.max_concurrent))
		except KeyboardInterrupt:
			logger.info("Upload cancelled by user")
