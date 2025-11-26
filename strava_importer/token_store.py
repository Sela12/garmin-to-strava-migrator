"""Abstract interfaces and concrete implementations for token storage.

This module defines the TokenStore abstraction to allow swapping token persistence
strategies (file, database, in-memory, etc.) without changing uploader code.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class TokenInfo:
    """Container for access/refresh token information."""
    access_token: str
    refresh_token: str | None
    expires_at: int


class TokenStore(ABC):
    """Abstract base class for token persistence strategies."""

    @abstractmethod
    def load(self) -> TokenInfo | None:
        """Load and return stored token, or None if not found."""
        pass

    @abstractmethod
    def save(self, token: TokenInfo) -> None:
        """Persist token to storage."""
        pass


class FileTokenStore(TokenStore):
    """Token storage backed by a JSON file."""

    def __init__(self, file_path: Path | str) -> None:
        """Initialize with a file path for token storage."""
        self.file_path = Path(file_path)

    def load(self) -> TokenInfo | None:
        """Load token from JSON file if it exists."""
        try:
            if not self.file_path.exists():
                return None
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            token = TokenInfo(
                access_token=data["access_token"],
                refresh_token=data.get("refresh_token"),
                expires_at=int(data.get("expires_at", 0)),
            )
            logger.debug("Loaded token from %s", self.file_path)
            return token
        except Exception:
            logger.exception("Failed to load token from %s", self.file_path)
            return None

    def save(self, token: TokenInfo) -> None:
        """Persist token to JSON file."""
        try:
            payload = {
                "access_token": token.access_token,
                "refresh_token": token.refresh_token,
                "expires_at": token.expires_at,
            }
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            logger.debug("Saved token to %s", self.file_path)
        except Exception:
            logger.exception("Failed to save token to %s", self.file_path)


class InMemoryTokenStore(TokenStore):
    """Token storage in memory (lost on process exit)."""

    def __init__(self) -> None:
        """Initialize in-memory store."""
        self._token: TokenInfo | None = None

    def load(self) -> TokenInfo | None:
        """Return in-memory token if set."""
        return self._token

    def save(self, token: TokenInfo) -> None:
        """Store token in memory."""
        self._token = token
        logger.debug("Stored token in memory")
