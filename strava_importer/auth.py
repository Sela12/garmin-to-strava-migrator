import json
import time
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class TokenInfo:
    access_token: str
    refresh_token: Optional[str]
    expires_at: int


class StravaAuth:
    """Handles exchanging the auth code and refreshing tokens.

    Optionally persists tokens to `token_file` when provided to avoid
    re-exchanging the auth code each run.
    """
    TOKEN_URL = "https://www.strava.com/oauth/token"

    def __init__(self, client_id: str, client_secret: str, auth_code: str, token_file: Optional[Path] = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_code = auth_code
        self.token_file = token_file
        self.token: Optional[TokenInfo] = None
        if self.token_file:
            self._load_from_file()

    def _load_from_file(self) -> None:
        try:
            if self.token_file and self.token_file.exists():
                with open(self.token_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.token = TokenInfo(
                    access_token=data["access_token"],
                    refresh_token=data.get("refresh_token"),
                    expires_at=int(data.get("expires_at", 0)),
                )
                logger.debug("Loaded tokens from %s", self.token_file)
        except Exception:
            logger.exception("Failed to load token file; will exchange code")

    def _save_to_file(self) -> None:
        if not self.token_file or not self.token:
            return
        try:
            payload = {
                "access_token": self.token.access_token,
                "refresh_token": self.token.refresh_token,
                "expires_at": self.token.expires_at,
            }
            self.token_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            logger.debug("Saved tokens to %s", self.token_file)
        except Exception:
            logger.exception("Failed to save token file")

    def exchange_code(self) -> TokenInfo:
        resp = requests.post(self.TOKEN_URL, data={
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": self.auth_code,
            "grant_type": "authorization_code",
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        token = TokenInfo(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            expires_at=int(time.time()) + int(data.get("expires_in", 0)),
        )
        self.token = token
        self._save_to_file()
        logger.info("Exchanged code for access token, expires_at=%s", token.expires_at)
        return token

    def refresh(self) -> TokenInfo:
        if not self.token or not self.token.refresh_token:
            raise RuntimeError("No refresh token available")
        resp = requests.post(self.TOKEN_URL, data={
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.token.refresh_token,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        token = TokenInfo(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token", self.token.refresh_token),
            expires_at=int(time.time()) + int(data.get("expires_in", 0)),
        )
        self.token = token
        self._save_to_file()
        logger.info("Refreshed access token, new expires_at=%s", token.expires_at)
        return token

    def ensure_token(self) -> str:
        if not self.token:
            # Try to obtain token by exchanging code
            self.exchange_code()
        # refresh if expiring soon
        if self.token and self.token.expires_at - int(time.time()) < 60:
            try:
                self.refresh()
            except Exception:
                logger.exception("Token refresh failed; attempting exchange code")
                self.exchange_code()
        if not self.token:
            raise RuntimeError("Failed to obtain token")
        return self.token.access_token