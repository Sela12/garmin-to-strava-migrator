import time
import logging
from pathlib import Path
from typing import Optional

import requests

from .token_store import TokenStore, FileTokenStore, TokenInfo

logger = logging.getLogger(__name__)


class StravaAuth:
    """Handles exchanging the auth code and refreshing tokens.

    Optionally persists tokens via provided TokenStore to avoid
    re-exchanging the auth code each run.
    """
    TOKEN_URL = "https://www.strava.com/oauth/token"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        auth_code: str,
        token_store: TokenStore | None = None,
        token_file: Path | None = None,
    ) -> None:
        """Initialize the auth helper.

        Parameters
        ----------
        client_id: str
            Strava client id.
        client_secret: str
            Strava client secret.
        auth_code: str
            One-time authorization code.
        token_store: TokenStore | None
            TokenStore implementation for persistence (defaults to FileTokenStore).
        token_file: Path | None
            Path for FileTokenStore (backward compatibility). Ignored if token_store provided.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_code = auth_code
        
        # Support backward compatibility: if no token_store provided, create FileTokenStore
        if token_store is None:
            token_path = token_file if token_file else Path.home() / ".strava_tokens.json"
            token_store = FileTokenStore(token_path)
        
        self.token_store = token_store
        self.token: TokenInfo | None = None
        self._load_from_store()

    def _load_from_store(self) -> None:
        try:
            self.token = self.token_store.load()
            if self.token:
                logger.debug("Loaded tokens from store")
        except Exception:
            logger.exception("Failed to load token from store; will exchange code")

    def _save_to_store(self) -> None:
        if not self.token:
            return
        try:
            self.token_store.save(self.token)
            logger.debug("Saved tokens to store")
        except Exception:
            logger.exception("Failed to save token to store")

    def exchange_code(self) -> TokenInfo:
        resp = requests.post(self.TOKEN_URL, json={
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
        self._save_to_store()
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
        self._save_to_store()
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