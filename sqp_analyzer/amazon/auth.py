"""SP-API authentication using Login with Amazon (LWA)."""

import time
from dataclasses import dataclass
from typing import Any

import requests

from ..config import SPAPIConfig


LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"


@dataclass
class AccessToken:
    """LWA access token with expiry tracking."""

    token: str
    expires_at: float

    def is_expired(self) -> bool:
        """Check if token is expired (with 60s buffer)."""
        return time.time() >= (self.expires_at - 60)


class SPAPIAuth:
    """Handle SP-API authentication via Login with Amazon."""

    def __init__(self, config: SPAPIConfig):
        self.config = config
        self._access_token: AccessToken | None = None

    def get_access_token(self) -> str:
        """Get valid access token, refreshing if necessary."""
        if self._access_token is None or self._access_token.is_expired():
            self._refresh_token()
        return self._access_token.token

    def _refresh_token(self) -> None:
        """Refresh the access token using the refresh token."""
        response = requests.post(
            LWA_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.config.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        expires_in = data.get("expires_in", 3600)

        self._access_token = AccessToken(
            token=data["access_token"],
            expires_at=time.time() + expires_in,
        )

    def get_auth_headers(self) -> dict[str, str]:
        """Get headers required for SP-API requests."""
        return {
            "x-amz-access-token": self.get_access_token(),
            "Content-Type": "application/json",
        }

    def test_connection(self) -> dict[str, Any]:
        """Test authentication by getting a token.

        Returns dict with success status and message.
        """
        try:
            token = self.get_access_token()
            return {
                "success": True,
                "message": "Successfully authenticated with Amazon SP-API",
                "token_preview": f"{token[:20]}...",
            }
        except requests.RequestException as e:
            return {
                "success": False,
                "message": f"Authentication failed: {str(e)}",
            }
