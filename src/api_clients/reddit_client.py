"""Reddit API client using OAuth client credentials."""

from __future__ import annotations

import time
from typing import Any

from src.api_clients.base import ApiClientConfig, BaseApiClient
from src.utils.secrets import resolve_secret


class RedditApiClient(BaseApiClient):
    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        user_agent: str,
        config: ApiClientConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    @classmethod
    def from_secrets(
        cls,
        *,
        secret_scope: str = "kv_databricks_scope",
        dbutils: Any | None = None,
        config: ApiClientConfig | None = None,
    ) -> "RedditApiClient":
        client_id = resolve_secret("reddit-client-id", secret_scope=secret_scope, dbutils=dbutils)
        client_secret = resolve_secret("reddit-client-secret", secret_scope=secret_scope, dbutils=dbutils)
        user_agent = resolve_secret(
            "reddit-user-agent",
            secret_scope=secret_scope,
            dbutils=dbutils,
            required=False,
        ) or "project322-reddit-client/1.0"

        return cls(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            config=config,
        )

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "User-Agent": self.user_agent,
        }

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        token_payload = self._request_json(
            "POST",
            "https://www.reddit.com/api/v1/access_token",
            headers={"User-Agent": self.user_agent},
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
        )
        self._access_token = token_payload["access_token"]
        expires_in = int(token_payload.get("expires_in", 3600))
        self._token_expires_at = time.time() + max(expires_in - 60, 60)
        return self._access_token

    def _request_oauth(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"https://oauth.reddit.com{path}",
            headers=self._auth_headers(),
            params=params,
        )

    def get_hot_posts(self, *, subreddit: str = "leagueoflegends", limit: int = 25) -> list[dict[str, Any]]:
        payload = self._request_oauth(f"/r/{subreddit}/hot", params={"limit": limit})
        return [item["data"] for item in payload.get("data", {}).get("children", [])]

    def search_patch_posts(
        self,
        *,
        subreddit: str = "leagueoflegends",
        query: str = "patch",
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        payload = self._request_oauth(
            f"/r/{subreddit}/search",
            params={"q": query, "restrict_sr": "on", "sort": "new", "limit": limit},
        )
        return [item["data"] for item in payload.get("data", {}).get("children", [])]

    def get_comments(self, *, subreddit: str, post_id: str, limit: int = 100) -> list[dict[str, Any]]:
        payload = self._request_oauth(
            f"/r/{subreddit}/comments/{post_id}",
            params={"limit": limit, "sort": "best"},
        )
        if not isinstance(payload, list) or len(payload) < 2:
            return []

        comments_listing = payload[1].get("data", {}).get("children", [])
        comments: list[dict[str, Any]] = []
        for comment in comments_listing:
            data = comment.get("data", {})
            if data.get("body"):
                comments.append(data)
        return comments
