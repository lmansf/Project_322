"""Riot Games API client."""

from __future__ import annotations

from typing import Any

from src.api_clients.base import ApiClientConfig, BaseApiClient
from src.utils.secrets import resolve_secret


class RiotApiClient(BaseApiClient):
    def __init__(
        self,
        *,
        api_key: str,
        regional_routing: str = "americas",
        platform_routing: str = "na1",
        config: ApiClientConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self.api_key = api_key
        self.regional_routing = regional_routing
        self.platform_routing = platform_routing

    @classmethod
    def from_secrets(
        cls,
        *,
        secret_scope: str = "kv_databricks_scope",
        dbutils: Any | None = None,
        regional_routing: str = "americas",
        platform_routing: str = "na1",
        config: ApiClientConfig | None = None,
    ) -> "RiotApiClient":
        api_key = resolve_secret("riot-api-key", secret_scope=secret_scope, dbutils=dbutils)
        return cls(
            api_key=api_key,
            regional_routing=regional_routing,
            platform_routing=platform_routing,
            config=config,
        )

    def _headers(self) -> dict[str, str]:
        return {"X-Riot-Token": self.api_key}

    def _regional_url(self, path: str) -> str:
        return f"https://{self.regional_routing}.api.riotgames.com{path}"

    def _platform_url(self, path: str) -> str:
        return f"https://{self.platform_routing}.api.riotgames.com{path}"

    def get_match_ids_by_puuid(
        self,
        *,
        puuid: str,
        start: int = 0,
        count: int = 10,
        queue: int | None = None,
        game_type: str | None = None,
    ) -> list[str]:
        params: dict[str, Any] = {"start": start, "count": count}
        if queue is not None:
            params["queue"] = queue
        if game_type is not None:
            params["type"] = game_type

        return self._request_json(
            "GET",
            self._regional_url(f"/lol/match/v5/matches/by-puuid/{puuid}/ids"),
            headers=self._headers(),
            params=params,
        )

    def get_match(self, match_id: str) -> dict[str, Any]:
        return self._request_json(
            "GET",
            self._regional_url(f"/lol/match/v5/matches/{match_id}"),
            headers=self._headers(),
        )

    def get_timeline(self, match_id: str) -> dict[str, Any]:
        return self._request_json(
            "GET",
            self._regional_url(f"/lol/match/v5/matches/{match_id}/timeline"),
            headers=self._headers(),
        )

    def get_champion_rotations(self) -> dict[str, Any]:
        return self._request_json(
            "GET",
            self._platform_url("/lol/platform/v3/champion-rotations"),
            headers=self._headers(),
        )
