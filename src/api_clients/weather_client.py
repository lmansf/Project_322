"""OpenWeather API client."""

from __future__ import annotations

from typing import Any

from src.api_clients.base import ApiClientConfig, BaseApiClient
from src.utils.secrets import resolve_secret


class OpenWeatherApiClient(BaseApiClient):
    def __init__(self, *, api_key: str, config: ApiClientConfig | None = None) -> None:
        super().__init__(config=config)
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5"

    @classmethod
    def from_secrets(
        cls,
        *,
        secret_scope: str = "kv_databricks_scope",
        dbutils: Any | None = None,
        config: ApiClientConfig | None = None,
    ) -> "OpenWeatherApiClient":
        api_key = resolve_secret("openweather-api-key", secret_scope=secret_scope, dbutils=dbutils)
        return cls(api_key=api_key, config=config)

    def get_current_weather(
        self,
        *,
        lat: float,
        lon: float,
        units: str = "metric",
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"{self.base_url}/weather",
            params={"lat": lat, "lon": lon, "appid": self.api_key, "units": units},
        )
