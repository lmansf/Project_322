"""Shared HTTP client utilities with retry behavior."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class ApiClientConfig:
    timeout_seconds: int = 20
    max_retries: int = 3
    backoff_seconds: float = 1.0


class BaseApiClient:
    """Base class that handles HTTP requests with basic retry semantics."""

    def __init__(self, config: ApiClientConfig | None = None) -> None:
        self.config = config or ApiClientConfig()
        self.session = requests.Session()

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        auth: Any | None = None,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    data=data,
                    auth=auth,
                    timeout=self.config.timeout_seconds,
                )

                if response.status_code == 429 or response.status_code >= 500:
                    raise requests.HTTPError(
                        f"Transient API error {response.status_code}: {response.text[:300]}"
                    )

                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt < self.config.max_retries:
                    time.sleep(self.config.backoff_seconds * attempt)
                    continue
                break

        raise RuntimeError(f"Request failed after retries: {method} {url}") from last_error
