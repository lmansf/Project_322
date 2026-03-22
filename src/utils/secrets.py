"""Secret resolution helpers for Databricks and local development."""

from __future__ import annotations

import os
from typing import Any


def _normalize_env_key(secret_name: str, prefix: str = "PROJECT322") -> str:
    normalized = secret_name.upper().replace("-", "_")
    return f"{prefix}_{normalized}"


def resolve_secret(
    secret_name: str,
    *,
    secret_scope: str | None = None,
    required: bool = True,
    dbutils: Any | None = None,
) -> str | None:
    """Resolve secrets from Databricks secret scope, then environment variables."""
    if dbutils is not None and secret_scope:
        try:
            value = dbutils.secrets.get(scope=secret_scope, key=secret_name)
            if value:
                return value
        except Exception:
            pass

    env_candidates = [
        _normalize_env_key(secret_name),
        secret_name,
        secret_name.upper().replace("-", "_"),
    ]
    for env_key in env_candidates:
        env_value = os.getenv(env_key)
        if env_value:
            return env_value

    if required:
        raise ValueError(
            f"Secret '{secret_name}' was not found in Databricks scope or environment variables."
        )
    return None
