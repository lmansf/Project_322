"""Tests for secret resolution behavior."""

import pytest

from src.utils.secrets import resolve_secret


def test_resolve_secret_from_prefixed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROJECT322_RIOT_API_KEY", "abc123")
    assert resolve_secret("riot-api-key") == "abc123"


def test_resolve_secret_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROJECT322_RIOT_API_KEY", raising=False)
    with pytest.raises(ValueError):
        resolve_secret("riot-api-key")


def test_resolve_secret_optional_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROJECT322_GOOGLE_TRENDS_PROXY", raising=False)
    assert resolve_secret("google-trends-proxy", required=False) is None
