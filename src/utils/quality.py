"""Data quality and ingestion telemetry stubs."""

from datetime import datetime
from typing import Any


def record_ingestion_event(
    source_name: str,
    target_path: str,
    run_ts: datetime,
    *,
    status: str = "success",
    record_count: int = 0,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Placeholder for ingestion event logging."""
    _ = (source_name, target_path, run_ts, status, record_count, metadata)


def assert_required_columns(columns: list[str], required: list[str]) -> None:
    missing = [col for col in required if col not in columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
