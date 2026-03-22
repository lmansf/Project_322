"""Data quality and ingestion telemetry stubs."""

from datetime import datetime


def record_ingestion_event(source_name: str, target_path: str, run_ts: datetime) -> None:
    """Placeholder for ingestion event logging."""
    _ = (source_name, target_path, run_ts)


def assert_required_columns(columns: list[str], required: list[str]) -> None:
    missing = [col for col in required if col not in columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
