"""Path helpers for Unity Catalog Volume layout."""

from datetime import datetime

DEFAULT_VOLUME_ROOT = "/Volumes/game_intel_dev/default/lakehouse"


def build_bronze_base_path(source_name: str, volume_root: str = DEFAULT_VOLUME_ROOT) -> str:
    return f"{volume_root}/bronze/{source_name}/raw"


def build_retired_base_path(source_name: str, volume_root: str = DEFAULT_VOLUME_ROOT) -> str:
    return f"{volume_root}/bronze/{source_name}/retired"


def build_state_path(source_name: str, volume_root: str = DEFAULT_VOLUME_ROOT) -> str:
    return f"{volume_root}/_state/{source_name}/idempotency_state.json"


def build_raw_path(source_name: str, run_ts: datetime, volume_root: str = DEFAULT_VOLUME_ROOT) -> str:
    base_path = build_bronze_base_path(source_name=source_name, volume_root=volume_root)
    ingest_date = run_ts.strftime("%Y-%m-%d")
    ingest_hour = run_ts.strftime("%H")
    return f"{base_path}/ingest_date={ingest_date}/ingest_hour={ingest_hour}"


def build_silver_table_path(
    table_name: str,
    run_ts: datetime,
    volume_root: str = DEFAULT_VOLUME_ROOT,
) -> str:
    event_date = run_ts.strftime("%Y-%m-%d")
    ingest_hour = run_ts.strftime("%H")
    return f"{volume_root}/silver/{table_name}/event_date={event_date}/ingest_hour={ingest_hour}"


def build_gold_table_path(
    table_name: str,
    run_ts: datetime,
    volume_root: str = DEFAULT_VOLUME_ROOT,
) -> str:
    event_date = run_ts.strftime("%Y-%m-%d")
    ingest_hour = run_ts.strftime("%H")
    return f"{volume_root}/gold/{table_name}/event_date={event_date}/ingest_hour={ingest_hour}"
