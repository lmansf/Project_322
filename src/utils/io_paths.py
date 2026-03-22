"""Path helpers for Unity Catalog Volume layout."""

from datetime import datetime

DEFAULT_VOLUME_ROOT = "/Volumes/game_intel_dev/lakehouse"


def build_raw_path(source_name: str, run_ts: datetime, volume_root: str = DEFAULT_VOLUME_ROOT) -> str:
    ingest_date = run_ts.strftime("%Y-%m-%d")
    ingest_hour = run_ts.strftime("%H")
    return f"{volume_root}/bronze/{source_name}/raw/ingest_date={ingest_date}/ingest_hour={ingest_hour}"
