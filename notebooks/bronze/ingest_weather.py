"""Bronze ingestion scaffold for OpenWeather API."""

from datetime import datetime, timezone

from src.utils.io_paths import build_raw_path
from src.utils.quality import record_ingestion_event


def run(source_name: str = "weather") -> dict:
    """Ingest regional weather snapshots and land immutable raw JSON into Bronze."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)

    # TODO: Resolve region to coordinates map and call OpenWeather endpoint.
    # TODO: Persist payloads as raw JSON with snapshot timestamp and region.
    record_ingestion_event(source_name=source_name, target_path=target_path, run_ts=run_ts)

    return {"source": source_name, "target_path": target_path, "run_ts": run_ts.isoformat()}


if __name__ == "__main__":
    print(run())
