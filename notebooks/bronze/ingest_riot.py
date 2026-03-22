"""Bronze ingestion scaffold for Riot Games API."""

from datetime import datetime, timezone

from src.utils.io_paths import build_raw_path
from src.utils.quality import record_ingestion_event


def run(source_name: str = "riot") -> dict:
    """Ingest Riot payloads and land immutable raw JSON into Bronze volume paths."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)

    # TODO: Wire Riot API client and pagination.
    # TODO: Persist payloads as raw JSON with ingestion metadata.
    record_ingestion_event(source_name=source_name, target_path=target_path, run_ts=run_ts)

    return {"source": source_name, "target_path": target_path, "run_ts": run_ts.isoformat()}


if __name__ == "__main__":
    print(run())
