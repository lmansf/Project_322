"""Bronze ingestion scaffold for Reddit API."""

from datetime import datetime, timezone

from src.utils.io_paths import build_raw_path
from src.utils.quality import record_ingestion_event


def run(source_name: str = "reddit") -> dict:
    """Ingest subreddit posts/comments and land immutable raw JSON into Bronze."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)

    # TODO: Pull hot and patch-related posts from r/leagueoflegends.
    # TODO: Pull comments and persist raw payloads with ingestion metadata.
    record_ingestion_event(source_name=source_name, target_path=target_path, run_ts=run_ts)

    return {"source": source_name, "target_path": target_path, "run_ts": run_ts.isoformat()}


if __name__ == "__main__":
    print(run())
