"""Silver aggregate scaffold for id retirement churn metrics."""

from __future__ import annotations

from datetime import datetime, timezone

from src.utils.id_churn import compute_id_churn_aggregates
from src.utils.idempotency import load_state
from src.utils.io_paths import build_state_path


def run() -> dict:
    """Compute active vs retired id aggregates for downstream ratio consumption."""
    run_ts = datetime.now(timezone.utc)

    source_states = {
        "riot": load_state(build_state_path("riot")),
        "reddit": load_state(build_state_path("reddit")),
        "weather": load_state(build_state_path("weather")),
    }

    aggregates = compute_id_churn_aggregates(source_states)

    # TODO: Persist this aggregate as a Silver table: silver_id_churn_metrics
    return {
        "status": "placeholder",
        "run_ts": run_ts.isoformat(),
        "table": "silver_id_churn_metrics",
        "totals": aggregates["totals"],
        "by_source": aggregates["by_source"],
    }


if __name__ == "__main__":
    print(run())
