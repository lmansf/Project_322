# Databricks notebook source
"""Silver aggregate scaffold for id retirement churn metrics."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def _ensure_project_root_on_path() -> None:
    cwd = Path.cwd()
    for candidate in (cwd, cwd.parent, cwd.parent.parent):
        if (candidate / "src").exists():
            sys.path.insert(0, str(candidate))
            return

    dbutils_ref = globals().get("dbutils")
    if dbutils_ref is None:
        return

    try:
        notebook_path = (
            dbutils_ref.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        )
    except Exception:
        return

    if "/notebooks/" not in notebook_path:
        return

    project_root = notebook_path.split("/notebooks/")[0]
    if project_root and project_root not in sys.path:
        sys.path.insert(0, project_root)


_ensure_project_root_on_path()

from src.utils.id_churn import compute_id_churn_aggregates
from src.utils.idempotency import load_state
from src.utils.io_paths import DEFAULT_VOLUME_ROOT, build_silver_table_path, build_state_path
from src.utils.layer_writer import write_layer_records


def run() -> dict:
    """Compute active vs retired id aggregates for downstream ratio consumption."""
    run_ts = datetime.now(timezone.utc)
    spark_ref = globals().get("spark")
    volume_root = os.getenv("VOLUME_ROOT", DEFAULT_VOLUME_ROOT)
    table_name = "silver_id_churn_metrics"

    source_states = {
        "riot": load_state(build_state_path("riot", volume_root=volume_root)),
        "google_trends": load_state(build_state_path("google_trends", volume_root=volume_root)),
        "weather": load_state(build_state_path("weather", volume_root=volume_root)),
    }

    aggregates = compute_id_churn_aggregates(source_states)
    target_path = build_silver_table_path(table_name=table_name, run_ts=run_ts, volume_root=volume_root)

    rows = []
    for source_name, source_values in aggregates["by_source"].items():
        rows.append(
            {
                "metric_scope": "source",
                "source": source_name,
                "total_active": source_values["total_active"],
                "total_retired": source_values["total_retired"],
                "retired_to_active_ratio": source_values["retired_to_active_ratio"],
                "metric_ts_utc": run_ts.isoformat(),
            }
        )

    rows.append(
        {
            "metric_scope": "total",
            "source": "all",
            "total_active": aggregates["totals"]["total_active"],
            "total_retired": aggregates["totals"]["total_retired"],
            "retired_to_active_ratio": aggregates["totals"]["retired_to_active_ratio"],
            "metric_ts_utc": run_ts.isoformat(),
        }
    )

    write_result = write_layer_records(
        table_name=table_name,
        records=rows,
        target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    return {
        "status": "persisted",
        "run_ts": run_ts.isoformat(),
        "table": table_name,
        "totals": aggregates["totals"],
        "by_source": aggregates["by_source"],
        "write": write_result,
    }


if __name__ == "__main__":
    print(run())
