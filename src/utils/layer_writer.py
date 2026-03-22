"""Generic layer writer for Silver/Gold records with Parquet-first behavior."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _localize_volume_path(volume_path: str) -> Path:
    if volume_path.startswith("/Volumes/"):
        return Path("_local_volumes") / volume_path.lstrip("/")
    if volume_path.startswith("dbfs:/"):
        return Path("_local_volumes") / volume_path.replace("dbfs:/", "dbfs/")
    return Path(volume_path)


def write_layer_records(
    *,
    table_name: str,
    records: list[dict[str, Any]],
    target_path: str,
    run_ts: datetime,
    spark: Any | None = None,
    prefer_parquet: bool = True,
) -> dict[str, Any]:
    """Write records to layer path as Parquet when Spark is available, else NDJSON."""
    if not records:
        return {
            "table": table_name,
            "record_count": 0,
            "format": "none",
            "path": target_path,
        }

    if spark is not None and prefer_parquet:
        dataframe = spark.createDataFrame(records)
        dataframe.write.mode("append").parquet(target_path)
        return {
            "table": table_name,
            "record_count": len(records),
            "format": "parquet",
            "path": target_path,
        }

    local_target = _localize_volume_path(target_path)
    local_target.mkdir(parents=True, exist_ok=True)
    file_name = f"batch_{run_ts.strftime('%Y%m%d%H%M%S')}.json"
    output_file = local_target / file_name

    with output_file.open("w", encoding="utf-8") as handle:
        for row in records:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    return {
        "table": table_name,
        "record_count": len(records),
        "format": "json",
        "path": str(output_file).replace("\\", "/"),
    }
