"""Utilities for landing Bronze payloads as Parquet (preferred) or JSON fallback."""

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


def write_bronze_payloads(
    *,
    source_name: str,
    entity_name: str,
    records: list[dict[str, Any]],
    base_target_path: str,
    run_ts: datetime,
    spark: Any | None = None,
    prefer_parquet: bool = True,
) -> dict[str, Any]:
    """Write Bronze records as Parquet when Spark is available, else as NDJSON."""
    if not records:
        return {
            "source": source_name,
            "entity": entity_name,
            "record_count": 0,
            "format": "none",
            "path": f"{base_target_path}/{entity_name}",
        }

    target_path = f"{base_target_path}/{entity_name}"
    rows = [
        {
            "source_name": source_name,
            "entity_name": entity_name,
            "ingested_at_utc": run_ts.isoformat(),
            "payload_json": json.dumps(record, ensure_ascii=True),
        }
        for record in records
    ]

    if spark is not None and prefer_parquet:
        dataframe = spark.createDataFrame(rows)
        dataframe.write.mode("append").parquet(target_path)
        return {
            "source": source_name,
            "entity": entity_name,
            "record_count": len(rows),
            "format": "parquet",
            "path": target_path,
        }

    local_target = _localize_volume_path(target_path)
    local_target.mkdir(parents=True, exist_ok=True)
    file_name = f"batch_{run_ts.strftime('%Y%m%d%H%M%S')}.json"
    output_file = local_target / file_name

    with output_file.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    return {
        "source": source_name,
        "entity": entity_name,
        "record_count": len(rows),
        "format": "json",
        "path": str(output_file).replace("\\", "/"),
    }


def write_bronze_structured_records(
    *,
    source_name: str,
    entity_name: str,
    records: list[dict[str, Any]],
    base_target_path: str,
    run_ts: datetime,
    spark: Any | None = None,
    prefer_parquet: bool = True,
) -> dict[str, Any]:
    """Write structured records as-is, used for retired-id archive landing."""
    if not records:
        return {
            "source": source_name,
            "entity": entity_name,
            "record_count": 0,
            "format": "none",
            "path": f"{base_target_path}/{entity_name}",
        }

    target_path = f"{base_target_path}/{entity_name}"

    if spark is not None and prefer_parquet:
        dataframe = spark.createDataFrame(records)
        dataframe.write.mode("append").parquet(target_path)
        return {
            "source": source_name,
            "entity": entity_name,
            "record_count": len(records),
            "format": "parquet",
            "path": target_path,
        }

    local_target = _localize_volume_path(target_path)
    local_target.mkdir(parents=True, exist_ok=True)
    file_name = f"batch_{run_ts.strftime('%Y%m%d%H%M%S')}.json"
    output_file = local_target / file_name

    with output_file.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")

    return {
        "source": source_name,
        "entity": entity_name,
        "record_count": len(records),
        "format": "json",
        "path": str(output_file).replace("\\", "/"),
    }
