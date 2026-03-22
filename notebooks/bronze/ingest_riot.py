# Databricks notebook source
"""Bronze ingestion scaffold for Riot Games API."""

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

from src.api_clients.riot_client import RiotApiClient
from src.utils.bronze_writer import write_bronze_payloads, write_bronze_structured_records
from src.utils.idempotency import compact_ids_with_retirement, load_state, save_state
from src.utils.io_paths import build_raw_path, build_retired_base_path, build_state_path
from src.utils.quality import record_ingestion_event


def _tracked_puuids_from_env() -> list[str]:
    raw = os.getenv("TRACKED_PUUIDS", "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def run(source_name: str = "riot") -> dict:
    """Ingest Riot payloads and land immutable raw JSON into Bronze volume paths."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)
    region = os.getenv("RIOT_REGIONAL_ROUTING", "americas")
    platform = os.getenv("RIOT_PLATFORM_ROUTING", "na1")
    tracked_puuids = _tracked_puuids_from_env()
    dbutils_ref = globals().get("dbutils")
    spark_ref = globals().get("spark")
    state_path = build_state_path(source_name=source_name)
    retired_base_path = build_retired_base_path(source_name=source_name)
    state = load_state(state_path)
    seen_match_ids = set(state.get("seen_match_ids", []))

    client = RiotApiClient.from_secrets(
        secret_scope=os.getenv("SECRETS_SCOPE", "kv_databricks_scope"),
        dbutils=dbutils_ref,
        regional_routing=region,
        platform_routing=platform,
    )

    match_ids_to_process: list[str] = []
    for puuid in tracked_puuids:
        for match_id in client.get_match_ids_by_puuid(puuid=puuid, count=5):
            if match_id in seen_match_ids:
                continue
            match_ids_to_process.append(match_id)

    match_ids_to_process, _, _ = compact_ids_with_retirement(
        match_ids_to_process,
        keep_last=2000,
        retired_records=[],
        retired_date=run_ts.date().isoformat(),
        primary_key_prefix="riot_match_tmp",
    )

    matches: list[dict] = []
    timelines: list[dict] = []
    for match_id in match_ids_to_process:
        matches.append(client.get_match(match_id))
        timelines.append(client.get_timeline(match_id))

    rotations = client.get_champion_rotations()
    total_records = len(matches) + len(timelines) + 1

    updated_seen, retired_match_ids, new_retired_match_ids = compact_ids_with_retirement(
        list(seen_match_ids) + match_ids_to_process,
        keep_last=5000,
        retired_records=state.get("retired_match_ids", []),
        retired_date=run_ts.date().isoformat(),
        primary_key_prefix="riot_match_retired",
    )
    save_state(
        state_path,
        {
            "seen_match_ids": updated_seen,
            "retired_match_ids": retired_match_ids,
            "last_run_ts": run_ts.isoformat(),
        },
    )

    retired_write = write_bronze_structured_records(
        source_name=source_name,
        entity_name="bronze_riot_match_ids_retired",
        records=new_retired_match_ids,
        base_target_path=retired_base_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    match_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_riot_matches_raw",
        records=matches,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )
    timeline_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_riot_timelines_raw",
        records=timelines,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )
    champion_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_riot_champions_raw",
        records=[rotations],
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    record_ingestion_event(
        source_name=source_name,
        target_path=target_path,
        run_ts=run_ts,
        record_count=total_records,
        metadata={
            "tracked_puuid_count": len(tracked_puuids),
            "new_match_ids": len(match_ids_to_process),
            "retired_match_ids": len(retired_match_ids),
            "new_retired_match_ids": len(new_retired_match_ids),
            "match_count": len(matches),
            "timeline_count": len(timelines),
            "landing_formats": {
                "bronze_riot_matches_raw": match_write["format"],
                "bronze_riot_timelines_raw": timeline_write["format"],
                "bronze_riot_champions_raw": champion_write["format"],
                "bronze_riot_match_ids_retired": retired_write["format"],
            },
        },
    )

    return {
        "source": source_name,
        "target_path": target_path,
        "run_ts": run_ts.isoformat(),
        "new_match_ids": len(match_ids_to_process),
        "retired_match_ids": len(retired_match_ids),
        "new_retired_match_ids": len(new_retired_match_ids),
        "match_count": len(matches),
        "timeline_count": len(timelines),
        "has_rotations": bool(rotations),
        "landing": {
            "bronze_riot_matches_raw": match_write,
            "bronze_riot_timelines_raw": timeline_write,
            "bronze_riot_champions_raw": champion_write,
            "bronze_riot_match_ids_retired": retired_write,
        },
    }


if __name__ == "__main__":
    print(run())
