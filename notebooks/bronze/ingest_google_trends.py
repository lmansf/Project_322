# Databricks notebook source
"""Bronze ingestion scaffold for Google Trends via PyTrends."""

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

from src.api_clients.google_trends_client import GoogleTrendsApiClient
from src.utils.bronze_writer import write_bronze_payloads, write_bronze_structured_records
from src.utils.idempotency import compact_ids_with_retirement, load_state, save_state
from src.utils.io_paths import build_raw_path, build_retired_base_path, build_state_path
from src.utils.quality import record_ingestion_event


DEFAULT_CHAMPION_TERMS = ["ahri", "yasuo", "jinx", "lee sin", "lux"]
DEFAULT_PATCH_TERMS = ["league of legends patch", "lol patch notes", "league patch"]
DEFAULT_GEOS = ["US", "DE", "KR"]


def _parse_csv_env(var_name: str, default_values: list[str]) -> list[str]:
    raw = os.getenv(var_name)
    if not raw:
        return default_values
    parsed = [item.strip() for item in raw.split(",") if item.strip()]
    return parsed or default_values


def _build_series_key(record: dict[str, str | int | bool]) -> str:
    return "|".join(
        [
            str(record.get("keyword", "")),
            str(record.get("geo", "")),
            str(record.get("timeframe", "")),
            str(record.get("snapshot_ts", "")),
        ]
    )


def run(source_name: str = "google_trends") -> dict:
    """Ingest Google Trends time series and regional slices into Bronze."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)
    retired_base_path = build_retired_base_path(source_name=source_name)
    state_path = build_state_path(source_name=source_name)
    spark_ref = globals().get("spark")

    timeframe = os.getenv("GOOGLE_TRENDS_TIMEFRAME", "today 3-m")
    champion_terms = _parse_csv_env("GOOGLE_TRENDS_CHAMPION_TERMS", DEFAULT_CHAMPION_TERMS)
    patch_terms = _parse_csv_env("GOOGLE_TRENDS_PATCH_TERMS", DEFAULT_PATCH_TERMS)
    geos = _parse_csv_env("GOOGLE_TRENDS_GEOS", DEFAULT_GEOS)

    client = GoogleTrendsApiClient()
    state = load_state(state_path)
    seen_series_ids = set(state.get("seen_series_ids", []))

    champion_interest_records: list[dict] = []
    patch_hype_records: list[dict] = []

    for geo in geos:
        champion_interest_records.extend(
            client.get_interest_over_time(keywords=champion_terms, timeframe=timeframe, geo=geo)
        )
        patch_hype_records.extend(client.get_interest_over_time(keywords=patch_terms, timeframe=timeframe, geo=geo))

        for patch_term in patch_terms:
            patch_hype_records.extend(
                client.get_interest_by_region(
                    keyword=patch_term,
                    timeframe=timeframe,
                    geo=geo,
                )
            )

    new_champion_interest: list[dict] = []
    new_patch_hype: list[dict] = []
    all_keys_for_state = list(seen_series_ids)

    for record in champion_interest_records:
        series_key = _build_series_key(record)
        record["trend_series_key"] = series_key
        record["ingested_at_utc"] = run_ts.isoformat()
        if series_key in seen_series_ids:
            continue
        new_champion_interest.append(record)
        all_keys_for_state.append(series_key)

    for record in patch_hype_records:
        series_key = _build_series_key(record)
        record["trend_series_key"] = series_key
        record["ingested_at_utc"] = run_ts.isoformat()
        if series_key in seen_series_ids:
            continue
        new_patch_hype.append(record)
        all_keys_for_state.append(series_key)

    champion_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_google_trends_champion_interest_raw",
        records=new_champion_interest,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )
    patch_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_google_trends_patch_hype_raw",
        records=new_patch_hype,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    updated_keys, retired_series_ids, new_retired_series_ids = compact_ids_with_retirement(
        all_keys_for_state,
        keep_last=500000,
        retired_records=state.get("retired_series_ids", []),
        retired_date=run_ts.date().isoformat(),
        primary_key_prefix="google_trends_series_retired",
    )

    save_state(
        state_path,
        {
            "seen_series_ids": updated_keys,
            "retired_series_ids": retired_series_ids,
            "last_run_ts": run_ts.isoformat(),
            "last_timeframe": timeframe,
            "last_geos": geos,
        },
    )

    retired_write = write_bronze_structured_records(
        source_name=source_name,
        entity_name="bronze_google_trends_series_ids_retired",
        records=new_retired_series_ids,
        base_target_path=retired_base_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    record_ingestion_event(
        source_name=source_name,
        target_path=target_path,
        run_ts=run_ts,
        record_count=len(new_champion_interest) + len(new_patch_hype),
        metadata={
            "timeframe": timeframe,
            "geos": geos,
            "champion_terms": champion_terms,
            "patch_terms": patch_terms,
            "new_champion_interest": len(new_champion_interest),
            "new_patch_hype": len(new_patch_hype),
            "retired_series_ids": len(retired_series_ids),
            "new_retired_series_ids": len(new_retired_series_ids),
            "landing_formats": {
                "bronze_google_trends_champion_interest_raw": champion_write["format"],
                "bronze_google_trends_patch_hype_raw": patch_write["format"],
                "bronze_google_trends_series_ids_retired": retired_write["format"],
            },
        },
    )

    return {
        "source": source_name,
        "target_path": target_path,
        "run_ts": run_ts.isoformat(),
        "timeframe": timeframe,
        "geos": geos,
        "new_champion_interest": len(new_champion_interest),
        "new_patch_hype": len(new_patch_hype),
        "new_retired_series_ids": len(new_retired_series_ids),
        "landing": {
            "bronze_google_trends_champion_interest_raw": champion_write,
            "bronze_google_trends_patch_hype_raw": patch_write,
            "bronze_google_trends_series_ids_retired": retired_write,
        },
    }


if __name__ == "__main__":
    print(run())
