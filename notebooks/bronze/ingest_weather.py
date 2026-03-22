"""Bronze ingestion scaffold for OpenWeather API."""

import os
from datetime import datetime, timezone

from src.api_clients.weather_client import OpenWeatherApiClient
from src.utils.bronze_writer import write_bronze_payloads
from src.utils.idempotency import load_state, save_state
from src.utils.io_paths import build_raw_path, build_state_path
from src.utils.quality import record_ingestion_event


REGION_COORDS = {
    "NA": (37.7749, -122.4194),
    "EUW": (48.8566, 2.3522),
    "EUNE": (52.2297, 21.0122),
    "KR": (37.5665, 126.9780),
    "BR": (-23.5505, -46.6333),
}


def run(source_name: str = "weather") -> dict:
    """Ingest regional weather snapshots and land immutable raw JSON into Bronze."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)
    scope = os.getenv("SECRETS_SCOPE", "kv_databricks_scope")
    dbutils_ref = globals().get("dbutils")
    spark_ref = globals().get("spark")
    state_path = build_state_path(source_name=source_name)
    state = load_state(state_path)
    current_hour_key = run_ts.strftime("%Y-%m-%dT%H")
    last_hour_key = state.get("last_snapshot_hour")

    if last_hour_key == current_hour_key:
        return {
            "source": source_name,
            "target_path": target_path,
            "run_ts": run_ts.isoformat(),
            "snapshot_count": 0,
            "idempotency_skipped": True,
            "reason": "hour_already_processed",
        }

    client = OpenWeatherApiClient.from_secrets(secret_scope=scope, dbutils=dbutils_ref)
    snapshots: list[dict] = []
    for region, (lat, lon) in REGION_COORDS.items():
        payload = client.get_current_weather(lat=lat, lon=lon)
        payload["region_code"] = region
        payload["snapshot_ts_utc"] = run_ts.isoformat()
        snapshots.append(payload)

    snapshot_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_weather_snapshots_raw",
        records=snapshots,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    save_state(
        state_path,
        {
            "last_snapshot_hour": current_hour_key,
            "last_run_ts": run_ts.isoformat(),
        },
    )

    record_ingestion_event(
        source_name=source_name,
        target_path=target_path,
        run_ts=run_ts,
        record_count=len(snapshots),
        metadata={
            "regions": list(REGION_COORDS.keys()),
            "landing_format": snapshot_write["format"],
        },
    )

    return {
        "source": source_name,
        "target_path": target_path,
        "run_ts": run_ts.isoformat(),
        "snapshot_count": len(snapshots),
        "idempotency_skipped": False,
        "landing": {"bronze_weather_snapshots_raw": snapshot_write},
    }


if __name__ == "__main__":
    print(run())
