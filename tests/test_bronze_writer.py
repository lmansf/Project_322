"""Tests for Bronze payload landing utility."""

from datetime import datetime, timezone
import json
from pathlib import Path

from src.utils.bronze_writer import write_bronze_payloads, write_bronze_structured_records


def test_write_bronze_payloads_json_fallback(tmp_path: Path) -> None:
    run_ts = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    target_base = str(tmp_path / "bronze" / "riot" / "raw")
    payloads = [{"id": "1", "value": 10}, {"id": "2", "value": 20}]

    result = write_bronze_payloads(
        source_name="riot",
        entity_name="matches",
        records=payloads,
        base_target_path=target_base,
        run_ts=run_ts,
        spark=None,
        prefer_parquet=True,
    )

    assert result["format"] == "json"
    assert result["record_count"] == 2
    assert Path(result["path"]).exists()


def test_write_bronze_payloads_empty_records_noop() -> None:
    run_ts = datetime.now(timezone.utc)
    result = write_bronze_payloads(
        source_name="reddit",
        entity_name="comments",
        records=[],
        base_target_path="/Volumes/game_intel_dev/lakehouse/bronze/reddit/raw",
        run_ts=run_ts,
        spark=None,
        prefer_parquet=True,
    )

    assert result["format"] == "none"
    assert result["record_count"] == 0


def test_write_bronze_structured_records_json_fallback(tmp_path: Path) -> None:
    run_ts = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    target_base = str(tmp_path / "bronze" / "reddit" / "retired")
    records = [
        {"primary_key": "pk_1", "id": "a1", "retired_date": "2026-03-22"},
        {"primary_key": "pk_2", "id": "a2", "retired_date": "2026-03-22"},
    ]

    result = write_bronze_structured_records(
        source_name="reddit",
        entity_name="bronze_reddit_post_ids_retired",
        records=records,
        base_target_path=target_base,
        run_ts=run_ts,
        spark=None,
        prefer_parquet=True,
    )

    assert result["format"] == "json"
    assert result["record_count"] == 2
    output_file = Path(result["path"])
    assert output_file.exists()

    lines = output_file.read_text(encoding="utf-8").strip().splitlines()
    parsed = [json.loads(line) for line in lines]
    assert set(parsed[0].keys()) == {"primary_key", "id", "retired_date"}
