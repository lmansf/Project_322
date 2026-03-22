"""Tests for generic Silver/Gold layer writer."""

from datetime import datetime, timezone
from pathlib import Path

from src.utils.layer_writer import write_layer_records


def test_write_layer_records_json_fallback(tmp_path: Path) -> None:
    run_ts = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    target_path = str(tmp_path / "silver" / "silver_id_churn_metrics")

    result = write_layer_records(
        table_name="silver_id_churn_metrics",
        records=[{"source": "all", "total_active": 10, "total_retired": 2, "retired_to_active_ratio": 0.2}],
        target_path=target_path,
        run_ts=run_ts,
        spark=None,
        prefer_parquet=True,
    )

    assert result["format"] == "json"
    assert result["record_count"] == 1
    assert Path(result["path"]).exists()


def test_write_layer_records_empty_noop(tmp_path: Path) -> None:
    run_ts = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    target_path = str(tmp_path / "silver" / "silver_id_churn_metrics")

    result = write_layer_records(
        table_name="silver_id_churn_metrics",
        records=[],
        target_path=target_path,
        run_ts=run_ts,
        spark=None,
        prefer_parquet=True,
    )

    assert result["format"] == "none"
    assert result["record_count"] == 0
