"""Tests for layer table path helpers."""

from datetime import datetime

from src.utils.io_paths import build_gold_table_path, build_silver_table_path


def test_build_silver_table_path() -> None:
    run_ts = datetime(2026, 3, 22, 8, 15)
    path_value = build_silver_table_path("silver_id_churn_metrics", run_ts)
    assert path_value.endswith("/silver/silver_id_churn_metrics/event_date=2026-03-22/ingest_hour=08")


def test_build_gold_table_path() -> None:
    run_ts = datetime(2026, 3, 22, 19, 5)
    path_value = build_gold_table_path("gold_meta_evolution", run_ts)
    assert path_value.endswith("/gold/gold_meta_evolution/event_date=2026-03-22/ingest_hour=19")
