"""Tests for id churn aggregate computation."""

from src.utils.id_churn import compute_id_churn_aggregates


def test_compute_id_churn_aggregates_totals_and_ratio() -> None:
    source_states = {
        "riot": {
            "seen_match_ids": ["m1", "m2", "m2"],
            "retired_match_ids": [
                {"primary_key": "pk1", "id": "m0", "retired_date": "2026-03-22"},
            ],
        },
        "google_trends": {
            "seen_series_ids": ["s1", "s2", "s3"],
            "retired_series_ids": [
                {"primary_key": "pk2", "id": "p0", "retired_date": "2026-03-22"},
                {"primary_key": "pk3", "id": "c0", "retired_date": "2026-03-22"},
            ],
        },
    }

    result = compute_id_churn_aggregates(source_states)

    assert result["totals"]["total_active"] == 5
    assert result["totals"]["total_retired"] == 3
    assert result["totals"]["retired_to_active_ratio"] == 0.6


def test_compute_id_churn_aggregates_zero_active_ratio_guard() -> None:
    source_states = {
        "weather": {
            "retired_snapshot_ids": [
                {"primary_key": "pk1", "id": "w0", "retired_date": "2026-03-22"},
            ]
        }
    }

    result = compute_id_churn_aggregates(source_states)
    assert result["totals"]["total_active"] == 0
    assert result["totals"]["total_retired"] == 1
    assert result["totals"]["retired_to_active_ratio"] == 0.0
