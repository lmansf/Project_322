"""Tests for idempotency state persistence helpers."""

from pathlib import Path

from src.utils.idempotency import compact_ids, compact_ids_with_retirement, load_state, save_state


def test_state_round_trip(tmp_path: Path) -> None:
    state_path = str(tmp_path / "state" / "idempotency_state.json")
    payload = {"seen_ids": ["a", "b"], "last_run_ts": "2026-03-22T00:00:00Z"}

    save_state(state_path, payload)
    loaded = load_state(state_path)

    assert loaded == payload


def test_compact_ids_preserves_order_and_uniqueness() -> None:
    values = ["a", "b", "a", "c", "d", "b"]
    compacted = compact_ids(values, keep_last=3)
    assert compacted == ["b", "c", "d"]


def test_compact_ids_with_retirement_archives_pruned_ids() -> None:
    retained, retired, new_retired = compact_ids_with_retirement(
        ["id1", "id2", "id3", "id4"],
        keep_last=2,
        retired_records=[],
        retired_date="2026-03-22",
        primary_key_prefix="test",
    )

    assert retained == ["id3", "id4"]
    assert len(retired) == 2
    assert len(new_retired) == 2
    for record in retired:
        assert set(record.keys()) == {"primary_key", "id", "retired_date"}
        assert record["retired_date"] == "2026-03-22"
        assert record["primary_key"].startswith("test_")


def test_compact_ids_with_retirement_does_not_duplicate_existing_retired_ids() -> None:
    existing = [
        {
            "primary_key": "test_abc",
            "id": "id1",
            "retired_date": "2026-03-21",
        }
    ]

    _, retired, new_retired = compact_ids_with_retirement(
        ["id1", "id2", "id3"],
        keep_last=1,
        retired_records=existing,
        retired_date="2026-03-22",
        primary_key_prefix="test",
    )

    retired_ids = [item["id"] for item in retired]
    assert retired_ids.count("id1") == 1
    assert all(item["id"] != "id1" for item in new_retired)
