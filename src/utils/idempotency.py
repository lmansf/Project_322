"""Persistent idempotency state helpers for Bronze ingestion jobs."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any


def _localize_path(path_value: str) -> Path:
    if path_value.startswith("/Volumes/"):
        return Path("_local_volumes") / path_value.lstrip("/")
    if path_value.startswith("dbfs:/"):
        return Path("_local_volumes") / path_value.replace("dbfs:/", "dbfs/")
    return Path(path_value)


def load_state(path_value: str) -> dict[str, Any]:
    path_obj = _localize_path(path_value)
    if not path_obj.exists():
        return {}
    with path_obj.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def save_state(path_value: str, state: dict[str, Any]) -> None:
    path_obj = _localize_path(path_value)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with path_obj.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=True, sort_keys=True)


def compact_ids(values: list[str], *, keep_last: int) -> list[str]:
    if keep_last <= 0:
        return []
    deduped = list(dict.fromkeys(values))
    if len(deduped) <= keep_last:
        return deduped
    return deduped[-keep_last:]


def compact_ids_with_retirement(
    values: list[str],
    *,
    keep_last: int,
    retired_records: list[dict[str, str]] | None,
    retired_date: str,
    primary_key_prefix: str,
    max_retired_records: int = 50000,
) -> tuple[list[str], list[dict[str, str]], list[dict[str, str]]]:
    """Compact id list and archive pruned ids with minimal retirement metadata."""
    deduped = list(dict.fromkeys(values))
    retained = compact_ids(deduped, keep_last=keep_last)

    if len(deduped) <= len(retained):
        existing = retired_records or []
        if len(existing) <= max_retired_records:
            return retained, existing, []
        return retained, existing[-max_retired_records:], []

    pruned_ids = deduped[: len(deduped) - len(retained)]
    existing_records = retired_records or []
    existing_ids = {record.get("id") for record in existing_records}

    new_records: list[dict[str, str]] = []
    for raw_id in pruned_ids:
        if not raw_id or raw_id in existing_ids:
            continue
        new_records.append(
            {
                "primary_key": f"{primary_key_prefix}_{uuid.uuid4().hex}",
                "id": raw_id,
                "retired_date": retired_date,
            }
        )

    combined = existing_records + new_records
    if len(combined) > max_retired_records:
        combined = combined[-max_retired_records:]

    return retained, combined, new_records
