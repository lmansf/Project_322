"""Utilities to compute active/retired id churn aggregates for downstream use."""

from __future__ import annotations

from typing import Any


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def compute_id_churn_aggregates(source_states: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Compute per-source and total active/retired aggregates with a downstream ratio."""
    by_source: dict[str, dict[str, Any]] = {}
    total_active = 0
    total_retired = 0

    for source_name, state in source_states.items():
        active_ids = set()
        retired_count = 0

        for key, value in state.items():
            if not isinstance(value, list):
                continue
            if key.startswith("seen_") and key.endswith("_ids"):
                active_ids.update(item for item in value if isinstance(item, str) and item)
            if key.startswith("retired_") and key.endswith("_ids"):
                retired_count += len([item for item in value if isinstance(item, dict)])

        active_count = len(active_ids)
        by_source[source_name] = {
            "source": source_name,
            "total_active": active_count,
            "total_retired": retired_count,
            "retired_to_active_ratio": _safe_ratio(retired_count, active_count),
        }

        total_active += active_count
        total_retired += retired_count

    return {
        "by_source": by_source,
        "totals": {
            "total_active": total_active,
            "total_retired": total_retired,
            "retired_to_active_ratio": _safe_ratio(total_retired, total_active),
        },
    }
