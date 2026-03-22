# Databricks notebook source
"""Gold feature scaffold for match outcome prediction."""

import sys
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

from src.contracts.features import MATCH_OUTCOME_FEATURES


def run() -> dict:
    """Build feature table at grain match_id + team_id."""
    # TODO: Join Riot Silver + weather context + Google Trends aggregates.
    # TODO: Materialize features for batch training and inference.
    return {"status": "placeholder", "feature_count": len(MATCH_OUTCOME_FEATURES)}


if __name__ == "__main__":
    print(run())
