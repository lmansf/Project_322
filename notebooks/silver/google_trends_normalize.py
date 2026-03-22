# Databricks notebook source
"""Silver scaffold for Google Trends normalization and momentum features."""

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

from src.contracts.tables import SILVER_TABLES


def run() -> dict:
    """Normalize trends series and derive momentum-ready entities."""
    # TODO: Load bronze_google_trends_champion_interest_raw payloads.
    # TODO: Normalize snapshot timestamps and map regions to NA/EU/KR buckets.
    # TODO: Build 7-day momentum and patch hype indicators.
    return {"status": "placeholder", "tables": SILVER_TABLES["google_trends"]}


if __name__ == "__main__":
    print(run())
