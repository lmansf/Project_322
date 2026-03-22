# Databricks notebook source
"""Silver alignment scaffold for weather context."""

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
    """Align weather snapshots to match start timestamps by region."""
    # TODO: Map Riot regions to approximate coordinates.
    # TODO: Align nearest snapshot to match start with tolerance window.
    return {"status": "placeholder", "tables": SILVER_TABLES["weather"]}


if __name__ == "__main__":
    print(run())
