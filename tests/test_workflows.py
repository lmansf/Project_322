"""Workflow scaffolding tests."""

from pathlib import Path


def test_workflow_files_exist() -> None:
    root = Path(__file__).resolve().parents[1]
    assert (root / "workflows" / "jobs" / "ingestion.yml").exists()
    assert (root / "workflows" / "jobs" / "medallion.yml").exists()
