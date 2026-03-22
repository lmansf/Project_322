# Databricks notebook source
"""Silver normalization scaffold for Riot domain tables."""

from src.contracts.tables import SILVER_TABLES


def run() -> dict:
    """Normalize Riot Bronze JSON into conformed Silver entities."""
    # TODO: Parse match metadata, participants, timelines, champion metadata.
    # TODO: Deduplicate by match_id and enforce conformed schema contracts.
    return {"status": "placeholder", "tables": SILVER_TABLES["riot"]}


if __name__ == "__main__":
    print(run())
