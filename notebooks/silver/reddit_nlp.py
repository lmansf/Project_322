# Databricks notebook source
"""Silver NLP scaffold for Reddit posts and comments."""

from src.contracts.tables import SILVER_TABLES


def run() -> dict:
    """Create cleaned Reddit entities and attach transformer sentiment/topics."""
    # TODO: Clean markdown, strip links, normalize case and punctuation.
    # TODO: Add transformer outputs: sentiment_label, sentiment_score, topic_id.
    return {"status": "placeholder", "tables": SILVER_TABLES["reddit"]}


if __name__ == "__main__":
    print(run())
