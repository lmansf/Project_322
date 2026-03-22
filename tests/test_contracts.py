"""Contract smoke tests for table and feature definitions."""

from src.contracts.features import MATCH_OUTCOME_FEATURES, PLAYER_TILT_FEATURES
from src.contracts.tables import BRONZE_TABLES, GOLD_TABLES, SILVER_TABLES


def test_bronze_domains_present() -> None:
    assert set(BRONZE_TABLES.keys()) == {"riot", "weather", "reddit"}


def test_silver_domains_present() -> None:
    assert set(SILVER_TABLES.keys()) == {"riot", "weather", "reddit"}


def test_gold_sections_present() -> None:
    assert set(GOLD_TABLES.keys()) == {"ml_features", "analytics"}


def test_feature_sets_non_empty() -> None:
    assert len(MATCH_OUTCOME_FEATURES) > 0
    assert len(PLAYER_TILT_FEATURES) > 0
