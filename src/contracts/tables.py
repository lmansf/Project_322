"""Table contracts for Bronze, Silver, and Gold layers."""

BRONZE_TABLES = {
    "riot": [
        "bronze_riot_matches_raw",
        "bronze_riot_timelines_raw",
        "bronze_riot_champions_raw",
    ],
    "weather": ["bronze_weather_snapshots_raw"],
    "reddit": [
        "bronze_reddit_posts_raw",
        "bronze_reddit_comments_raw",
    ],
}

SILVER_TABLES = {
    "riot": [
        "silver_riot_matches",
        "silver_riot_participants",
        "silver_riot_timeline_events",
        "silver_riot_champions",
    ],
    "weather": ["silver_weather_region_snapshots"],
    "reddit": [
        "silver_reddit_posts_enriched",
        "silver_reddit_comments_enriched",
    ],
    "ops": [
        "silver_id_churn_metrics",
    ],
}

GOLD_TABLES = {
    "ml_features": [
        "gold_feature_match_outcome",
        "gold_feature_player_tilt",
    ],
    "analytics": [
        "gold_meta_evolution",
        "gold_player_behavior",
        "gold_sentiment_vs_performance",
    ],
}
