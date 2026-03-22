"""Feature contracts for ML products."""

MATCH_OUTCOME_FEATURES = [
    "champion_ids",
    "role_distribution",
    "champion_embedding",
    "rank_tier",
    "recent_win_rate",
    "kda_trend",
    "patch_version",
    "queue_type",
    "match_hour_utc",
    "weather_temperature_c",
    "weather_humidity_pct",
    "weather_precipitation_mm",
    "patch_sentiment_score",
    "champion_sentiment_score",
]

PLAYER_TILT_FEATURES = [
    "loss_streak_len",
    "kda_trend_5_games",
    "session_length_minutes",
    "session_game_count",
    "time_of_day_fatigue_index",
    "patch_sentiment_score",
]

MODEL_TARGETS = {
    "match_outcome": "team_win",
    "player_tilt": "tilt_risk_label",
}
