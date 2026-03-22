# Table Catalog (Scaffold)

## Bronze
- bronze_riot_matches_raw
- bronze_riot_timelines_raw
- bronze_riot_champions_raw
- bronze_weather_snapshots_raw
- bronze_google_trends_champion_interest_raw
- bronze_google_trends_patch_hype_raw

## Bronze Retired Archives
- bronze_riot_match_ids_retired
- bronze_google_trends_series_ids_retired

## Silver
- silver_riot_matches
- silver_riot_participants
- silver_riot_timeline_events
- silver_riot_champions
- silver_weather_region_snapshots
- silver_google_trends_champion_interest
- silver_google_trends_patch_hype
- silver_id_churn_metrics

## Gold ML Features
- gold_feature_match_outcome (grain: match_id, team_id)
- gold_feature_player_tilt (grain: summoner_puuid, session_id)

## Gold Analytics
- gold_meta_evolution
- gold_player_behavior
- gold_sentiment_vs_performance
