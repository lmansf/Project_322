# Table Catalog (Scaffold)

## Bronze
- bronze_riot_matches_raw
- bronze_riot_timelines_raw
- bronze_riot_champions_raw
- bronze_weather_snapshots_raw
- bronze_reddit_posts_raw
- bronze_reddit_comments_raw

## Silver
- silver_riot_matches
- silver_riot_participants
- silver_riot_timeline_events
- silver_riot_champions
- silver_weather_region_snapshots
- silver_reddit_posts_enriched
- silver_reddit_comments_enriched

## Gold ML Features
- gold_feature_match_outcome (grain: match_id, team_id)
- gold_feature_player_tilt (grain: summoner_puuid, session_id)

## Gold Analytics
- gold_meta_evolution
- gold_player_behavior
- gold_sentiment_vs_performance
