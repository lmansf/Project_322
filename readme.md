# Player Performance and Environment Intelligence Platform

Databricks Lakehouse scaffold for a cross-domain intelligence platform that blends gameplay, environment, and community sentiment to understand and predict player performance.

## Why this version is stronger
- Twitch is intentionally removed to reduce noise and improve explainability.
- The signal mix is more defensible for performance prediction:
	- Behavior: Riot gameplay and timelines.
	- Environment: weather context at match time.
	- Sentiment: patch and champion community reaction from Reddit.

## Data domains and sources

### Riot Games API
- Match metadata
- Participant stats
- Timelines (frame-level events)
- Champion metadata
- Queue, region, and patch context

### OpenWeather API
- Temperature
- Humidity
- Precipitation
- Weather condition codes
- Region-based weather snapshots aligned to match time

### Reddit API
- Posts from r/leagueoflegends
- Patch discussions
- Champion-specific threads
- Comment-level sentiment and topics (transformer-ready scaffold)

## Ingestion and orchestration
Databricks Workflows are defined via Databricks Asset Bundles.

- Riot ingestion: every 15 minutes
- Reddit ingestion: every 30 minutes
- Weather ingestion: hourly

Workflow definitions:
- `workflows/jobs/ingestion.yml`
- `workflows/jobs/medallion.yml`

Bundle root:
- `databricks.yml`

## Medallion architecture

### Bronze
Immutable raw JSON landing with ingestion metadata.

Tables:
- bronze_riot_matches_raw
- bronze_riot_timelines_raw
- bronze_riot_champions_raw
- bronze_weather_snapshots_raw
- bronze_reddit_posts_raw
- bronze_reddit_comments_raw

### Silver
Cleaned, normalized, and conformed entities.

Riot:
- silver_riot_matches
- silver_riot_participants
- silver_riot_timeline_events
- silver_riot_champions

Weather:
- silver_weather_region_snapshots

Reddit:
- silver_reddit_posts_enriched
- silver_reddit_comments_enriched

NLP enrichment columns (scaffold):
- sentiment_label
- sentiment_score
- topic_id

### Gold
ML features and analytics products.

ML feature tables:
- gold_feature_match_outcome (grain: match_id, team_id)
- gold_feature_player_tilt (grain: summoner_puuid, session_id)

Analytics marts:
- gold_meta_evolution
- gold_player_behavior
- gold_sentiment_vs_performance

## Machine learning architecture

### Model 1: Match outcome prediction
- Target: team_win
- Candidate model family: XGBoost or LightGBM
- Tracking: MLflow
- Batch inference output: gold-level prediction table (to be created in implementation phase)

### Model 2: Player tilt prediction
- Target: tilt_risk_label
- Candidate model family: Gradient boosting or logistic regression
- Tracking: MLflow
- Batch inference output: gold-level prediction table (to be created in implementation phase)

Feature reuse plan:
- Register both feature tables in Databricks Feature Store during implementation.

## Dashboard products

### Meta Evolution dashboard
- Patch selector
- Champion pick/ban/win trends
- Sentiment overlays

### Player Behavior dashboard
- Tilt risk distribution
- Session fatigue patterns
- Region and time-of-day effects

### Sentiment Intelligence dashboard
- Patch sentiment vs win rate
- Champion sentiment vs pick rate
- Toxicity spikes around balance changes

## Scaffold structure

```text
Project_322/
	config/
		project.yml
		sources.yml
	docs/
		architecture.md
		table_catalog.md
	notebooks/
		bronze/
			ingest_riot.py
			ingest_weather.py
			ingest_reddit.py
		silver/
			riot_conform.py
			weather_align.py
			reddit_nlp.py
		gold/
			features_match_outcome.py
			features_player_tilt.py
			marts_analytics.py
	src/
		contracts/
			tables.py
			features.py
		utils/
			io_paths.py
			quality.py
	tests/
		test_contracts.py
		test_workflows.py
	workflows/jobs/
		ingestion.yml
		medallion.yml
	databricks.yml
	requirements.txt
	readme.md
```

## Local validation
1. Install dependencies:

	 ```powershell
	 pip install -r requirements.txt
	 ```

2. Run tests:

	 ```powershell
	 pytest
	 ```

## Implementation roadmap
- Wire real API clients and auth for Riot, OpenWeather, and Reddit.
- Implement Bronze raw writers with schema capture and idempotency.
- Implement Silver conformance and NLP inference jobs.
- Build Gold features and analytics marts.
- Add MLflow training/inference pipelines and Feature Store registration.
- Publish Databricks SQL or Power BI dashboards.
