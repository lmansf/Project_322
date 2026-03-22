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

Landing behavior:
- If Spark runtime is available (Databricks), Bronze payloads are written as Parquet files.
- If Spark runtime is not available (local/dev), Bronze payloads fall back to newline-delimited JSON.

Idempotency behavior:
- Riot ingestion keeps a rolling state of seen match ids and only lands unseen matches/timelines.
- Weather ingestion keeps an hourly cursor and skips if the current hour has already been processed.
- Reddit ingestion keeps rolling seen post/comment ids and only lands unseen rows.
- State files are stored under the volume state path at /_state/<source>/idempotency_state.json.
- When seen-id lists are compacted, pruned ids are archived as retired records with exactly:
	- primary_key
	- id
	- retired_date
- Newly retired ids are also landed as files under /bronze/<source>/retired in Parquet (Spark) or JSON (local fallback).

Downstream id-churn aggregates:
- Silver task `id_churn_metrics.py` computes per-source and total metrics for downstream consumption.
- Aggregate fields include:
	- total_active
	- total_retired
	- retired_to_active_ratio
- Aggregates are persisted to parquet (Spark) or JSON fallback at the silver table path for `silver_id_churn_metrics`.

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
		api_clients/
			base.py
			riot_client.py
			weather_client.py
			reddit_client.py
		contracts/
			tables.py
			features.py
		utils/
			io_paths.py
			quality.py
			secrets.py
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

## Databricks quickstart
- Use the short runbook in `docs/quickstart_databricks.md`.

## Secrets wiring
- Databricks runtime path:
	- Clients resolve secrets from `dbutils.secrets.get(scope=..., key=...)` first.
	- Default scope is `kv_databricks_scope`.
- Local runtime path:
	- Clients fall back to environment variables.
	- Use `config/secrets.example.env` as a template for local setup.

Required secret keys:
- `riot-api-key`
- `openweather-api-key`
- `reddit-client-id`
- `reddit-client-secret`

Optional secret key:
- `reddit-user-agent`

## Implementation roadmap
- Wire real API clients and auth for Riot, OpenWeather, and Reddit.
- Implement Bronze raw writers with schema capture and idempotency.
- Implement Silver conformance and NLP inference jobs.
- Build Gold features and analytics marts.
- Add MLflow training/inference pipelines and Feature Store registration.
- Publish Databricks SQL or Power BI dashboards.
