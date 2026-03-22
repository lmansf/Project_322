# Databricks Quickstart (Simple)

## 1. Create secret scope and keys
Use scope: `kv_databricks_scope`

Required keys:
- riot-api-key
- openweather-api-key
- reddit-client-id
- reddit-client-secret

Optional key:
- reddit-user-agent

## 2. Ensure volume exists
Expected root path:
- `/Volumes/game_intel_dev/default/lakehouse`

## 3. Set minimal env vars for jobs
- `TRACKED_PUUIDS`
- `SECRETS_SCOPE=kv_databricks_scope`
- `VOLUME_ROOT=/Volumes/game_intel_dev/default/lakehouse`

Optional:
- `RIOT_REGIONAL_ROUTING`
- `RIOT_PLATFORM_ROUTING`
- `REDDIT_SUBREDDIT`

## 4. Deploy bundle
From project root:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## 5. Run in order
1. Run ingestion jobs:
   - `riot_ingestion`
   - `weather_ingestion`
   - `reddit_ingestion`
2. Run `medallion_pipeline`

## 6. Validate id churn output
Silver output table path pattern:
- `/Volumes/game_intel_dev/default/lakehouse/silver/silver_id_churn_metrics/event_date=YYYY-MM-DD/ingest_hour=HH`

It contains rows with:
- `metric_scope` (`source` or `total`)
- `source`
- `total_active`
- `total_retired`
- `retired_to_active_ratio`
- `metric_ts_utc`
