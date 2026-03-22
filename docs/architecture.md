# Architecture Notes

## Core Narrative
This project blends gameplay behavior, environmental context, and search-interest signals to explain and predict player outcomes in League of Legends.

## Domain Split
- Riot API: gameplay truth source.
- OpenWeather API: environmental context at match-time.
- Google Trends (PyTrends): global and regional search interest around champions and patch cycles.

## Medallion Intent
- Bronze: immutable JSON landing plus ingestion metadata.
- Silver: conformed entities and normalized Google Trends time series.
- Gold: ML feature products and analytics marts.

## Orchestration
Databricks Workflows are scaffolded through Asset Bundles:
- Ingestion jobs: Riot (15m), Weather (hourly), Google Trends (daily).
- Medallion pipeline: Silver transforms then Gold products.
