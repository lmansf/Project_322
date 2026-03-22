# Architecture Notes

## Core Narrative
This project blends gameplay behavior, environmental context, and community sentiment to explain and predict player outcomes in League of Legends.

## Domain Split
- Riot API: gameplay truth source.
- OpenWeather API: environmental context at match-time.
- Reddit API: community sentiment and topic drift around patches and champions.

## Medallion Intent
- Bronze: immutable JSON landing plus ingestion metadata.
- Silver: conformed entities and NLP-enriched Reddit records.
- Gold: ML feature products and analytics marts.

## Orchestration
Databricks Workflows are scaffolded through Asset Bundles:
- Ingestion jobs: Riot (15m), Reddit (30m), Weather (hourly).
- Medallion pipeline: Silver transforms then Gold products.
