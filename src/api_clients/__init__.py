"""External API clients for Riot, OpenWeather, and Google Trends."""

from src.api_clients.google_trends_client import GoogleTrendsApiClient
from src.api_clients.riot_client import RiotApiClient
from src.api_clients.weather_client import OpenWeatherApiClient

__all__ = ["RiotApiClient", "OpenWeatherApiClient", "GoogleTrendsApiClient"]
