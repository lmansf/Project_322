"""Google Trends client wrapper built on PyTrends."""

from __future__ import annotations

import importlib
from typing import Any

from src.api_clients.base import ApiClientConfig


class GoogleTrendsApiClient:
    """Thin client around PyTrends for keyword interest retrieval."""

    def __init__(
        self,
        *,
        hl: str = "en-US",
        tz: int = 0,
        config: ApiClientConfig | None = None,
    ) -> None:
        self.config = config or ApiClientConfig()
        self.hl = hl
        self.tz = tz
        self._trend_req = self._build_trend_req()

    def _build_trend_req(self) -> Any:
        try:
            pytrends_request = importlib.import_module("pytrends.request")
            trend_req_cls = getattr(pytrends_request, "TrendReq")
        except ImportError as exc:
            raise RuntimeError(
                "PyTrends is required for Google Trends ingestion. "
                "Install dependencies from requirements.txt."
            ) from exc

        return trend_req_cls(
            hl=self.hl,
            tz=self.tz,
            retries=self.config.max_retries,
            backoff_factor=self.config.backoff_seconds,
            timeout=(self.config.timeout_seconds, self.config.timeout_seconds),
        )

    def get_interest_over_time(
        self,
        *,
        keywords: list[str],
        timeframe: str,
        geo: str,
    ) -> list[dict[str, Any]]:
        if not keywords:
            return []

        self._trend_req.build_payload(keywords, timeframe=timeframe, geo=geo)
        frame = self._trend_req.interest_over_time()
        if frame is None or frame.empty:
            return []

        records: list[dict[str, Any]] = []
        is_partial_series = frame["isPartial"] if "isPartial" in frame.columns else None
        for dt, row in frame.iterrows():
            for keyword in keywords:
                if keyword not in row:
                    continue
                records.append(
                    {
                        "keyword": keyword,
                        "geo": geo,
                        "timeframe": timeframe,
                        "snapshot_ts": dt.isoformat(),
                        "interest_score": int(row[keyword]),
                        "is_partial": bool(is_partial_series.loc[dt]) if is_partial_series is not None else False,
                    }
                )
        return records

    def get_interest_by_region(
        self,
        *,
        keyword: str,
        timeframe: str,
        geo: str,
        resolution: str = "COUNTRY",
    ) -> list[dict[str, Any]]:
        self._trend_req.build_payload([keyword], timeframe=timeframe, geo=geo)
        frame = self._trend_req.interest_by_region(
            resolution=resolution,
            inc_low_vol=True,
            inc_geo_code=True,
        )
        if frame is None or frame.empty:
            return []

        records: list[dict[str, Any]] = []
        for region_name, row in frame.iterrows():
            records.append(
                {
                    "keyword": keyword,
                    "source_geo": geo,
                    "timeframe": timeframe,
                    "region_name": region_name,
                    "geo_code": row.get("geoCode"),
                    "interest_score": int(row.get(keyword, 0)),
                }
            )
        return records
